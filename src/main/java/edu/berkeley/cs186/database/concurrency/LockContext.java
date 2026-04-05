package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * LockContext wraps around LockManager to provide the hierarchical structure
 * of multigranularity locking. Calls to acquire/release/etc. locks should
 * be mostly done through a LockContext, which provides access to locking
 * methods at a certain point in the hierarchy (database, table X, etc.)
 */
public class LockContext {
    // You should not remove any of these fields. You may add additional
    // fields/methods as you see fit.

    // The underlying lock manager.
    protected final LockManager lockman;

    // The parent LockContext object, or null if this LockContext is at the top of the hierarchy.
    protected final LockContext parent;

    // The name of the resource this LockContext represents.
    protected ResourceName name;

    // Whether this LockContext is readonly. If a LockContext is readonly, acquire/release/promote/escalate should
    // throw an UnsupportedOperationException.
    protected boolean readonly;

    // A mapping between transaction numbers, and the number of locks on children of this LockContext
    // that the transaction holds.
    protected final Map<Long, Integer> numChildLocks;

    // You should not modify or use this directly.
    protected final Map<String, LockContext> children;

    // Whether or not any new child LockContexts should be marked readonly.
    protected boolean childLocksDisabled;

    public LockContext(LockManager lockman, LockContext parent, String name) {
        this(lockman, parent, name, false);
    }

    protected LockContext(LockManager lockman, LockContext parent, String name,
                          boolean readonly) {
        this.lockman = lockman;
        this.parent = parent;
        if (parent == null) {
            this.name = new ResourceName(name);
        } else {
            this.name = new ResourceName(parent.getResourceName(), name);
        }
        this.readonly = readonly;
        this.numChildLocks = new ConcurrentHashMap<>();
        this.children = new ConcurrentHashMap<>();
        this.childLocksDisabled = readonly;
    }

    /**
     * Gets a lock context corresponding to `name` from a lock manager.
     */
    public static LockContext fromResourceName(LockManager lockman, ResourceName name) {
        Iterator<String> names = name.getNames().iterator();
        LockContext ctx;
        String n1 = names.next();
        ctx = lockman.context(n1);
        while (names.hasNext()) {
            String n = names.next();
            ctx = ctx.childContext(n);
        }
        return ctx;
    }

    /**
     * Get the name of the resource that this lock context pertains to.
     */
    public ResourceName getResourceName() {
        return name;
    }

    /**
     * Acquire a `lockType` lock, for transaction `transaction`.
     *
     * Note: you must make any necessary updates to numChildLocks, or else calls
     * to LockContext#getNumChildren will not work properly.
     *
     * @throws InvalidLockException if the request is invalid
     * @throws DuplicateLockRequestException if a lock is already held by the
     * transaction.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void acquire(TransactionContext transaction, LockType lockType)
            throws InvalidLockException, DuplicateLockRequestException {
        if (readonly) {
            throw new UnsupportedOperationException("readonly context");
        }
        // nl is not a valid lock type to acquire - use release instead
        if (lockType == LockType.NL) {
            throw new InvalidLockException("use release instead of acquiring nl");
        }
        // check that parent lock allows this lock type
        if (parent != null) {
            LockType parentLockType = parent.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentLockType, lockType)) {
                throw new InvalidLockException("parent lock insufficient for requested lock type");
            }
        }
        // if ancestor has six, acquiring is or s on descendant is redundant and forbidden
        if (hasSIXAncestor(transaction) && (lockType == LockType.IS || lockType == LockType.S)) {
            throw new InvalidLockException("ancestor has six, cannot acquire is or s");
        }
        lockman.acquire(transaction, name, lockType);
        if (parent != null) {
            parent.numChildLocks.merge(transaction.getTransNum(), 1, Integer::sum);
        }
        return;
    }

    /**
     * Release `transaction`'s lock on `name`.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or
     * else calls to LockContext#getNumChildren will not work properly.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     * @throws InvalidLockException if the lock cannot be released because
     * doing so would violate multigranularity locking constraints
     * @throws UnsupportedOperationException if context is readonly
     */
    public void release(TransactionContext transaction)
            throws NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("readonly context");
        }
        // cannot release if children still hold locks
        if (getNumChildren(transaction) > 0) {
            throw new InvalidLockException("cannot release: children still hold locks");
        }
        lockman.release(transaction, name);
        if (parent != null) {
            parent.numChildLocks.merge(transaction.getTransNum(), -1, Integer::sum);
        }
        return;
    }

    /**
     * Promote `transaction`'s lock to `newLockType`. For promotion to SIX from
     * IS/IX, all S and IS locks on descendants must be simultaneously
     * released. The helper function sisDescendants may be helpful here.
     *
     * Note: you *must* make any necessary updates to numChildLocks, or else
     * calls to LockContext#getNumChildren will not work properly.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock
     * @throws NoLockHeldException if `transaction` has no lock
     * @throws InvalidLockException if the requested lock type is not a
     * promotion or promoting would cause the lock manager to enter an invalid
     * state (e.g. IS(parent), X(child)). A promotion from lock type A to lock
     * type B is valid if B is substitutable for A and B is not equal to A, or
     * if B is SIX and A is IS/IX/S, and invalid otherwise. hasSIXAncestor may
     * be helpful here.
     * @throws UnsupportedOperationException if context is readonly
     */
    public void promote(TransactionContext transaction, LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        if (readonly) {
            throw new UnsupportedOperationException("readonly context");
        }
        LockType currentType = getExplicitLockType(transaction);
        if (currentType == LockType.NL) {
            throw new NoLockHeldException("no lock held");
        }
        if (currentType == newLockType) {
            throw new DuplicateLockRequestException("already holds this lock type");
        }
        // promoting to six: valid from is, ix, or s only
        if (newLockType == LockType.SIX) {
            boolean validFrom = (currentType == LockType.IS || currentType == LockType.IX || currentType == LockType.S);
            if (!validFrom) {
                throw new InvalidLockException("invalid lock promotion");
            }
            if (hasSIXAncestor(transaction)) {
                throw new InvalidLockException("ancestor has six, cannot promote to six");
            }
            // release all is/s descendants simultaneously with six acquisition
            List<ResourceName> releasedNames = sisDescendants(transaction);
            releasedNames.add(name);
            lockman.acquireAndRelease(transaction, name, LockType.SIX, releasedNames);
            // decrement numchildlocks for each released descendant's parent context
            for (ResourceName relName : releasedNames) {
                if (relName.equals(name)) {
                    continue;
                }
                ResourceName parentName = relName.parent();
                if (parentName != null) {
                    LockContext parentCtx = LockContext.fromResourceName(lockman, parentName);
                    parentCtx.numChildLocks.merge(transaction.getTransNum(), -1, Integer::sum);
                }
            }
            return;
        }
        // non-six promotion: new type must substitute current
        if (!LockType.substitutable(newLockType, currentType)) {
            throw new InvalidLockException("invalid lock promotion");
        }
        // check parent still allows the new lock type
        if (parent != null) {
            LockType parentLockType = parent.getExplicitLockType(transaction);
            if (!LockType.canBeParentLock(parentLockType, newLockType)) {
                throw new InvalidLockException("parent lock insufficient for promotion");
            }
        }
        lockman.promote(transaction, name, newLockType);
        return;
    }

    /**
     * Escalate `transaction`'s lock from descendants of this context to this
     * level, using either an S or X lock. There should be no descendant locks
     * after this call, and every operation valid on descendants of this context
     * before this call must still be valid. You should only make *one* mutating
     * call to the lock manager, and should only request information about
     * TRANSACTION from the lock manager.
     *
     * For example, if a transaction has the following locks:
     *
     *                    IX(database)
     *                    /         \
     *               IX(table1)    S(table2)
     *                /      \
     *    S(table1 page3)  X(table1 page5)
     *
     * then after table1Context.escalate(transaction) is called, we should have:
     *
     *                    IX(database)
     *                    /         \
     *               X(table1)     S(table2)
     *
     * You should not make any mutating calls if the locks held by the
     * transaction do not change (such as when you call escalate multiple times
     * in a row).
     *
     * Note: you *must* make any necessary updates to numChildLocks of all
     * relevant contexts, or else calls to LockContext#getNumChildren will not
     * work properly.
     *
     * @throws NoLockHeldException if `transaction` has no lock at this level
     * @throws UnsupportedOperationException if context is readonly
     */
    public void escalate(TransactionContext transaction) throws NoLockHeldException {
        if (readonly) {
            throw new UnsupportedOperationException("readonly context");
        }
        LockType currentType = getExplicitLockType(transaction);
        if (currentType == LockType.NL) {
            throw new NoLockHeldException("no lock at this level");
        }
        // collect all descendant locks for this transaction
        List<Lock> allLocks = lockman.getLocks(transaction);
        List<ResourceName> descNames = new ArrayList<>();
        boolean needX = false;
        // check if current type requires x
        if (currentType == LockType.X || currentType == LockType.IX || currentType == LockType.SIX) {
            needX = true;
        }
        for (Lock lock : allLocks) {
            if (lock.name.isDescendantOf(name)) {
                descNames.add(lock.name);
                if (lock.lockType == LockType.X || lock.lockType == LockType.IX || lock.lockType == LockType.SIX) {
                    needX = true;
                }
            }
        }
        LockType targetType = needX ? LockType.X : LockType.S;
        // no-op if no descendants and already at target
        if (descNames.isEmpty() && currentType == targetType) {
            return;
        }
        // include this resource in release list so acquireandrelease updates in place
        List<ResourceName> releaseNames = new ArrayList<>(descNames);
        releaseNames.add(name);
        lockman.acquireAndRelease(transaction, name, targetType, releaseNames);
        // this context has no children with locks anymore
        numChildLocks.put(transaction.getTransNum(), 0);
        // update numchildlocks for intermediate ancestor contexts between descendants and this
        for (ResourceName descName : descNames) {
            ResourceName parentName = descName.parent();
            if (parentName != null && !parentName.equals(name)) {
                LockContext parentCtx = LockContext.fromResourceName(lockman, parentName);
                parentCtx.numChildLocks.merge(transaction.getTransNum(), -1, Integer::sum);
            }
        }
        return;
    }

    /**
     * Get the type of lock that `transaction` holds at this level, or NL if no
     * lock is held at this level.
     */
    public LockType getExplicitLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        return lockman.getLockType(transaction, name);
    }

    /**
     * Gets the type of lock that the transaction has at this level, either
     * implicitly (e.g. explicit S lock at higher level implies S lock at this
     * level) or explicitly. Returns NL if there is no explicit nor implicit
     * lock.
     */
    public LockType getEffectiveLockType(TransactionContext transaction) {
        if (transaction == null) return LockType.NL;
        // check ancestors for implicit locks: x ancestor gives x, s or six ancestor gives s
        LockType implicit = LockType.NL;
        LockContext curr = parent;
        while (curr != null) {
            LockType ancestorType = curr.getExplicitLockType(transaction);
            if (ancestorType == LockType.X) {
                implicit = LockType.X;
                break;
            }
            if (ancestorType == LockType.S || ancestorType == LockType.SIX) {
                implicit = LockType.S;
                // keep looking up in case there is an x ancestor above
            }
            curr = curr.parent;
        }
        LockType explicit = getExplicitLockType(transaction);
        if (implicit == LockType.X) {
            return LockType.X;
        }
        if (implicit == LockType.S) {
            if (explicit == LockType.X) {
                return LockType.X;
            }
            return LockType.S;
        }
        return explicit;
    }

    /**
     * Helper method to see if the transaction holds a SIX lock at an ancestor
     * of this context
     * @param transaction the transaction
     * @return true if holds a SIX at an ancestor, false if not
     */
    private boolean hasSIXAncestor(TransactionContext transaction) {
        LockContext curr = parent;
        while (curr != null) {
            if (curr.getExplicitLockType(transaction) == LockType.SIX) {
                return true;
            }
            curr = curr.parent;
        }
        return false;
    }

    /**
     * Helper method to get a list of resourceNames of all locks that are S or
     * IS and are descendants of current context for the given transaction.
     * @param transaction the given transaction
     * @return a list of ResourceNames of descendants which the transaction
     * holds an S or IS lock.
     */
    private List<ResourceName> sisDescendants(TransactionContext transaction) {
        List<ResourceName> result = new ArrayList<>();
        List<Lock> txLocks = lockman.getLocks(transaction);
        for (Lock lock : txLocks) {
            if (lock.name.isDescendantOf(name)) {
                if (lock.lockType == LockType.S || lock.lockType == LockType.IS) {
                    result.add(lock.name);
                }
            }
        }
        return result;
    }

    /**
     * Disables locking descendants. This causes all new child contexts of this
     * context to be readonly. This is used for indices and temporary tables
     * (where we disallow finer-grain locks), the former due to complexity
     * locking B+ trees, and the latter due to the fact that temporary tables
     * are only accessible to one transaction, so finer-grain locks make no
     * sense.
     */
    public void disableChildLocks() {
        this.childLocksDisabled = true;
    }

    /**
     * Gets the parent context.
     */
    public LockContext parentContext() {
        return parent;
    }

    /**
     * Gets the context for the child with name `name` and readable name
     * `readable`
     */
    public synchronized LockContext childContext(String name) {
        LockContext temp = new LockContext(lockman, this, name,
                this.childLocksDisabled || this.readonly);
        LockContext child = this.children.putIfAbsent(name, temp);
        if (child == null) child = temp;
        return child;
    }

    /**
     * Gets the context for the child with name `name`.
     */
    public synchronized LockContext childContext(long name) {
        return childContext(Long.toString(name));
    }

    /**
     * Gets the number of locks held on children a single transaction.
     */
    public int getNumChildren(TransactionContext transaction) {
        return numChildLocks.getOrDefault(transaction.getTransNum(), 0);
    }

    @Override
    public String toString() {
        return "LockContext(" + name.toString() + ")";
    }
}

