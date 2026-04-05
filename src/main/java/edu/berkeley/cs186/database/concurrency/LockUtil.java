package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.TransactionContext;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above: In this case, consider what values the explicit
     *   lock type can be, and think about how ancestor looks will need to be
     *   acquired or changed.
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null || lockContext == null) return;

        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // nl request: nothing to do
        if (requestType == LockType.NL) {
            return;
        }
        // effective lock already sufficient
        if (LockType.substitutable(effectiveLockType, requestType)) {
            return;
        }
        // special case: ix held and s requested -> promote to six
        if (explicitLockType == LockType.IX && requestType == LockType.S) {
            ensureAncestors(transaction, lockContext, LockType.SIX);
            lockContext.promote(transaction, LockType.SIX);
            return;
        }
        // if an intent lock is held, escalate to coarse s or x
        if (explicitLockType.isIntent()) {
            ensureAncestors(transaction, lockContext, requestType);
            lockContext.escalate(transaction);
            // after escalate, check if the result is sufficient; if not, promote
            LockType afterEscalate = lockContext.getExplicitLockType(transaction);
            if (!LockType.substitutable(afterEscalate, requestType)) {
                lockContext.promote(transaction, requestType);
            }
            return;
        }
        // no lock held or s held and x needed: acquire or promote
        ensureAncestors(transaction, lockContext, requestType);
        if (explicitLockType == LockType.NL) {
            lockContext.acquire(transaction, requestType);
        } else {
            lockContext.promote(transaction, requestType);
        }
        return;
    }

    // ensures all ancestors of lockContext have at least the intent lock required for childType
    private static void ensureAncestors(TransactionContext transaction, LockContext lockContext, LockType childType) {
        LockContext parentCtx = lockContext.parentContext();
        if (parentCtx == null) {
            return;
        }
        LockType requiredParent = LockType.parentLock(childType);
        // recurse first so we handle from root down
        ensureAncestors(transaction, parentCtx, requiredParent);
        LockType parentExplicit = parentCtx.getExplicitLockType(transaction);
        if (parentExplicit == LockType.NL) {
            parentCtx.acquire(transaction, requiredParent);
        } else if (!LockType.substitutable(parentExplicit, requiredParent)) {
            // need to upgrade parent lock
            if (parentExplicit == LockType.IS && requiredParent == LockType.IX) {
                parentCtx.promote(transaction, LockType.IX);
            } else if (parentExplicit == LockType.S && requiredParent == LockType.IX) {
                parentCtx.promote(transaction, LockType.SIX);
            } else {
                parentCtx.promote(transaction, requiredParent);
            }
        }
    }
}
