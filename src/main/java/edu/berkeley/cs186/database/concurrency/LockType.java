package edu.berkeley.cs186.database.concurrency;

/**
 * Utility methods to track the relationships between different lock types.
 */
public enum LockType {
    S,   // shared
    X,   // exclusive
    IS,  // intention shared
    IX,  // intention exclusive
    SIX, // shared intention exclusive
    NL;  // no lock held

    /**
     * This method checks whether lock types A and B are compatible with
     * each other. If a transaction can hold lock type A on a resource
     * at the same time another transaction holds lock type B on the same
     * resource, the lock types are compatible.
     */
    public static boolean compatible(LockType a, LockType b) {
        if (a == null || b == null) {
            throw new NullPointerException("null lock type");
        }
        // nl is compatible with everything
        if (a == NL || b == NL) {
            return true;
        }
        // x is exclusive - incompatible with everything except nl
        if (a == X || b == X) {
            return false;
        }
        // is is compatible with is, ix, s, six (everything remaining except x which is handled)
        if (a == IS) {
            return true;
        }
        if (b == IS) {
            return true;
        }
        // remaining types are ix, s, six
        // ix is compatible only with ix (and is which is handled above)
        if (a == IX) {
            return b == IX;
        }
        if (b == IX) {
            return false;
        }
        // remaining types are s and six
        // s is compatible only with s (and is, ix which are handled)
        if (a == S) {
            return b == S;
        }
        // a == six: six is incompatible with s and six
        return false;
    }

    /**
     * This method returns the lock on the parent resource
     * that should be requested for a lock of type A to be granted.
     */
    public static LockType parentLock(LockType a) {
        if (a == null) {
            throw new NullPointerException("null lock type");
        }
        switch (a) {
        case S: return IS;
        case X: return IX;
        case IS: return IS;
        case IX: return IX;
        case SIX: return IX;
        case NL: return NL;
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }

    /**
     * This method returns if parentLockType has permissions to grant a childLockType
     * on a child.
     */
    public static boolean canBeParentLock(LockType parentLockType, LockType childLockType) {
        if (parentLockType == null || childLockType == null) {
            throw new NullPointerException("null lock type");
        }
        // any lock type can be parent of nl
        if (childLockType == NL) {
            return true;
        }
        // six special case: cannot have is, s, or six children (redundant)
        if (parentLockType == SIX) {
            return childLockType == IX || childLockType == X;
        }
        // otherwise check if parent substitutes the required parent lock for child
        return substitutable(parentLockType, parentLock(childLockType));
    }

    /**
     * This method returns whether a lock can be used for a situation
     * requiring another lock (e.g. an S lock can be substituted with
     * an X lock, because an X lock allows the transaction to do everything
     * the S lock allowed it to do).
     */
    public static boolean substitutable(LockType substitute, LockType required) {
        if (required == null || substitute == null) {
            throw new NullPointerException("null lock type");
        }
        // same lock always substitutes itself
        if (substitute == required) {
            return true;
        }
        // anything substitutes nl
        if (required == NL) {
            return true;
        }
        // nl substitutes nothing else
        if (substitute == NL) {
            return false;
        }
        // x substitutes everything
        if (substitute == X) {
            return true;
        }
        // nothing else substitutes x
        if (required == X) {
            return false;
        }
        // lattice order: nl < is < s < six < x  and  nl < is < ix < six < x
        // six substitutes s, ix, and is
        if (substitute == SIX) {
            return required == S || required == IX || required == IS;
        }
        // s substitutes is (s >= is in the s-branch of the lattice)
        if (substitute == S) {
            return required == IS;
        }
        // ix substitutes is (ix >= is in the ix-branch of the lattice)
        if (substitute == IX) {
            return required == IS;
        }
        // is only substitutes nl and itself (both handled above)
        return false;
    }

    /**
     * @return True if this lock is IX, IS, or SIX. False otherwise.
     */
    public boolean isIntent() {
        return this == LockType.IX || this == LockType.IS || this == LockType.SIX;
    }

    @Override
    public String toString() {
        switch (this) {
        case S: return "S";
        case X: return "X";
        case IS: return "IS";
        case IX: return "IX";
        case SIX: return "SIX";
        case NL: return "NL";
        default: throw new UnsupportedOperationException("bad lock type");
        }
    }
}

