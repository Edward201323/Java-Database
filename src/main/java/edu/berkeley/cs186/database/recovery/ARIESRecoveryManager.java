package edu.berkeley.cs186.database.recovery;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.common.Pair;
import edu.berkeley.cs186.database.concurrency.DummyLockContext;
import edu.berkeley.cs186.database.io.DiskSpaceManager;
import edu.berkeley.cs186.database.memory.BufferManager;
import edu.berkeley.cs186.database.memory.Page;
import edu.berkeley.cs186.database.recovery.records.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * Implementation of ARIES.
 */
public class ARIESRecoveryManager implements RecoveryManager {
    // Disk space manager.
    DiskSpaceManager diskSpaceManager;
    // Buffer manager.
    BufferManager bufferManager;

    // Function to create a new transaction for recovery with a given
    // transaction number.
    private Function<Long, Transaction> newTransaction;

    // Log manager
    LogManager logManager;
    // Dirty page table (page number -> recLSN).
    Map<Long, Long> dirtyPageTable = new ConcurrentHashMap<>();
    // Transaction table (transaction number -> entry).
    Map<Long, TransactionTableEntry> transactionTable = new ConcurrentHashMap<>();
    // true if redo phase of restart has terminated, false otherwise. Used
    // to prevent DPT entries from being flushed during restartRedo.
    boolean redoComplete;

    public ARIESRecoveryManager(Function<Long, Transaction> newTransaction) {
        this.newTransaction = newTransaction;
    }

    /**
     * Initializes the log; only called the first time the database is set up.
     * The master record should be added to the log, and a checkpoint should be
     * taken.
     */
    @Override
    public void initialize() {
        this.logManager.appendToLog(new MasterLogRecord(0));
        this.checkpoint();
    }

    /**
     * Sets the buffer/disk managers. This is not part of the constructor
     * because of the cyclic dependency between the buffer manager and recovery
     * manager (the buffer manager must interface with the recovery manager to
     * block page evictions until the log has been flushed, but the recovery
     * manager needs to interface with the buffer manager to write the log and
     * redo changes).
     * @param diskSpaceManager disk space manager
     * @param bufferManager buffer manager
     */
    @Override
    public void setManagers(DiskSpaceManager diskSpaceManager, BufferManager bufferManager) {
        this.diskSpaceManager = diskSpaceManager;
        this.bufferManager = bufferManager;
        this.logManager = new LogManager(bufferManager);
    }

    // Forward Processing //////////////////////////////////////////////////////

    /**
     * Called when a new transaction is started.
     *
     * The transaction should be added to the transaction table.
     *
     * @param transaction new transaction
     */
    @Override
    public synchronized void startTransaction(Transaction transaction) {
        this.transactionTable.put(transaction.getTransNum(), new TransactionTableEntry(transaction));
    }

    /**
     * Called when a transaction is about to start committing.
     *
     * A commit record should be appended, the log should be flushed,
     * and the transaction table and the transaction status should be updated.
     *
     * @param transNum transaction being committed
     * @return LSN of the commit record
     */
    @Override
    public long commit(long transNum) {
        // get the transaction entry
        TransactionTableEntry entry = transactionTable.get(transNum);
        long prevLSN = entry.lastLSN;
        // append the commit record to the log
        LogRecord record = new CommitTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // update last lsn and flush to ensure durability
        entry.lastLSN = LSN;
        logManager.flushToLSN(LSN);
        // mark transaction as committing
        entry.transaction.setStatus(Transaction.Status.COMMITTING);
        return LSN;
    }

    /**
     * Called when a transaction is set to be aborted.
     *
     * An abort record should be appended, and the transaction table and
     * transaction status should be updated. Calling this function should not
     * perform any rollbacks.
     *
     * @param transNum transaction being aborted
     * @return LSN of the abort record
     */
    @Override
    public long abort(long transNum) {
        // get the transaction entry
        TransactionTableEntry entry = transactionTable.get(transNum);
        long prevLSN = entry.lastLSN;
        // append the abort record to the log
        LogRecord record = new AbortTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // update last lsn and mark transaction as aborting
        entry.lastLSN = LSN;
        entry.transaction.setStatus(Transaction.Status.ABORTING);
        return LSN;
    }

    /**
     * Called when a transaction is cleaning up; this should roll back
     * changes if the transaction is aborting (see the rollbackToLSN helper
     * function below).
     *
     * Any changes that need to be undone should be undone, the transaction should
     * be removed from the transaction table, the end record should be appended,
     * and the transaction status should be updated.
     *
     * @param transNum transaction to end
     * @return LSN of the end record
     */
    @Override
    public long end(long transNum) {
        // get the transaction entry
        TransactionTableEntry entry = transactionTable.get(transNum);
        // if aborting, roll back all changes before ending
        if (entry.transaction.getStatus() == Transaction.Status.ABORTING ||
            entry.transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
            rollbackToLSN(transNum, 0);
        }
        // cleanup the transaction resources
        entry.transaction.cleanup();
        // append the end record
        long prevLSN = entry.lastLSN;
        LogRecord record = new EndTransactionLogRecord(transNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // update status and remove from transaction table
        entry.lastLSN = LSN;
        entry.transaction.setStatus(Transaction.Status.COMPLETE);
        transactionTable.remove(transNum);
        return LSN;
    }

    /**
     * Recommended helper function: performs a rollback of all of a
     * transaction's actions, up to (but not including) a certain LSN.
     * Starting with the LSN of the most recent record that hasn't been undone:
     * - while the current LSN is greater than the LSN we're rolling back to:
     *    - if the record at the current LSN is undoable:
     *       - Get a compensation log record (CLR) by calling undo on the record
     *       - Append the CLR
     *       - Call redo on the CLR to perform the undo
     *    - update the current LSN to that of the next record to undo
     *
     * Note above that calling .undo() on a record does not perform the undo, it
     * just creates the compensation log record.
     *
     * @param transNum transaction to perform a rollback for
     * @param LSN LSN to which we should rollback
     */
    private void rollbackToLSN(long transNum, long LSN) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        LogRecord lastRecord = logManager.fetchLogRecord(transactionEntry.lastLSN);
        long lastRecordLSN = lastRecord.getLSN();
        // Small optimization: if the last record is a CLR we can start rolling
        // back from the next record that hasn't yet been undone.
        long currentLSN = lastRecord.getUndoNextLSN().orElse(lastRecordLSN);
        // walk backwards through the log undoing records until we reach LSN
        while (currentLSN > LSN) {
            LogRecord current = logManager.fetchLogRecord(currentLSN);
            if (current.isUndoable()) {
                // create the clr for this record and append it
                LogRecord clr = current.undo(transactionEntry.lastLSN);
                long clrLSN = logManager.appendToLog(clr);
                transactionEntry.lastLSN = clrLSN;
                // redo the clr to actually perform the undo
                clr.redo(this, diskSpaceManager, bufferManager);
            }
            // navigate to the next record to undo: use undoNextLSN for clrs, prevLSN otherwise
            if (current.getUndoNextLSN().isPresent()) {
                currentLSN = current.getUndoNextLSN().get();
            } else if (current.getPrevLSN().isPresent()) {
                currentLSN = current.getPrevLSN().get();
            } else {
                currentLSN = 0;
            }
        }
    }

    /**
     * Called before a page is flushed from the buffer cache. This
     * method is never called on a log page.
     *
     * The log should be as far as necessary.
     *
     * @param pageLSN pageLSN of page about to be flushed
     */
    @Override
    public void pageFlushHook(long pageLSN) {
        logManager.flushToLSN(pageLSN);
    }

    /**
     * Called when a page has been updated on disk.
     *
     * As the page is no longer dirty, it should be removed from the
     * dirty page table.
     *
     * @param pageNum page number of page updated on disk
     */
    @Override
    public void diskIOHook(long pageNum) {
        if (redoComplete) dirtyPageTable.remove(pageNum);
    }

    /**
     * Called when a write to a page happens.
     *
     * This method is never called on a log page. Arguments to the before and after params
     * are guaranteed to be the same length.
     *
     * The appropriate log record should be appended, and the transaction table
     * and dirty page table should be updated accordingly.
     *
     * @param transNum transaction performing the write
     * @param pageNum page number of page being written
     * @param pageOffset offset into page where write begins
     * @param before bytes starting at pageOffset before the write
     * @param after bytes starting at pageOffset after the write
     * @return LSN of last record written to log
     */
    @Override
    public long logPageWrite(long transNum, long pageNum, short pageOffset, byte[] before,
                             byte[] after) {
        assert (before.length == after.length);
        assert (before.length <= BufferManager.EFFECTIVE_PAGE_SIZE / 2);
        // get the transaction entry
        TransactionTableEntry entry = transactionTable.get(transNum);
        long prevLSN = entry.lastLSN;
        // append the update page record
        LogRecord record = new UpdatePageLogRecord(transNum, pageNum, prevLSN, pageOffset, before, after);
        long LSN = logManager.appendToLog(record);
        // update last lsn in the transaction table
        entry.lastLSN = LSN;
        // add to dirty page table with the recLSN if not already present
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        return LSN;
    }

    /**
     * Called when a new partition is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param partNum partition number of the new partition
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a partition is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the partition is the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the partition be freed
     * @param partNum partition number of the partition being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePart(long transNum, int partNum) {
        // Ignore if part of the log.
        if (partNum == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePartLogRecord(transNum, partNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a new page is allocated. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the allocation
     * @param pageNum page number of the new page
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logAllocPage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new AllocPageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Called when a page is freed. A log flush is necessary,
     * since changes are visible on disk immediately after this returns.
     *
     * This method should return -1 if the page is in the log partition.
     *
     * The appropriate log record should be appended, and the log flushed.
     * The transaction table should be updated accordingly.
     *
     * @param transNum transaction requesting the page be freed
     * @param pageNum page number of the page being freed
     * @return LSN of record or -1 if log partition
     */
    @Override
    public long logFreePage(long transNum, long pageNum) {
        // Ignore if part of the log.
        if (DiskSpaceManager.getPartNum(pageNum) == 0) return -1L;

        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        long prevLSN = transactionEntry.lastLSN;
        LogRecord record = new FreePageLogRecord(transNum, pageNum, prevLSN);
        long LSN = logManager.appendToLog(record);
        // Update lastLSN
        transactionEntry.lastLSN = LSN;
        dirtyPageTable.remove(pageNum);
        // Flush log
        logManager.flushToLSN(LSN);
        return LSN;
    }

    /**
     * Creates a savepoint for a transaction. Creating a savepoint with
     * the same name as an existing savepoint for the transaction should
     * delete the old savepoint.
     *
     * The appropriate LSN should be recorded so that a partial rollback
     * is possible later.
     *
     * @param transNum transaction to make savepoint for
     * @param name name of savepoint
     */
    @Override
    public void savepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.addSavepoint(name);
    }

    /**
     * Releases (deletes) a savepoint for a transaction.
     * @param transNum transaction to delete savepoint for
     * @param name name of savepoint
     */
    @Override
    public void releaseSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);
        transactionEntry.deleteSavepoint(name);
    }

    /**
     * Rolls back transaction to a savepoint.
     *
     * All changes done by the transaction since the savepoint should be undone,
     * in reverse order, with the appropriate CLRs written to log. The transaction
     * status should remain unchanged.
     *
     * @param transNum transaction to partially rollback
     * @param name name of savepoint
     */
    @Override
    public void rollbackToSavepoint(long transNum, String name) {
        TransactionTableEntry transactionEntry = transactionTable.get(transNum);
        assert (transactionEntry != null);

        // all of the transaction's changes strictly after the record at LSN should be undone
        long savepointLSN = transactionEntry.getSavepoint(name);
        // roll back to the savepoint
        rollbackToLSN(transNum, savepointLSN);
    }

    /**
     * Create a checkpoint.
     *
     * First, a begin checkpoint record should be written.
     *
     * Then, end checkpoint records should be filled up as much as possible first
     * using recLSNs from the DPT, then status/lastLSNs from the transactions
     * table, and written when full (or when nothing is left to be written).
     * You may find the method EndCheckpointLogRecord#fitsInOneRecord here to
     * figure out when to write an end checkpoint record.
     *
     * Finally, the master record should be rewritten with the LSN of the
     * begin checkpoint record.
     */
    @Override
    public synchronized void checkpoint() {
        // Create begin checkpoint log record and write to log
        LogRecord beginRecord = new BeginCheckpointLogRecord();
        long beginLSN = logManager.appendToLog(beginRecord);

        Map<Long, Long> chkptDPT = new HashMap<>();
        Map<Long, Pair<Transaction.Status, Long>> chkptTxnTable = new HashMap<>();

        // fill end checkpoint records with dpt entries first
        for (Map.Entry<Long, Long> dptEntry : dirtyPageTable.entrySet()) {
            // if adding the next dpt entry would overflow a page, flush what we have
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size() + 1, chkptTxnTable.size())) {
                LogRecord flushRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(flushRecord);
                chkptDPT.clear();
                chkptTxnTable.clear();
            }
            chkptDPT.put(dptEntry.getKey(), dptEntry.getValue());
        }

        // then fill with transaction table entries
        for (Map.Entry<Long, TransactionTableEntry> txnEntry : transactionTable.entrySet()) {
            // if adding the next txn entry would overflow a page, flush what we have
            if (!EndCheckpointLogRecord.fitsInOneRecord(chkptDPT.size(), chkptTxnTable.size() + 1)) {
                LogRecord flushRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
                logManager.appendToLog(flushRecord);
                chkptDPT.clear();
                chkptTxnTable.clear();
            }
            chkptTxnTable.put(txnEntry.getKey(), new Pair<>(
                txnEntry.getValue().transaction.getStatus(),
                txnEntry.getValue().lastLSN
            ));
        }

        // Last end checkpoint record
        LogRecord endRecord = new EndCheckpointLogRecord(chkptDPT, chkptTxnTable);
        logManager.appendToLog(endRecord);
        // Ensure checkpoint is fully flushed before updating the master record
        flushToLSN(endRecord.getLSN());

        // Update master record
        MasterLogRecord masterRecord = new MasterLogRecord(beginLSN);
        logManager.rewriteMasterRecord(masterRecord);
    }

    /**
     * Flushes the log to at least the specified record,
     * essentially flushing up to and including the page
     * that contains the record specified by the LSN.
     *
     * @param LSN LSN up to which the log should be flushed
     */
    @Override
    public void flushToLSN(long LSN) {
        this.logManager.flushToLSN(LSN);
    }

    @Override
    public void dirtyPage(long pageNum, long LSN) {
        dirtyPageTable.putIfAbsent(pageNum, LSN);
        // Handle race condition where earlier log is beaten to the insertion by
        // a later log.
        dirtyPageTable.computeIfPresent(pageNum, (k, v) -> Math.min(LSN,v));
    }

    @Override
    public void close() {
        this.checkpoint();
        this.logManager.close();
    }

    // Restart Recovery ////////////////////////////////////////////////////////

    /**
     * Called whenever the database starts up, and performs restart recovery.
     * Recovery is complete when the Runnable returned is run to termination.
     * New transactions may be started once this method returns.
     *
     * This should perform the three phases of recovery, and also clean the
     * dirty page table of non-dirty pages (pages that aren't dirty in the
     * buffer manager) between redo and undo, and perform a checkpoint after
     * undo.
     */
    @Override
    public void restart() {
        this.restartAnalysis();
        this.restartRedo();
        this.redoComplete = true;
        this.cleanDPT();
        this.restartUndo();
        this.checkpoint();
    }

    /**
     * This method performs the analysis pass of restart recovery.
     *
     * First, the master record should be read (LSN 0). The master record contains
     * one piece of information: the LSN of the last successful checkpoint.
     *
     * We then begin scanning log records, starting at the beginning of the
     * last successful checkpoint.
     *
     * If the log record is for a transaction operation (getTransNum is present)
     * - update the transaction table
     *
     * If the log record is page-related (getPageNum is present), update the dpt
     *   - update/undoupdate page will dirty pages
     *   - free/undoalloc page always flush changes to disk
     *   - no action needed for alloc/undofree page
     *
     * If the log record is for a change in transaction status:
     * - update transaction status to COMMITTING/RECOVERY_ABORTING/COMPLETE
     * - update the transaction table
     * - if END_TRANSACTION: clean up transaction (Transaction#cleanup), remove
     *   from txn table, and add to endedTransactions
     *
     * If the log record is an end_checkpoint record:
     * - Copy all entries of checkpoint DPT (replace existing entries if any)
     * - Skip txn table entries for transactions that have already ended
     * - Add to transaction table if not already present
     * - Update lastLSN to be the larger of the existing entry's (if any) and
     *   the checkpoint's
     * - The status's in the transaction table should be updated if it is possible
     *   to transition from the status in the table to the status in the
     *   checkpoint. For example, running -> aborting is a possible transition,
     *   but aborting -> running is not.
     *
     * After all records in the log are processed, for each ttable entry:
     *  - if COMMITTING: clean up the transaction, change status to COMPLETE,
     *    remove from the ttable, and append an end record
     *  - if RUNNING: change status to RECOVERY_ABORTING, and append an abort
     *    record
     *  - if RECOVERY_ABORTING: no action needed
     */
    void restartAnalysis() {
        // Read master record
        LogRecord record = logManager.fetchLogRecord(0L);
        // Type checking
        assert (record != null && record.getType() == LogType.MASTER);
        MasterLogRecord masterRecord = (MasterLogRecord) record;
        // get start checkpoint LSN
        long LSN = masterRecord.lastCheckpointLSN;
        // set of transactions that have already ended - skip these in checkpoint records
        Set<Long> endedTransactions = new HashSet<>();

        // scan forward from the last checkpoint
        Iterator<LogRecord> iter = logManager.scanFrom(LSN);
        while (iter.hasNext()) {
            LogRecord logRecord = iter.next();

            // if this record belongs to a transaction, ensure it exists in the table and update lastLSN
            if (logRecord.getTransNum().isPresent()) {
                long transNum = logRecord.getTransNum().get();
                if (!endedTransactions.contains(transNum)) {
                    if (!transactionTable.containsKey(transNum)) {
                        startTransaction(newTransaction.apply(transNum));
                    }
                    transactionTable.get(transNum).lastLSN = logRecord.getLSN();
                }
            }

            // handle page-related records: update or remove from dirty page table
            if (logRecord.getPageNum().isPresent()) {
                long pageNum = logRecord.getPageNum().get();
                LogType type = logRecord.getType();
                if (type == LogType.UPDATE_PAGE || type == LogType.UNDO_UPDATE_PAGE) {
                    // these records dirty the page in the buffer pool
                    dirtyPage(pageNum, logRecord.getLSN());
                } else if (type == LogType.FREE_PAGE || type == LogType.UNDO_ALLOC_PAGE) {
                    // these flush changes to disk, so page is no longer dirty
                    dirtyPageTable.remove(pageNum);
                }
                // alloc page and undo free page need no action on the dpt
            }

            // handle transaction status change records
            LogType type = logRecord.getType();
            if (type == LogType.COMMIT_TRANSACTION) {
                long transNum = logRecord.getTransNum().get();
                transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMMITTING);
            } else if (type == LogType.ABORT_TRANSACTION) {
                long transNum = logRecord.getTransNum().get();
                // use recovery_aborting status during restart analysis
                transactionTable.get(transNum).transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            } else if (type == LogType.END_TRANSACTION) {
                long transNum = logRecord.getTransNum().get();
                transactionTable.get(transNum).transaction.cleanup();
                transactionTable.get(transNum).transaction.setStatus(Transaction.Status.COMPLETE);
                transactionTable.remove(transNum);
                endedTransactions.add(transNum);
            } else if (type == LogType.END_CHECKPOINT) {
                // copy all dpt entries from the checkpoint (replaces existing entries)
                dirtyPageTable.putAll(logRecord.getDirtyPageTable());

                // process checkpoint transaction table entries
                for (Map.Entry<Long, Pair<Transaction.Status, Long>> chkptEntry :
                        logRecord.getTransactionTable().entrySet()) {
                    long transNum = chkptEntry.getKey();
                    // skip transactions that have already ended during this scan
                    if (endedTransactions.contains(transNum)) {
                        continue;
                    }
                    Transaction.Status chkptStatus = chkptEntry.getValue().getFirst();
                    long chkptLastLSN = chkptEntry.getValue().getSecond();

                    // during restart, aborting transactions are always recovery_aborting
                    if (chkptStatus == Transaction.Status.ABORTING) {
                        chkptStatus = Transaction.Status.RECOVERY_ABORTING;
                    }

                    if (!transactionTable.containsKey(transNum)) {
                        startTransaction(newTransaction.apply(transNum));
                    }

                    TransactionTableEntry tableEntry = transactionTable.get(transNum);
                    // lastLSN is the max of what we've seen and what the checkpoint recorded
                    tableEntry.lastLSN = Math.max(tableEntry.lastLSN, chkptLastLSN);

                    // only update status if it's a valid forward transition
                    Transaction.Status currentStatus = tableEntry.transaction.getStatus();
                    if (statusCanTransition(currentStatus, chkptStatus)) {
                        tableEntry.transaction.setStatus(chkptStatus);
                    }
                }
            }
        }

        // after scanning all records, handle remaining transactions in the table
        List<Long> toCommit = new ArrayList<>();
        List<Long> toAbort = new ArrayList<>();

        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            Transaction.Status status = entry.getValue().transaction.getStatus();
            if (status == Transaction.Status.COMMITTING) {
                toCommit.add(entry.getKey());
            } else if (status == Transaction.Status.RUNNING) {
                toAbort.add(entry.getKey());
            }
            // recovery_aborting: no action needed here, handled during undo phase
        }

        // clean up transactions that were committing
        for (long transNum : toCommit) {
            TransactionTableEntry entry = transactionTable.get(transNum);
            entry.transaction.cleanup();
            entry.transaction.setStatus(Transaction.Status.COMPLETE);
            LogRecord endRecord = new EndTransactionLogRecord(transNum, entry.lastLSN);
            long endLSN = logManager.appendToLog(endRecord);
            entry.lastLSN = endLSN;
            transactionTable.remove(transNum);
        }

        // mark running transactions as recovery_aborting
        for (long transNum : toAbort) {
            TransactionTableEntry entry = transactionTable.get(transNum);
            entry.transaction.setStatus(Transaction.Status.RECOVERY_ABORTING);
            LogRecord abortRecord = new AbortTransactionLogRecord(transNum, entry.lastLSN);
            long abortLSN = logManager.appendToLog(abortRecord);
            entry.lastLSN = abortLSN;
        }
    }

    // returns true if transitioning from current status to next is a valid forward move in the lifecycle
    private boolean statusCanTransition(Transaction.Status current, Transaction.Status next) {
        if (current == next) {
            return false;
        }
        if (current == Transaction.Status.COMPLETE) {
            return false;
        }
        if (current == Transaction.Status.COMMITTING) {
            return next == Transaction.Status.COMPLETE;
        }
        if (current == Transaction.Status.ABORTING || current == Transaction.Status.RECOVERY_ABORTING) {
            // aborting can move to complete or to recovery_aborting (same stage different enum)
            return next == Transaction.Status.COMPLETE
                || next == Transaction.Status.RECOVERY_ABORTING
                || next == Transaction.Status.ABORTING;
        }
        // running can transition to anything
        return true;
    }

    /**
     * This method performs the redo pass of restart recovery.
     *
     * First, determine the starting point for REDO from the dirty page table.
     *
     * Then, scanning from the starting point, if the record is redoable and
     * - partition-related (Alloc/Free/UndoAlloc/UndoFree..Part), always redo it
     * - allocates a page (AllocPage/UndoFreePage), always redo it
     * - modifies a page (Update/UndoUpdate/Free/UndoAlloc....Page) in
     *   the dirty page table with LSN >= recLSN, the page is fetched from disk,
     *   the pageLSN is checked, and the record is redone if needed.
     */
    void restartRedo() {
        // if the dpt is empty there is nothing to redo
        if (dirtyPageTable.isEmpty()) {
            return;
        }
        // starting point is the minimum recLSN across all dirty pages
        long redoLSN = Collections.min(dirtyPageTable.values());
        Iterator<LogRecord> iter = logManager.scanFrom(redoLSN);
        while (iter.hasNext()) {
            LogRecord record = iter.next();
            if (!record.isRedoable()) {
                continue;
            }
            // partition-related records are always redone
            if (record.getPartNum().isPresent()) {
                record.redo(this, diskSpaceManager, bufferManager);
                continue;
            }
            // page-related records need additional checks
            if (record.getPageNum().isPresent()) {
                long pageNum = record.getPageNum().get();
                LogType type = record.getType();
                // page alloc records are always redone
                if (type == LogType.ALLOC_PAGE || type == LogType.UNDO_FREE_PAGE) {
                    record.redo(this, diskSpaceManager, bufferManager);
                    continue;
                }
                // page modifying records: only redo if page is in dpt with lsn >= recLSN
                // and the page on disk has a smaller pageLSN than this record
                if (dirtyPageTable.containsKey(pageNum)) {
                    long recLSN = dirtyPageTable.get(pageNum);
                    if (record.getLSN() >= recLSN) {
                        Page page = bufferManager.fetchPage(new DummyLockContext(), pageNum);
                        try {
                            if (page.getPageLSN() < record.getLSN()) {
                                record.redo(this, diskSpaceManager, bufferManager);
                            }
                        } finally {
                            page.unpin();
                        }
                    }
                }
            }
        }
    }

    /**
     * This method performs the undo pass of restart recovery.

     * First, a priority queue is created sorted on lastLSN of all aborting
     * transactions.
     *
     * Then, always working on the largest LSN in the priority queue until we are done,
     * - if the record is undoable, undo it, and append the appropriate CLR
     * - replace the entry with a new one, using the undoNextLSN if available,
     *   if the prevLSN otherwise.
     * - if the new LSN is 0, clean up the transaction, set the status to complete,
     *   and remove from transaction table.
     */
    void restartUndo() {
        // build a max-heap of (lastLSN, transNum) for all recovery_aborting transactions
        PriorityQueue<Pair<Long, Long>> pq = new PriorityQueue<>(new PairFirstReverseComparator<>());
        for (Map.Entry<Long, TransactionTableEntry> entry : transactionTable.entrySet()) {
            if (entry.getValue().transaction.getStatus() == Transaction.Status.RECOVERY_ABORTING) {
                pq.add(new Pair<>(entry.getValue().lastLSN, entry.getKey()));
            }
        }
        // process the record with the largest LSN first until all aborting transactions are done
        while (!pq.isEmpty()) {
            Pair<Long, Long> top = pq.poll();
            long lsn = top.getFirst();
            long transNum = top.getSecond();
            TransactionTableEntry entry = transactionTable.get(transNum);
            LogRecord record = logManager.fetchLogRecord(lsn);

            if (record.isUndoable()) {
                // create the clr and append it, then redo the clr to perform the undo
                LogRecord clr = record.undo(entry.lastLSN);
                long clrLSN = logManager.appendToLog(clr);
                entry.lastLSN = clrLSN;
                clr.redo(this, diskSpaceManager, bufferManager);
            }

            // find the next lsn to process: use undoNextLSN for clrs, prevLSN for regular records
            long nextLSN;
            if (record.getUndoNextLSN().isPresent()) {
                nextLSN = record.getUndoNextLSN().get();
            } else if (record.getPrevLSN().isPresent()) {
                nextLSN = record.getPrevLSN().get();
            } else {
                nextLSN = 0;
            }

            if (nextLSN == 0) {
                // all changes for this transaction have been undone, end it
                entry.transaction.cleanup();
                entry.transaction.setStatus(Transaction.Status.COMPLETE);
                LogRecord endRecord = new EndTransactionLogRecord(transNum, entry.lastLSN);
                logManager.appendToLog(endRecord);
                transactionTable.remove(transNum);
            } else {
                // continue processing this transaction from the next lsn
                pq.add(new Pair<>(nextLSN, transNum));
            }
        }
    }

    /**
     * Removes pages from the DPT that are not dirty in the buffer manager.
     * This is slow and should only be used during recovery.
     */
    void cleanDPT() {
        Set<Long> dirtyPages = new HashSet<>();
        bufferManager.iterPageNums((pageNum, dirty) -> {
            if (dirty) dirtyPages.add(pageNum);
        });
        Map<Long, Long> oldDPT = new HashMap<>(dirtyPageTable);
        dirtyPageTable.clear();
        for (long pageNum : dirtyPages) {
            if (oldDPT.containsKey(pageNum)) {
                dirtyPageTable.put(pageNum, oldDPT.get(pageNum));
            }
        }
    }

    // Helpers /////////////////////////////////////////////////////////////////
    /**
     * Comparator for Pair<A, B> comparing only on the first element (type A),
     * in reverse order.
     */
    private static class PairFirstReverseComparator<A extends Comparable<A>, B> implements Comparator<Pair<A, B>> {
        @Override
        public int compare(Pair<A, B> p0, Pair<A, B> p1) {
            return p1.getFirst().compareTo(p0.getFirst());
        }
    }
}
