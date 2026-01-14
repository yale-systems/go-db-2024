package transaction

import (
	"sync"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/logging"
	"mit.edu/dsg/godb/storage"
)

// activeTxnEntry tracks a running transaction and its starting point in the log.
type activeTxnEntry struct {
	txn      *TransactionContext
	startLsn common.LSN
}

// TransactionManager is the central component managing the lifecycle of transactions.
// It coordinates with the LockManager for concurrency control and the LogManager for
// Write-Ahead Logging (WAL) and recovery.
type TransactionManager struct {
	// activeTxns maps TransactionIDs to their runtime context and metadata
	activeTxns *xsync.MapOf[common.TransactionID, activeTxnEntry]

	logManager  logging.LogManager
	bufferPool  *storage.BufferPool
	lockManager *LockManager

	nextTxnID common.TransactionID
	// Pool to recycle transaction contexts
	txnPool sync.Pool
}

// NewTransactionManager initializes the transaction manager.
func NewTransactionManager(logManager logging.LogManager, bufferPool *storage.BufferPool, lockManager *LockManager) *TransactionManager {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &TransactionManager{
		activeTxns:  xsync.NewMapOf[common.TransactionID, activeTxnEntry](),
		logManager:  logManager,
		bufferPool:  bufferPool,
		lockManager: lockManager,
		nextTxnID:   1,
		txnPool: sync.Pool{
			New: func() any {
				return &TransactionContext{
					id:         common.InvalidTransactionID,
					logRecords: newLogRecordBuffer(),
					heldLocks:  make(map[DBLockTag]DBLockMode),
				}
			},
		},
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Begin starts a new transaction and returns the initialized context.
func (tm *TransactionManager) Begin() (*TransactionContext, error) {
	// <silentstrip lab1|lab2|lab3|lab4>
	tid := tm.nextTxnID
	tm.nextTxnID++

	txn := tm.txnPool.Get().(*TransactionContext)
	txn.Reset(tid)

	// Write LogBegin (WAL)
	lsn, err := tm.logManager.Append(txn.NewBeginTransactionRecord())
	if err != nil {
		return nil, err
	}

	tm.activeTxns.Store(tid, activeTxnEntry{
		txn:      txn,
		startLsn: lsn,
	})

	return txn, nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Commit completes a transaction and makes its effects durable and visible.
func (tm *TransactionManager) Commit(txn *TransactionContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	lsn, err := tm.logManager.Append(txn.NewCommitRecord())

	if err != nil {
		return err
	}

	if err := tm.logManager.WaitUntilFlushed(lsn); err != nil {
		return err
	}

	txn.ReleaseAllLocks()
	tm.activeTxns.Delete(txn.id)
	tm.txnPool.Put(txn)
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Abort stops a transaction and ensures its effects are rolled back
func (tm *TransactionManager) Abort(txn *TransactionContext) error {
	// Rollback In-Memory changes (Indexes)
	// YOU SHOULD NOT NEED TO MODIFY THIS LOGIC
	for i := len(txn.cleanupStack) - 1; i >= 0; i-- {
		cleanupTask := txn.cleanupStack[i]
		cleanupTask.Target.Invoke(cleanupTask.Type, cleanupTask.Key, cleanupTask.RID)
	}

	// <silentstrip lab1|lab2|lab3|lab4>
	numRecordsToUndo := txn.logRecords.len()
	for i := numRecordsToUndo - 1; i >= 0; i-- {
		record := txn.logRecords.get(i)
		// Not something that requires undo
		if !undoable(record) {
			continue
		}
		// Fetch the frame to modify
		frame, err := tm.bufferPool.GetPage(record.RID().PageID)
		if err != nil {
			return err
		}

		hp := frame.AsHeapPage()
		err = tm.undo(txn, record, hp)
		tm.bufferPool.UnpinPage(frame, true)
		if err != nil {
			return err
		}
	}

	abort := txn.NewAbortRecord()
	_, err := tm.logManager.Append(abort)
	if err != nil {
		return err
	}

	txn.ReleaseAllLocks()
	tm.activeTxns.Delete(txn.id)
	tm.txnPool.Put(txn)
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// RestartTransactionForRecovery is used during database recovery (ARIES Analysis phase).
// It reconstructs a TransactionContext for a transaction that was active at the time of the crash.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) RestartTransactionForRecovery(txnId common.TransactionID) *TransactionContext {
	// <silentstrip lab1|lab2|lab3|lab4>
	txn := tm.txnPool.Get().(*TransactionContext)
	txn.Reset(txnId)
	return txn
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>
func undoable(r logging.LogRecord) bool {
	switch r.RecordType() {
	case logging.LogInsert, logging.LogUpdate, logging.LogDelete:
		return true
	default:
		return false
	}
}

func (tm *TransactionManager) undo(txn *TransactionContext, r logging.LogRecord, hp storage.HeapPage) error {
	var clr logging.LogRecord
	switch r.RecordType() {
	case logging.LogInsert:
		clr = txn.NewInsertCLR(r)
	case logging.LogUpdate:
		clr = txn.NewUpdateCLR(r)
	case logging.LogDelete:
		clr = txn.NewDeleteCLR(r)
	default:
		panic("unhandled default case")
	}
	clrLSN, err := tm.logManager.Append(clr)
	if err != nil {
		return err
	}

	hp.PageLatch.Lock()
	switch r.RecordType() {
	case logging.LogInsert:
		hp.MarkDeleted(r.RID(), true)
	case logging.LogUpdate:
		copy(hp.AccessTuple(r.RID()), r.BeforeImage())
	case logging.LogDelete:
		hp.MarkDeleted(r.RID(), false)
	default:
		panic("unhandled default case")
	}
	hp.MonotonicallyUpdateLSN(clrLSN)
	hp.PageLatch.Unlock()
	return nil
}

// </silentstrip>

// ATTEntry represents a snapshot of an active transaction for the Active Transaction Table (ATT).
type ATTEntry struct {
	ID       common.TransactionID
	StartLSN common.LSN
}

// GetActiveTransactionsSnapshot returns a snapshot of currently active transaction IDs and their start LSNs.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) GetActiveTransactionsSnapshot() []ATTEntry {
	// <silentstrip lab1|lab2|lab3|lab4>
	var activeIDs []ATTEntry

	tm.activeTxns.Range(func(tid common.TransactionID, val activeTxnEntry) bool {
		activeIDs = append(activeIDs, ATTEntry{
			ID:       tid,
			StartLSN: val.startLsn,
		})
		return true
	})

	return activeIDs
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
