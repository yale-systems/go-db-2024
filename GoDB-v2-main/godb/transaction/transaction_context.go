package transaction

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/logging"
	"mit.edu/dsg/godb/storage"
)

// logRecordBuffer manages a contiguous block of memory for transaction rollback logs.
// It allows efficient allocation and reuse of memory for LogRecords, as well as for the transaction
// to maintain a history of its operations for efficient undo without scanning the log.
type logRecordBuffer struct {
	// buffer holds the serialized undo records.
	buffer []byte

	// offsets tracks the starting indexing of each record in the buffer.
	offsets []int
}

// newLogRecordBuffer creates a stack with some pre-allocated capacity.
func newLogRecordBuffer() *logRecordBuffer {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &logRecordBuffer{
		buffer:  make([]byte, 0, 4096),
		offsets: make([]int, 0, 16),
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// allocate reserves `totalSize` bytes in the buffer for a new record.
// It returns a slice referencing the allocated space.
// It also records the offset of this new record, effectively pushing it onto the stack.
func (s *logRecordBuffer) allocate(totalSize int) []byte {
	// <silentstrip lab1|lab2|lab3|lab4>
	currLen := len(s.buffer)

	// Ensure capacity
	if cap(s.buffer) < currLen+totalSize {
		newCap := max(2*cap(s.buffer), currLen+totalSize)
		newBuf := make([]byte, currLen, newCap)
		copy(newBuf, s.buffer)
		s.buffer = newBuf
	}

	// Extend buffer
	s.buffer = s.buffer[:currLen+totalSize]
	s.offsets = append(s.offsets, currLen)
	return s.buffer[currLen : totalSize+currLen]
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// len returns the number of records currently stored in the buffer.
func (s *logRecordBuffer) len() int {
	// <silentstrip lab1|lab2|lab3|lab4>
	return len(s.offsets)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// get returns the LogRecord at the specified index `i`.
// The index `i` corresponds to the order of insertion (0 is the first record).
func (s *logRecordBuffer) get(i int) logging.LogRecord {
	// <silentstrip lab1|lab2|lab3|lab4>
	var end int
	if i == len(s.offsets)-1 {
		end = len(s.buffer)
	} else {
		i = s.offsets[i+1]
	}

	return logging.AsLogRecord(s.buffer[s.offsets[i]:end])
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// pop removes the most recently added record from the buffer.
// This effectively rewinds the stack by one record.
func (s *logRecordBuffer) pop() {
	// <silentstrip lab1|lab2|lab3|lab4>
	i := s.len() - 1
	offset := s.offsets[i]
	s.offsets = s.offsets[:i]
	s.buffer = s.buffer[:offset]
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// reset clears the buffer (sets length to 0) without releasing the underlying memory.
// This is used when reusing the TransactionContext.
func (s *logRecordBuffer) reset() {
	// <silentstrip lab1|lab2|lab3|lab4>
	s.buffer = s.buffer[:0]
	s.offsets = s.offsets[:0]
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// TransactionContext holds the runtime state of a single transaction.
type TransactionContext struct {
	id         common.TransactionID
	lm         *LockManager
	logRecords *logRecordBuffer
	heldLocks  map[DBLockTag]DBLockMode

	// cleanupStack holds in-memory undo actions (e.g., indexing rollbacks). This is used because in GoDB, indexes
	// are memory-only for simplicity, and do not need to participate in the WAL-driven recovery process.
	// In a real DBMS, indexes are often also on disk and must be protected by a WAL; in fact, indexing recovery is
	// often much more complicated than just the table storage itself, due to multi-page structural modifications
	// (e.g., B-tree merge/split). YOU SHOULD NOT NEED TO MANIPULATE THIS FIELD
	cleanupStack []IndexCleanupTask
}

// AddCleanup registers a function to be executed if the transaction Aborts.
// YOU SHOULD NOT NEED TO CALL OR MODIFY THIS FUNCTION
func (txn *TransactionContext) AddCleanup(task IndexCleanupTask) {
	txn.cleanupStack = append(txn.cleanupStack, task)
}

// AcquireLock attempts to acquire a lock on the specified resource, checking for reentrancy (if the lock is already
// held).  If the lock cannot be acquired immediately, this call may block or fail due
// to a deadlock.
func (txn *TransactionContext) AcquireLock(tag DBLockTag, mode DBLockMode) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	if _, ok := txn.heldLocks[tag]; ok {
		// Reentrancy
		if CoveredBy(mode, txn.heldLocks[tag]) {
			return nil
		}
	}
	if err := txn.lm.Lock(txn.id, tag, mode); err != nil {
		return err
	}
	txn.heldLocks[tag] = mode
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// ReleaseAllLocks releases all locks held by this transaction.
// This is typically called during the Commit or Abort phase of the transaction lifecycle.
func (txn *TransactionContext) ReleaseAllLocks() {
	// <silentstrip lab1|lab2|lab3|lab4>
	for lock, _ := range txn.heldLocks {
		txn.lm.Unlock(txn.id, lock)
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Reset clears the transaction context for reuse.
// This is critical when using sync.Pool to avoid leaking data between users.
func (txn *TransactionContext) Reset(id common.TransactionID) {
	// <silentstrip lab1|lab2|lab3|lab4>
	txn.id = id
	// reset the logRecordBuffer without deallocating its internal buffer
	txn.logRecords.reset()
	clear(txn.heldLocks)

	txn.cleanupStack = txn.cleanupStack[:0]
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// NewBeginTransactionRecord creates a 'Begin' log record using the context's buffer.
func (txn *TransactionContext) NewBeginTransactionRecord() logging.LogRecord {
	buf := txn.logRecords.allocate(logging.BeginTransactionRecordSize())
	return logging.NewBeginTransactionRecord(buf, txn.id)
}

// NewCommitRecord creates a 'Commit' log record using the context's buffer.
func (txn *TransactionContext) NewCommitRecord() logging.LogRecord {
	buf := txn.logRecords.allocate(logging.CommitRecordSize())
	return logging.NewCommitRecord(buf, txn.id)
}

// NewAbortRecord creates an 'Abort' log record using the context's buffer.
func (txn *TransactionContext) NewAbortRecord() logging.LogRecord {
	buf := txn.logRecords.allocate(logging.AbortRecordSize())
	return logging.NewAbortRecord(buf, txn.id)
}

// NewInsertCLR creates a Compensation Log Record (CLR) for an Insert operation.
func (txn *TransactionContext) NewInsertCLR(insertRecord logging.LogRecord) logging.LogRecord {
	buf := txn.logRecords.allocate(logging.InsertCLRSize())
	return logging.NewInsertCLR(buf, insertRecord)
}

// NewInsertRecord creates a log record for an Insert operation.
func (txn *TransactionContext) NewInsertRecord(rid common.RecordID, row storage.RawTuple) logging.LogRecord {
	buf := txn.logRecords.allocate(logging.InsertRecordSize(row))
	return logging.NewInsertRecord(buf, txn.id, rid, row)
}

// NewDeleteCLR creates a Compensation Log Record (CLR) for a Delete operation.
func (txn *TransactionContext) NewDeleteCLR(deleteRecord logging.LogRecord) logging.LogRecord {
	buf := txn.logRecords.allocate(logging.DeleteCLRSize())
	return logging.NewDeleteCLR(buf, deleteRecord)
}

// NewDeleteRecord creates a log record for a Delete operation.
func (txn *TransactionContext) NewDeleteRecord(rid common.RecordID) logging.LogRecord {
	buf := txn.logRecords.allocate(logging.DeleteRecordSize())
	return logging.NewDeleteRecord(buf, txn.id, rid)
}

// NewUpdateCLR creates a Compensation Log Record (CLR) for an Update operation.
func (txn *TransactionContext) NewUpdateCLR(updateRecord logging.LogRecord) logging.LogRecord {
	buf := txn.logRecords.allocate(logging.UpdateCLRSize(updateRecord))
	return logging.NewUpdateCLR(buf, updateRecord)
}

// NewUpdateRecord creates a log record for an Update operation.
func (txn *TransactionContext) NewUpdateRecord(rid common.RecordID, before, after storage.RawTuple) logging.LogRecord {
	buf := txn.logRecords.allocate(logging.UpdateRecordSize(before, after))
	return logging.NewUpdateRecord(buf, txn.id, rid, before, after)
}

// BufferRecordForRecovery updates the transaction's internal undo log when replaying for recovery
//
// Hint: You do not need to worry about this function until lab 4
func (txn *TransactionContext) BufferRecordForRecovery(r logging.LogRecord) {
	// <silentstrip lab1|lab2|lab3|lab4>
	if r.IsCLR() {
		// Pop the last record from the stack instead
		txn.logRecords.pop()
		return
	}
	buf := txn.logRecords.allocate(r.Size())
	logging.CreateCopy(buf, r)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
