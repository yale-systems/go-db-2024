package recovery

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/execution"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/logging"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// MasterRecordFileName is the file used to bootstrap recovery.
// Currently, it stores the LSN of the last CheckpointBegin record.
// In the future, this can be expanded to store indexing snapshots or other metadata.
const MasterRecordFileName = "checkpoint.dat"

// RecoveryManager implements the ARIES recovery protocol.
// It coordinates interactions between the Log Manager, Buffer Pool, and Transaction Manager
// to ensure database consistency and durability in the event of a crash.
type RecoveryManager struct {
	logManager         logging.LogManager
	bufferPool         *storage.BufferPool
	transactionManager *transaction.TransactionManager
	checkpointPath     string
	catalog            *catalog.Catalog
	indexManager       *indexing.IndexManager
	tableManager       *execution.TableManager

	// <silentstrip lab1|lab2|lab3|lab4>
	activeTransactions map[common.TransactionID]*transaction.TransactionContext
	dirtyPages         map[common.PageID]common.LSN
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Add more fields
	// </insert>
}

// NewRecoveryManager initializes a new RecoveryManager.
func NewRecoveryManager(
	logManager logging.LogManager,
	bufferPool *storage.BufferPool,
	transactionManager *transaction.TransactionManager,
	logPath string,
	catalog *catalog.Catalog,
	tableManager *execution.TableManager,
	indexManager *indexing.IndexManager) *RecoveryManager {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &RecoveryManager{
		logManager:         logManager,
		bufferPool:         bufferPool,
		transactionManager: transactionManager,
		checkpointPath:     logPath,
		catalog:            catalog,
		tableManager:       tableManager,
		indexManager:       indexManager,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Checkpoint creates a fuzzy checkpoint to speed up recovery and returns the LSN up to which the log can be truncated.
func (rm *RecoveryManager) Checkpoint() (common.LSN, error) {
	// <silentstrip lab1|lab2|lab3|lab4>
	beginRecordBuf := make([]byte, logging.BeginCheckpointRecordSize())
	beginRecord := logging.NewBeginCheckpointRecord(beginRecordBuf)
	beginLSN, err := rm.logManager.Append(beginRecord)
	if err != nil {
		return 0, err
	}

	dpt := rm.bufferPool.GetDirtyPageTableSnapshot()
	att := rm.transactionManager.GetActiveTransactionsSnapshot()

	checkpointDataSize := rm.checkpointDataSize(dpt, att)
	endRecordBuf := make([]byte, logging.EndCheckpointRecordSize(checkpointDataSize))
	endRecord := logging.NewEndCheckpointRecord(endRecordBuf, checkpointDataSize)
	rm.writeCheckpointData(endRecord.CheckpointData(), dpt, att)

	endLSN, err := rm.logManager.Append(endRecord)
	if err != nil {
		return 0, err
	}

	if err := rm.logManager.WaitUntilFlushed(endLSN); err != nil {
		return 0, err
	}

	// Start with beginLSN. If we have no dirty pages, we can technically truncate
	// everything before this checkpoint started.
	truncationLSN := beginLSN
	for _, recLSN := range dpt {
		if recLSN < truncationLSN {
			truncationLSN = recLSN
		}
	}

	// also check the StartLSN of all active transactions in 'att' and take the minimum of that too
	// (min(minRecLSN, minTxnStartLSN)).
	for _, entry := range att {
		if entry.StartLSN < truncationLSN {
			truncationLSN = entry.StartLSN
		}
	}

	return truncationLSN, rm.writeMasterRecordFile(beginLSN)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Recover performs the ARIES recovery protocol upon a crash.
func (rm *RecoveryManager) Recover() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	checkpointLSN, err := rm.readMasterRecordFile()
	if os.IsNotExist(err) {
		// No checkpoint exists, start from beginning of log
		checkpointLSN = 0
	} else if err != nil {
		return err
	}

	scanStart, lastRecord, err := rm.analysisPhase(checkpointLSN)
	if err != nil {
		return fmt.Errorf("analysis phase failed: %w", err)
	}

	if err := rm.redoPhase(scanStart, lastRecord); err != nil {
		return fmt.Errorf("redo phase failed: %w", err)
	}

	return rebuildIndexes(rm.catalog, rm.tableManager, rm.indexManager)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>

func rebuildIndexes(
	catalog *catalog.Catalog,
	tableManager *execution.TableManager,
	indexManager *indexing.IndexManager) error {

	for _, tableDef := range catalog.Tables {
		// Skip tables without indexes to avoid unnecessary scans
		if len(tableDef.Indexes) == 0 {
			continue
		}

		heap, err := tableManager.GetTable(tableDef.Oid)
		if err != nil {
			return err
		}

		// Resolve all index objects for this table
		var activeIndexes []indexing.Index
		var keyBuffers [][]byte

		for _, i := range tableDef.Indexes {
			indexObject, err := indexManager.GetIndex(i.Oid)
			if err != nil {
				return err
			}
			activeIndexes = append(activeIndexes, indexObject)
			keyBuffers = append(keyBuffers, make([]byte, indexObject.Metadata().KeySchema.BytesPerTuple()))
		}

		// Scan the table to rebuild indexes.
		// We use a nil transaction because we are in recovery mode (single-threaded).
		iter, err := heap.Iterator(nil, transaction.LockModeS, make([]byte, heap.StorageSchema().BytesPerTuple()))
		if err != nil {
			return err
		}

		for iter.Next() {
			tupleBytes := iter.CurrentTuple()
			rid := iter.CurrentRID()

			for i, index := range activeIndexes {
				// Project the values from the main tuple into the key tuple
				for k, colIdx := range index.Metadata().ProjectionList {
					// Extract value from the heap tuple (using table schema)
					val := heap.StorageSchema().GetValue(tupleBytes, colIdx)
					// Write value to the key buffer (using key schema)
					index.Metadata().KeySchema.SetValue(keyBuffers[i], k, val)
				}

				key := index.Metadata().AsKey(keyBuffers[i])
				if err := index.InsertEntry(key, rid, nil); err != nil {
					_ = iter.Close()
					return err
				}
			}
		}
		_ = iter.Close()
		if err := iter.Error(); err != nil {
			return err
		}
	}
	return nil
}

func (rm *RecoveryManager) analysisPhase(startLSN common.LSN) (scanStart common.LSN, lastRecord common.LSN, err error) {
	iter, err := rm.logManager.Iterator(startLSN)
	if err != nil {
		return common.LSN(0), common.LSN(0), err
	}
	defer iter.Close()

	scanStart = startLSN
	finishedTransactions := make(map[common.TransactionID]any)
	for iter.Next() {
		r := iter.CurrentRecord()
		lsn := iter.CurrentLSN()
		lastRecord = lsn
		switch r.RecordType() {
		case logging.LogBeginTransaction:
			rm.activeTransactions[r.TxnID()] = nil
		case logging.LogCommit:
			if _, exists := rm.activeTransactions[r.TxnID()]; exists {
				delete(rm.activeTransactions, r.TxnID())
			} else {
				// If this is the case, then Begin is beyond the start of the checkpoint, and we would be included
				// in the ATT.
				// Write it down if we are actually committed, so a later checkpoint does not add us back in to active transactions
				finishedTransactions[r.TxnID()] = nil
			}
		case logging.LogEndCheckpoint:
			snapshotDPT, snapshotATT := rm.deserializeCheckpointData(r.CheckpointData())
			for pid, recLSN := range snapshotDPT {
				rm.dirtyPages[pid] = recLSN
				if recLSN < scanStart {
					scanStart = recLSN
				}
			}
			for _, entry := range snapshotATT {
				if _, exists := finishedTransactions[entry.ID]; exists {
					delete(finishedTransactions, entry.ID)
					continue
				}
				rm.activeTransactions[entry.ID] = nil
				if entry.StartLSN < scanStart {
					scanStart = entry.StartLSN
				}
			}
		case logging.LogInsert, logging.LogUpdate, logging.LogDelete:
			pid := r.RID().PageID
			if _, exists := rm.dirtyPages[pid]; !exists {
				rm.dirtyPages[pid] = lsn
			}
		default:
			continue
		}
	}

	return scanStart, lastRecord, nil
}

func (rm *RecoveryManager) redoPhase(scanStart common.LSN, lastRecord common.LSN) error {
	iter, err := rm.logManager.Iterator(scanStart)
	if err != nil {
		return err
	}
	defer iter.Close()

	for iter.Next() {
		r := iter.CurrentRecord()
		lsn := iter.CurrentLSN()

		if txn, exists := rm.activeTransactions[r.TxnID()]; exists {
			if r.RecordType() == logging.LogBeginTransaction {
				rm.activeTransactions[r.TxnID()] = rm.transactionManager.RestartTransactionForRecovery(r.TxnID())
				continue
			}
			txn.BufferRecordForRecovery(r)
		}

		if redoable(r) {
			page, err := rm.bufferPool.GetPage(r.RID().PageID)
			if err != nil {
				return err
			}
			hp := page.AsHeapPage()
			rm.redo(lsn, r, hp)
			rm.bufferPool.UnpinPage(page, true)
		}

		if lsn == lastRecord {
			break
		}
	}

	// At the end, these transactions are presumed abort: we need to actually go through the real abort code path and generate abort records
	for _, txn := range rm.activeTransactions {
		err := rm.transactionManager.Abort(txn)
		if err != nil {
			return err
		}
	}
	return nil
}

func redoable(r logging.LogRecord) bool {
	switch r.RecordType() {
	case logging.LogInsert, logging.LogUpdate, logging.LogDelete, logging.LogInsertCLR, logging.LogUpdateCLR, logging.LogDeleteCLR:
		return true
	default:
		return false
	}
}

func (rm *RecoveryManager) redo(lsn common.LSN, r logging.LogRecord, hp storage.HeapPage) {
	switch r.RecordType() {
	case logging.LogInsert:
		hp.MarkAllocated(r.RID(), true)
		fallthrough
	case logging.LogUpdate:
		copy(hp.AccessTuple(r.RID()), r.AfterImage())
	case logging.LogDelete:
		hp.MarkDeleted(r.RID(), true)
	case logging.LogInsertCLR:
		hp.MarkDeleted(r.RID(), true)
	case logging.LogUpdateCLR:
		copy(hp.AccessTuple(r.RID()), r.AfterImage())
	case logging.LogDeleteCLR:
		hp.MarkDeleted(r.RID(), false)
	default:
		panic("unexpected log record type")
	}
	hp.MonotonicallyUpdateLSN(lsn)
}

func (rm *RecoveryManager) writeMasterRecordFile(lsn common.LSN) error {
	path := filepath.Join(rm.checkpointPath, MasterRecordFileName)
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, uint64(lsn))
	_, err = f.Write(buf)
	return err
}

func (rm *RecoveryManager) readMasterRecordFile() (common.LSN, error) {
	path := filepath.Join(rm.checkpointPath, MasterRecordFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	if len(data) < 8 {
		return 0, fmt.Errorf("checkpoint file corrupted")
	}
	// In the future, you would read additional bytes here to load indexing snapshots.
	return common.LSN(binary.LittleEndian.Uint64(data)), nil
}

func (rm *RecoveryManager) checkpointDataSize(dpt map[common.PageID]common.LSN, att []transaction.ATTEntry) int {
	// size of att (4) | att entries (16 bytes each) | size of dpt (4) | dpt entries (16 bytes each)
	return 8 + 16*len(att) + 16*len(dpt)

}

func (rm *RecoveryManager) writeCheckpointData(buf []byte, dpt map[common.PageID]common.LSN, att []transaction.ATTEntry) {
	common.Assert(len(buf) >= rm.checkpointDataSize(dpt, att), "buffer too small")
	offset := 0
	// Write ATT
	binary.LittleEndian.PutUint32(buf[offset:], uint32(len(att)))
	offset += 4
	for _, entry := range att {
		binary.LittleEndian.PutUint64(buf[offset:], uint64(entry.ID))
		offset += 8
		binary.LittleEndian.PutUint64(buf[offset:], uint64(entry.StartLSN))
		offset += 8
	}

	// Write DPT
	binary.LittleEndian.PutUint32(buf, uint32(len(dpt)))
	offset += 4
	for pid, lsn := range dpt {
		pid.WriteTo(buf[offset:])
		offset += common.PageIDSize
		binary.LittleEndian.PutUint64(buf[offset:], uint64(lsn))
		offset += 8
	}
}

func (rm *RecoveryManager) deserializeCheckpointData(data []byte) (dpt map[common.PageID]common.LSN, att []transaction.ATTEntry) {
	pos := 0
	numTxns := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	att = make([]transaction.ATTEntry, numTxns)
	for i := 0; i < numTxns; i++ {
		id := common.TransactionID(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		startLSN := common.LSN(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		att[i] = transaction.ATTEntry{
			ID:       id,
			StartLSN: startLSN,
		}
	}

	numPages := int(binary.LittleEndian.Uint32(data[pos:]))
	pos += 4
	dpt = make(map[common.PageID]common.LSN, numPages)
	for i := 0; i < numPages; i++ {
		pid := common.PageID{}
		pid.LoadFrom(data[pos:])
		pos += common.PageIDSize
		lsn := common.LSN(binary.LittleEndian.Uint64(data[pos:]))
		pos += 8
		dpt[pid] = lsn
	}
	return dpt, att
}

// </silentstrip>
