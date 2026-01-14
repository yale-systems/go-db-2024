package execution

import (
	"errors"
	"math/rand"
	"sync"
	"sync/atomic"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/logging"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// <silentstrip lab1|lab2|lab3|lab4>
// densityThreshold: If > 20% of pages are known to be partial (have space),
// we use random probing to find them efficiently without scanning solid blocks.
const densityThreshold = 0.20

// maxProbes: How many random pages to check before giving up and appending.
const maxProbes = 3

// </silentstrip>

// TableHeap represents a physical table stored as a heap file on disk.
// It handles the insertion, update, deletion, and reading of tuples, managing
// interactions with the BufferPool, LockManager, and LogManager.
type TableHeap struct {
	oid         common.ObjectID
	desc        *storage.RawTupleDesc
	bufferPool  *storage.BufferPool
	logManager  logging.LogManager
	lockManager *transaction.LockManager

	// <silentstrip lab1|lab2|lab3|lab4>
	// Stats for heuristics
	numPartialPages atomic.Int32 // Count of pages known to have free space
	numTotalPages   atomic.Int32 // Total pages in the file

	// Guard for file extension to prevent race conditions
	tailLatch sync.Mutex
	// </silentstrip>
}

// NewTableHeap creates a TableHeap and performs a metadata scan to initialize stats.
func NewTableHeap(table *catalog.Table, bufferPool *storage.BufferPool, logManager logging.LogManager, lockManager *transaction.LockManager) (*TableHeap, error) {
	// <silentstrip lab1|lab2|lab3|lab4>
	columnTypes := make([]common.Type, len(table.Columns))
	for i, col := range table.Columns {
		columnTypes[i] = col.Type
	}
	heapFile := &TableHeap{
		oid:         table.Oid,
		desc:        storage.NewRawTupleDesc(columnTypes),
		bufferPool:  bufferPool,
		logManager:  logManager,
		lockManager: lockManager,
	}

	file, err := bufferPool.StorageManager().GetDBFile(table.Oid)
	if err != nil {
		return nil, err
	}

	n, err := file.NumPages()
	if err != nil {
		return nil, err
	}
	heapFile.numTotalPages.Store(int32(n))

	// This is a new file and we should initialize it
	if n == 0 {
		// Create the first two pages, on AMP and one data page
		_, err := file.AllocatePage(2)
		if err != nil {
			return nil, err
		}
		// init new pages
		ampFrame, err := bufferPool.GetPage(common.PageID{Oid: table.Oid, PageNum: 0})
		if err != nil {
			return nil, err
		}
		storage.InitializeAllocationMapPage(ampFrame)
		bufferPool.UnpinPage(ampFrame, true)
		hpFrame, err := bufferPool.GetPage(common.PageID{Oid: table.Oid, PageNum: 1})
		if err != nil {
			return nil, err
		}
		storage.InitializeHeapPage(heapFile.desc, hpFrame)
		bufferPool.UnpinPage(hpFrame, true)

		heapFile.numTotalPages.Store(2)
		heapFile.numPartialPages.Store(1)
		return heapFile, nil
	}

	// This is restarting from a previous file, we should reconstruct in-memory data structures instead
	for currPage := 0; currPage < n; currPage += storage.AllocationMapPageSlots {
		ampID := common.PageID{Oid: heapFile.oid, PageNum: int32(currPage)}

		ampFrame, err := heapFile.bufferPool.GetPage(ampID)
		if err != nil {
			return nil, err
		}

		amp := ampFrame.AsAllocationMapPage()

		// Count free pages in this interval
		limit := n - currPage
		if limit > storage.AllocationMapPageSlots {
			limit = storage.AllocationMapPageSlots
		}

		numPartialPages := 0
		for i := 0; i < limit; i++ {
			if !amp.Bitmap.LoadBit(i) {
				numPartialPages++
			}
		}
		heapFile.numPartialPages.Store(int32(numPartialPages))
		heapFile.bufferPool.UnpinPage(ampFrame, false)
	}

	return heapFile, nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// StorageSchema returns the physical byte-layout descriptor of the tuples in this table.
func (tableHeap *TableHeap) StorageSchema() *storage.RawTupleDesc {
	return tableHeap.desc
}

// <silentstrip lab1|lab2|lab3|lab4>
func isAllocationMapPage(pageNum int32) bool {
	return pageNum%int32(storage.AllocationMapPageSlots) == 0
}

func getAMPForPageNum(pageNum int32) int32 {
	return pageNum / int32(storage.AllocationMapPageSlots) * int32(storage.AllocationMapPageSlots)
}

func getRelativeOffsetToAMP(pageNum int32) int {
	return int(pageNum - pageNum/int32(storage.AllocationMapPageSlots)*int32(storage.AllocationMapPageSlots))
}

func (tableHeap *TableHeap) tryInsert(txn *transaction.TransactionContext, pageID common.PageID, hp storage.HeapPage, row storage.RawTuple, amp storage.AllocationMapPage) (rid common.RecordID, ampDirty bool, err error) {
	hp.PageLatch.Lock()
	defer hp.PageLatch.Unlock()
	slot := hp.FindFreeSlot()
	if slot == -1 {
		// If we couldn't allocate a slot, the page is full. Mark it as full if it's not and return.
		amp.PageLatch.Lock()
		defer amp.PageLatch.Unlock()
		marked := amp.MarkPageFull(getRelativeOffsetToAMP(pageID.PageNum), true)
		if marked {
			// Only update if we are the people who actually marked it full
			tableHeap.numPartialPages.Add(-1)
		}
		return common.RecordID{}, marked, nil
	}

	rid = common.RecordID{
		PageID: pageID,
		Slot:   int32(slot),
	}

	if txn != nil {
		// Go ahead and insert the tuple, but first lock the slot
		err = txn.AcquireLock(transaction.NewTupleLockTag(rid), transaction.LockModeX)
		common.Assert(err != nil, "Should never fail to acquire lock on newly inserted tuple")

		insertRecord := txn.NewInsertRecord(rid, row)
		copy(insertRecord.AfterImage(), row)
		lsn, err := tableHeap.logManager.Append(insertRecord)
		if err != nil {
			return common.RecordID{}, false, err
		}
		hp.MonotonicallyUpdateLSN(lsn)
	}
	hp.MarkAllocated(rid, true)
	copy(hp.AccessTuple(rid), row)
	return rid, false, nil
}

func (tableHeap *TableHeap) tryInsertInPage(txn *transaction.TransactionContext, pageID common.PageID, row storage.RawTuple) (common.RecordID, error) {
	// Find the AMP managing this page and keep it pinned in case we need to update metadata
	ampID := common.PageID{Oid: tableHeap.oid, PageNum: getAMPForPageNum(pageID.PageNum)}
	ampFrame, err := tableHeap.bufferPool.GetPage(ampID)
	if err != nil {
		return common.RecordID{}, err
	}
	targetPID := common.PageID{Oid: tableHeap.oid, PageNum: pageID.PageNum}
	frame, err := tableHeap.bufferPool.GetPage(targetPID)
	if err != nil {
		tableHeap.bufferPool.UnpinPage(ampFrame, false)
		return common.RecordID{}, err
	}

	rid, ampDirty, err := tableHeap.tryInsert(txn, pageID, frame.AsHeapPage(), row, ampFrame.AsAllocationMapPage())
	if err != nil {
		tableHeap.bufferPool.UnpinPage(frame, false)
		tableHeap.bufferPool.UnpinPage(ampFrame, false)
		return common.RecordID{}, err
	}

	tableHeap.bufferPool.UnpinPage(frame, !rid.IsNil())
	tableHeap.bufferPool.UnpinPage(ampFrame, ampDirty)
	return rid, nil
}

func (tableHeap *TableHeap) tryInsertFromRandomStart(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	pageNum := rand.Int31n(tableHeap.numTotalPages.Load())
	// Find the AMP managing this page
	ampID := common.PageID{Oid: tableHeap.oid, PageNum: getAMPForPageNum(pageNum)}

	ampFrame, err := tableHeap.bufferPool.GetPage(ampID)
	if err != nil {
		return common.RecordID{}, err
	}

	amp := ampFrame.AsAllocationMapPage()
	amp.PageLatch.RLock()
	candidateRelativePosition := amp.FindFirstFreePage(int(pageNum) % storage.AllocationMapPageSlots)
	amp.PageLatch.RUnlock()

	if candidateRelativePosition == -1 {
		// No space, return invalid record id so caller retries
		tableHeap.bufferPool.UnpinPage(ampFrame, false)
		return common.RecordID{}, nil
	}

	pageID := common.PageID{Oid: tableHeap.oid, PageNum: ampID.PageNum + int32(candidateRelativePosition)}
	if pageID.PageNum >= tableHeap.numTotalPages.Load() {
		tableHeap.bufferPool.UnpinPage(ampFrame, false)
		return common.RecordID{}, nil
	}
	frame, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		tableHeap.bufferPool.UnpinPage(ampFrame, false)
		return common.RecordID{}, err
	}

	rid, ampDirty, err := tableHeap.tryInsert(txn, pageID, frame.AsHeapPage(), row, ampFrame.AsAllocationMapPage())
	if err != nil {
		tableHeap.bufferPool.UnpinPage(frame, false)
		tableHeap.bufferPool.UnpinPage(ampFrame, false)
		return common.RecordID{}, err
	}
	tableHeap.bufferPool.UnpinPage(frame, !rid.IsNil())
	tableHeap.bufferPool.UnpinPage(ampFrame, ampDirty)
	return rid, nil
}

func (tableHeap *TableHeap) tryExtendFile(expectedPageCount int32) error {
	tableHeap.tailLatch.Lock()
	defer tableHeap.tailLatch.Unlock()
	currentPageCount := tableHeap.numTotalPages.Load()
	if currentPageCount != expectedPageCount {
		// Someone might have extended while we waited for the latch. Nothing to do.
		return nil
	}

	dbFile, err := tableHeap.bufferPool.StorageManager().GetDBFile(tableHeap.oid)
	if err != nil {
		return err
	}
	newPageNum, err := dbFile.AllocatePage(1)
	if err != nil {
		return err
	}

	common.Assert(newPageNum == int(currentPageCount), "Page allocation should be sequential")

	// Check if we need a new AMP
	if isAllocationMapPage(currentPageCount) {
		frame, err := tableHeap.bufferPool.GetPage(common.PageID{Oid: tableHeap.oid, PageNum: currentPageCount})
		if err != nil {
			return err
		}
		storage.InitializeAllocationMapPage(frame)
		tableHeap.numTotalPages.Add(1)
		tableHeap.bufferPool.UnpinPage(frame, true)
	}
	// Allocate data Page
	frame, err := tableHeap.bufferPool.GetPage(common.PageID{Oid: tableHeap.oid, PageNum: currentPageCount})
	if err != nil {
		return err
	}
	storage.InitializeHeapPage(tableHeap.desc, frame)
	tableHeap.numTotalPages.Add(1)
	tableHeap.numPartialPages.Add(1)
	tableHeap.bufferPool.UnpinPage(frame, true)
	return nil
}

func (tableHeap *TableHeap) insertAtTail(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	for {
		tail := tableHeap.numTotalPages.Load() - 1
		common.Assert(tail >= 1, "TableHeap should always have at least 2 pages")

		// The tail could be an AMP if someone else is extending the heap or if we crashed before
		if !isAllocationMapPage(tail) {
			rid, err := tableHeap.tryInsertInPage(txn, common.PageID{
				Oid:     tableHeap.oid,
				PageNum: tail,
			}, row)
			if err != nil {
				return common.RecordID{}, err
			}
			if !rid.IsNil() {
				return rid, nil
			}
		}

		// Otherwise, we need to try allocating a new one and retry
		if err := tableHeap.tryExtendFile(tail + 1); err != nil {
			return common.RecordID{}, err
		}
	}
}

// </silentstrip>

// InsertTuple inserts a tuple into the TableHeap. It should find a free space, allocating if needed, and return the found slot.
func (tableHeap *TableHeap) InsertTuple(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	// <silentstrip lab1|lab2|lab3|lab4>
	if txn != nil {
		if err := txn.AcquireLock(transaction.NewTableLockTag(tableHeap.oid), transaction.LockModeIX); err != nil {
			return common.RecordID{}, err
		}
	}
	density := float64(tableHeap.numPartialPages.Load()) / float64(tableHeap.numTotalPages.Load())
	if density >= densityThreshold {
		for i := 0; i < maxProbes; i++ {
			rid, err := tableHeap.tryInsertFromRandomStart(txn, row)
			if err != nil {
				return common.RecordID{}, err
			}
			if !rid.IsNil() {
				return rid, nil
			}
		}
	}
	return tableHeap.insertAtTail(txn, row)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

var ErrTupleDeleted = errors.New("tuple has been deleted")

// DeleteTuple marks a tuple as deleted in the TableHeap. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) DeleteTuple(txn *transaction.TransactionContext, rid common.RecordID) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	if txn != nil {
		if err := txn.AcquireLock(transaction.NewTableLockTag(rid.PageID.Oid), transaction.LockModeIX); err != nil {
			return err
		}
		if err := txn.AcquireLock(transaction.NewTupleLockTag(rid), transaction.LockModeX); err != nil {
			return err
		}

	}

	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}
	hp := frame.AsHeapPage()
	hp.PageLatch.Lock()
	common.Assert(hp.IsAllocated(rid), "DeleteTuple should never delete deallocated tuples")
	if hp.IsDeleted(rid) {
		hp.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(frame, false)
		return ErrTupleDeleted
	}
	if txn != nil {
		deleteRecord := txn.NewDeleteRecord(rid)
		lsn, err := tableHeap.logManager.Append(deleteRecord)
		if err != nil {
			return err
		}
		hp.MonotonicallyUpdateLSN(lsn)
	}
	hp.MarkDeleted(rid, true)
	hp.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(frame, true)
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// ReadTuple reads the physical bytes of a tuple into the provided buffer. If forUpdate is true, read should acquire
// exclusive lock instead of shared. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) ReadTuple(txn *transaction.TransactionContext, rid common.RecordID, buffer []byte, forUpdate bool) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	if txn != nil {
		var tableLockMode, tupleLockMode transaction.DBLockMode
		if forUpdate {
			tableLockMode, tupleLockMode = transaction.LockModeIX, transaction.LockModeX
		} else {
			tableLockMode, tupleLockMode = transaction.LockModeIS, transaction.LockModeS
		}

		if err := txn.AcquireLock(transaction.NewTableLockTag(rid.PageID.Oid), tableLockMode); err != nil {
			return err
		}
		if err := txn.AcquireLock(transaction.NewTupleLockTag(rid), tupleLockMode); err != nil {
			return err
		}
	}

	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}

	hp := frame.AsHeapPage()
	hp.PageLatch.RLock()
	common.Assert(hp.IsAllocated(rid), "ReadTuple should never read deallocated tuples")
	deleted := hp.IsDeleted(rid)
	if !deleted {
		copy(buffer, hp.AccessTuple(rid))
	}
	hp.PageLatch.RUnlock()
	tableHeap.bufferPool.UnpinPage(frame, false)
	if deleted {
		return ErrTupleDeleted
	}
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// UpdateTuple updates a tuple in-place with new binary data. If the tuple has been deleted, return ErrTupleDeleted.
func (tableHeap *TableHeap) UpdateTuple(txn *transaction.TransactionContext, rid common.RecordID, updatedTuple storage.RawTuple) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	if txn != nil {
		if err := txn.AcquireLock(transaction.NewTableLockTag(rid.PageID.Oid), transaction.LockModeIX); err != nil {
			return err
		}
		if err := txn.AcquireLock(transaction.NewTupleLockTag(rid), transaction.LockModeX); err != nil {
			return err
		}
	}
	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}

	// Must add record after we are sure this update will happen, otherwise will undo something that hasn't happened
	hp := frame.AsHeapPage()
	hp.PageLatch.Lock()
	common.Assert(hp.IsAllocated(rid), "UpdateTuple should never update deallocated tuples")
	if hp.IsDeleted(rid) {
		hp.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(frame, false)
		return ErrTupleDeleted
	}
	slice := hp.AccessTuple(rid)
	if txn != nil {
		updateRecord := txn.NewUpdateRecord(rid, slice, updatedTuple)
		lsn, err := tableHeap.logManager.Append(updateRecord)
		if err != nil {
			hp.PageLatch.Unlock()
			tableHeap.bufferPool.UnpinPage(frame, false)
			return err
		}
		hp.MonotonicallyUpdateLSN(lsn)
	}
	copy(slice, updatedTuple)
	hp.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(frame, true)
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// VacuumPage attempts to clean up deleted slots on a specific page.
// If slots are deleted AND no transaction holds a lock on them, they are marked as free.
// This is used to reclaim space in the background.
func (tableHeap *TableHeap) VacuumPage(pageID common.PageID) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	ampPid := common.PageID{Oid: tableHeap.oid, PageNum: getAMPForPageNum(pageID.PageNum)}
	ampFrame, err := tableHeap.bufferPool.GetPage(ampPid)
	if err != nil {
		return err
	}

	page, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		tableHeap.bufferPool.UnpinPage(ampFrame, false)
		return err
	}

	hp := page.AsHeapPage()
	hp.PageLatch.Lock()
	freed := 0
	for i := 0; i < hp.NumSlots(); i++ {
		rid := common.RecordID{PageID: pageID, Slot: int32(i)}
		if hp.IsDeleted(rid) && !tableHeap.lockManager.LockHeld(transaction.NewTupleLockTag(rid)) {
			hp.MarkAllocated(rid, false)
			freed++
		}
	}
	if freed > 0 {
		amp := ampFrame.AsAllocationMapPage()
		amp.PageLatch.Lock()
		if amp.MarkPageFull(getRelativeOffsetToAMP(pageID.PageNum), false) {
			tableHeap.numPartialPages.Add(1)
		}
		amp.PageLatch.Unlock()
	}
	hp.PageLatch.Unlock()
	tableHeap.bufferPool.UnpinPage(page, freed > 0)
	tableHeap.bufferPool.UnpinPage(ampFrame, freed > 0)
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Iterator creates a new TableHeapIterator to scan the table. It acquires the supplied lock on the table (S, X, or SIX),
// and uses the supplied byte slice to fetch tuples in the returned iterator (for zero-allocation scanning).
func (tableHeap *TableHeap) Iterator(txn *transaction.TransactionContext, mode transaction.DBLockMode, buffer []byte) (TableHeapIterator, error) {
	// <silentstrip lab1|lab2|lab3|lab4>
	if txn != nil {
		common.Assert(mode != transaction.LockModeIS && mode != transaction.LockModeIX, "TableHeapIterator should only grab S, X, or SIX locks")
		err := txn.AcquireLock(transaction.NewTableLockTag(tableHeap.oid), mode)
		if err != nil {
			return TableHeapIterator{}, nil
		}
	}

	return TableHeapIterator{
		tableHeap: tableHeap,
		buffer:    buffer,
		currRID: common.RecordID{
			PageID: common.PageID{
				Oid:     tableHeap.oid,
				PageNum: 0,
			},
			Slot: -1,
		},
	}, nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// TableHeapIterator iterates over all valid (allocated and non-deleted) tuples in the heap.
type TableHeapIterator struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	tableHeap *TableHeap
	buffer    []byte

	currRID  common.RecordID
	currPage *storage.PageFrame // Keep the current page pinned while iterating its slots

	err error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

// IsNil returns true if the TableHeapIterator is the default, uninitialized value
func (it *TableHeapIterator) IsNil() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	return it.tableHeap == nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Next advances the iterator to the next valid tuple.
// It manages page pins automatically (unpinning the old page when moving to a new one).
func (it *TableHeapIterator) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if it.err != nil {
		return false
	}
	numPages := it.tableHeap.numTotalPages.Load()

	for it.currRID.PageNum < numPages {
		if isAllocationMapPage(it.currRID.PageNum) {
			it.currRID.PageNum++
			it.currPage = nil
			continue
		}

		if it.currPage == nil {
			page, err := it.tableHeap.bufferPool.GetPage(it.currRID.PageID)
			if err != nil {
				it.err = err
				return false
			}
			it.currPage = page
		}

		hp := it.currPage.AsHeapPage()
		hp.PageLatch.RLock()

		numSlots := hp.NumSlots()
		foundSlot := -1
		for i := int(it.currRID.Slot + 1); i < numSlots; i++ {
			rid := common.RecordID{PageID: it.currRID.PageID, Slot: int32(i)}
			// Check allocation first to avoid assertions
			if hp.IsAllocated(rid) && !hp.IsDeleted(rid) {
				foundSlot = i
				break
			}
		}

		if foundSlot == -1 {
			hp.PageLatch.RUnlock()
			it.tableHeap.bufferPool.UnpinPage(it.currPage, false)

			it.currPage = nil
			it.currRID.PageID = common.PageID{Oid: it.currRID.PageID.Oid, PageNum: it.currRID.PageID.PageNum + 1}
			it.currRID.Slot = -1
			continue
		}

		it.currRID.Slot = int32(foundSlot)

		copy(it.buffer, hp.AccessTuple(it.currRID))
		hp.PageLatch.RUnlock()
		return true
	}
	return false
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// CurrentTuple returns the raw bytes of the tuple at the current cursor position.
// The bytes are valid only until Next() is called again.
func (it *TableHeapIterator) CurrentTuple() storage.RawTuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return it.buffer
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// CurrentRID returns the RecordID of the current tuple.
func (it *TableHeapIterator) CurrentRID() common.RecordID {
	// <silentstrip lab1|lab2|lab3|lab4>
	return it.currRID
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// CurrentRID returns the first error encountered during iteration, if any.
func (it *TableHeapIterator) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return it.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Close releases any resources associated with the TableHeapIterator
func (it *TableHeapIterator) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	// If we are holding a page pin, release it
	if it.currPage != nil {
		it.tableHeap.bufferPool.UnpinPage(it.currPage, false)
	}
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
