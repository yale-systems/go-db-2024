package storage

import (
	"runtime"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
)

// <silentstrip lab1|lab2|lab3|lab4>
const maxScanSize = 64

// </silentstrip>

// BufferPool manages the reading and writing of database pages between the DiskFileManager and memory.
// It acts as a central cache to keep "hot" pages in memory with fixed capacity and selectively evicts
// pages to disk when the pool becomes full. Users will need to coordinate concurrent access to pages
// using page-level latches and metadata (which you should define in page.go). All methods
// must be thread-safe, as multiple threads will request the same or different pages concurrently.
// To get full credit, you likely need to do better than coarse-grained latching (i.e., a global latch for the entire
// BufferPool instance).
type BufferPool struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	storageManager DBFileManager
	frames         []PageFrame
	clockHand      uint64
	pageTable      *xsync.MapOf[common.PageID, *PageFrame]
	// </silentstrip>
	// // add more fields here...
	// panic("unimplemented")
	// </insert>
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
func NewBufferPool(numPages int, storageManager DBFileManager) *BufferPool {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &BufferPool{
		storageManager: storageManager,
		frames:         make([]PageFrame, numPages),
		clockHand:      0,
		pageTable:      xsync.NewMapOf[common.PageID, *PageFrame](),
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// StorageManager returns the underlying disk manager.
func (bp *BufferPool) StorageManager() DBFileManager {
	// <silentstrip lab1|lab2|lab3|lab4>
	return bp.storageManager
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>
func tryTouchPage(frame *PageFrame, pageID common.PageID) bool {
	frame.Lock()
	defer frame.Unlock()
	// Must check if this is the page we are looking for! Another thread may have evicted the page after we grabbed
	// this page frame but before we locked it.
	if frame.pageID != pageID {
		return false
	}
	frame.pinCount++
	frame.refBit = true
	return true
}

// Amount of entries we loop through before yielding to avoid busy loop
const strideSize = 64

func (bp *BufferPool) findVictim() (*PageFrame, error) {
	numFrames := uint64(len(bp.frames))
	numIters := 0
	for {
		for i := uint64(0); i < strideSize; i++ {
			idx := atomic.AddUint64(&bp.clockHand, 1) % numFrames

			frame := &bp.frames[idx]
			if !frame.TryLock() {
				// If someone has locked it, we probably cannot evict it!
				continue
			}

			if frame.pinCount > 0 {
				frame.Unlock()
				continue
			}

			// Stop respecting the ref bit if we have scanned for a while and couldn't find a victim
			if numIters >= maxScanSize || !frame.refBit {
				// Return it LOCKED so the caller can safely swap the contents.
				return frame, nil
			}

			// Second chance: clear refBit, unlock, and move on
			frame.refBit = false
			frame.Unlock()
			numIters++
		}
		runtime.Gosched()
	}
}

func (bp *BufferPool) evict(victim *PageFrame) error {
	// victim should be passed in LOCKED
	if victim.pageID.IsNil() {
		return nil
	}
	// Flush the page while holding the latch so others cannot concurrently load it
	if victim.dirty {
		file, err := bp.storageManager.GetDBFile(victim.pageID.Oid)
		if err != nil {
			// frame is returned LOCKED
			return err
		}
		if err = file.WritePage(int(victim.pageID.PageNum), victim.Bytes[:]); err != nil {
			return err
		}
		victim.dirty = false
		victim.recoveryLSN = 0
	}
	return nil
}

// </silentstrip>

// GetPage retrieves a page from the buffer pool, ensuring it is pinned (i.e. prevented from eviction until
// unpinned) and ready for use. If the page is already in the pool, the cached bytes are returned. If the page is not
// present, the method must first make space by selecting a victim frame to evict
// (potentially writing it to disk if dirty), and then read the requested page from disk into that frame.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	// <silentstrip lab1|lab2|lab3|lab4>
	for {
		if frame, ok := bp.pageTable.Load(pageID); ok {
			if tryTouchPage(frame, pageID) {
				return frame, nil
			}
			continue
		}

		file, err := bp.storageManager.GetDBFile(pageID.Oid)
		if err != nil {
			return nil, err
		}

		victimFrame, err := bp.findVictim()
		if err != nil {
			return nil, err
		}
		// victimFrame is returned LOCKED

		// Others may be concurrently loading this page. Attempt to install our victim as the only "official" frame
		// for this PageID before loading. Only the winner loads the page
		actualFrame, loaded := bp.pageTable.LoadOrStore(pageID, victimFrame)

		if loaded {
			// Someone else declared an official frame. We should unlock and wait for them to load it
			victimFrame.Unlock()
			if tryTouchPage(actualFrame, pageID) {
				return actualFrame, nil
			}
			continue
		}

		if err = bp.evict(victimFrame); err != nil {
			victimFrame.Unlock()
			bp.pageTable.Delete(pageID)
			return nil, err
		}

		// Evict AFTER flushing so we don't read the page from disk while flushing it
		bp.pageTable.Delete(victimFrame.pageID)

		if err = file.ReadPage(int(pageID.PageNum), victimFrame.Bytes[:]); err != nil {
			victimFrame.Unlock()
			bp.pageTable.Delete(pageID)
			return nil, err
		}

		victimFrame.pageID = pageID
		victimFrame.pinCount = 1
		// Do not initially set the ref bit -- only on second access do we consider it a true hot page
		victimFrame.refBit = false
		victimFrame.dirty = false
		victimFrame.Unlock()
		return victimFrame, nil
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	// <silentstrip lab1|lab2|lab3|lab4>
	frame.Lock()
	defer frame.Unlock()

	common.Assert(frame.pinCount > 0, "attempting to unpin a page that is not pinned")
	frame.pinCount--
	if setDirty && !frame.dirty {
		frame.dirty = true
		// Technically unnecessary since we know it's an aligned 8-byte read, but just so the go
		// race detector doesn't complain
		frame.PageLatch.Lock()
		frame.recoveryLSN = frame.LSN()
		frame.PageLatch.Unlock()
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a Checkpoint or Shutdown to ensure durability, but also useful for tests
//
// You can ignore the flushedUntil argument until lab 4
func (bp *BufferPool) FlushAllPages(flushedUntil common.LSN) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	for i := 0; i < len(bp.frames); i++ {
		frame := &bp.frames[i]
		frame.Lock()

		if frame.pageID.IsNil() || !frame.dirty || frame.LSN() >= flushedUntil {
			frame.Unlock()
			continue
		}

		// Flush under Read latch and pin to avoid concurrent modification or eviction
		frame.pinCount++
		pageID := frame.pageID
		// grab page latch before we unlock the frame to ensure no writer can change the bytes from under us and that
		// we won't miss a dirty bit
		frame.PageLatch.RLock()
		frame.Unlock()

		file, err := bp.storageManager.GetDBFile(frame.pageID.Oid)
		if err != nil {
			return err
		}
		if err = file.WritePage(int(pageID.PageNum), frame.Bytes[:]); err != nil {
			return err
		}

		frame.Lock()
		common.Assert(frame.pageID == pageID, "pageID should not change during flush")
		frame.pinCount--
		frame.dirty = false
		frame.recoveryLSN = 0
		// unlock page after we have updated it to prevent others from updating it and setting the dirty bit, which we
		// may inadvertently unset in the blind WriteToFullTuple later
		frame.PageLatch.RUnlock()
		frame.Unlock()
	}
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// GetDirtyPageTableSnapshot returns a map of all currently dirty pages and their RecoveryLSN.
// This is used by the Recovery Manager (ARIES) during the Analysis phase to reconstruct the
// state of the database.
//
// Hint: You do not need to worry about this function until lab 4
func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]common.LSN {
	// <silentstrip lab1|lab2|lab3|lab4>
	dpt := make(map[common.PageID]common.LSN)

	bp.pageTable.Range(func(key common.PageID, frame *PageFrame) bool {
		frame.Lock()
		defer frame.Unlock()

		if frame.dirty {
			dpt[key] = frame.recoveryLSN
		}
		return true
	})

	return dpt
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // You will not need to implement this until lab4
	// panic("unimplemented")
	// </insert>
}
