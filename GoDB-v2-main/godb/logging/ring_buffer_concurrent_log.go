package logging

import (
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"mit.edu/dsg/godb/common"
)

const numLogBuffers = 16

// ringLogBuffer represents a fixed-size memory segment in the WAL ring.
type ringLogBuffer struct {
	data [logBufferSize]byte

	// currentOffset indicates how many bytes have been reserved.
	// If negative, the buffer is sealed.
	currentOffset atomic.Int64

	// startLSN is the Log Sequence Number corresponding to data[0].
	startLSN common.LSN

	// endLSN tracks the highest LSN inside this buffer.
	endLSN common.LSN

	sync.RWMutex
}

func (b *ringLogBuffer) reset(startLSN common.LSN) {
	b.startLSN = startLSN
	b.endLSN = 0
	b.currentOffset.Store(0)
}

func (b *ringLogBuffer) tryReserve(size int) int {
	for {
		cur := b.currentOffset.Load()
		if cur < 0 || cur+int64(size) > logBufferSize {
			return -1
		}
		if b.currentOffset.CompareAndSwap(cur, cur+int64(size)) {
			return int(cur)
		}
	}
}

func (b *ringLogBuffer) seal() (bool, common.LSN) {
	for {
		cur := b.currentOffset.Load()
		if cur < 0 {
			return false, b.endLSN
		}
		// Attempt to flip sign to mark sealed
		if b.currentOffset.CompareAndSwap(cur, -cur) {
			size := cur
			// CRITICAL FIX: Store the endLSN so we can check if it's flushed later
			b.endLSN = b.startLSN + common.LSN(size)
			return true, b.endLSN
		}
	}
}

func (b *ringLogBuffer) sealed() bool {
	return b.currentOffset.Load() < 0
}

func (b *ringLogBuffer) append(logFile *os.File) (common.LSN, error) {
	b.Lock()
	defer b.Unlock()

	size := -b.currentOffset.Load()
	if size <= 0 {
		return b.startLSN, nil
	}
	if _, err := logFile.Write(b.data[:size]); err != nil {
		return common.LSN(-1), err
	}
	return b.startLSN + common.LSN(size), nil
}

func (b *ringLogBuffer) appendToBuffer(record LogRecord) common.LSN {
	b.RLock()
	defer b.RUnlock()

	if b.sealed() {
		return -1
	}
	offset := b.tryReserve(record.Size())
	if offset == -1 {
		return -1
	}

	lsn := b.startLSN + common.LSN(offset)
	record.WriteToLog(b.data[offset:])
	return lsn
}

// RingBufferConcurrentLogManager implements the Ring Buffer WAL.
type RingBufferConcurrentLogManager struct {
	buffers     [numLogBuffers]*ringLogBuffer
	activeIndex atomic.Uint32

	flushedLSN atomic.Int64
	logFile    *os.File

	// Synchronization
	flushCond    *sync.Cond
	requestFlush chan *ringLogBuffer

	// Lifecycle & Error Handling
	shutdown chan struct{}
	done     sync.WaitGroup
	asyncErr atomic.Value
}

func NewRingBufferLogManager(logPath string) (*RingBufferConcurrentLogManager, error) {
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	startLSN := common.LSN(stat.Size())

	lm := &RingBufferConcurrentLogManager{
		logFile:      f,
		flushCond:    sync.NewCond(&sync.Mutex{}),
		requestFlush: make(chan *ringLogBuffer, numLogBuffers),
		shutdown:     make(chan struct{}),
	}
	lm.flushedLSN.Store(int64(startLSN))

	for i := 0; i < numLogBuffers; i++ {
		lm.buffers[i] = &ringLogBuffer{}
	}
	lm.buffers[0].reset(startLSN)

	lm.done.Add(1)
	go lm.flushLoop()
	return lm, nil
}

// Helper to check for background IO errors
func (lm *RingBufferConcurrentLogManager) getError() error {
	if val := lm.asyncErr.Load(); val != nil {
		return val.(error)
	}
	return nil
}

func (lm *RingBufferConcurrentLogManager) setError(err error) {
	success := lm.asyncErr.CompareAndSwap(nil, err)
	common.Assert(success, "Error should only be set once")
	// wake up anyone who might be waiting that they should handle this error
	lm.flushCond.Broadcast()
}

func (lm *RingBufferConcurrentLogManager) Append(record LogRecord) (common.LSN, error) {
	if record.Size() > MaxLogRecordSize {
		return 0, common.GoDBError{Code: common.GoDBErrorCode(0), ErrString: "log record exceeds maximum size"}
	}

	for {
		// Fast fail if IO is broken or closed
		if err := lm.getError(); err != nil {
			return -1, err
		}
		select {
		case <-lm.shutdown:
			return -1, common.GoDBError{Code: common.LogClosedError, ErrString: "Log closed"}
		default:
			index := lm.activeIndex.Load()
			buf := lm.buffers[index]

			lsn := buf.appendToBuffer(record)
			if lsn != -1 {
				return lsn, nil
			}

			lm.maybeRotate(index, buf)
		}
	}
}

func (lm *RingBufferConcurrentLogManager) maybeRotate(index uint32, buf *ringLogBuffer) {
	success, newBaseLSN := buf.seal()
	if !success {
		return // Lost the race, just retry loop
	}

	// We won the race, so we own the rotation.
	lm.requestFlush <- buf

	newBufferIndex := (index + 1) % numLogBuffers
	newBuf := lm.buffers[newBufferIndex]

	// If new buffer is sealed, wait until it's flushed'
	if newBuf.sealed() {
		lm.flushCond.L.Lock()
		for lm.flushedLSN.Load() < int64(newBuf.endLSN) {
			// Stop waiting if system breaks
			if lm.getError() != nil {
				lm.flushCond.L.Unlock()
				return
			}
			lm.flushCond.Wait()
		}
		lm.flushCond.L.Unlock()
	}

	// Reset safely unseals the buffer for the next generation
	newBuf.reset(newBaseLSN)
	lm.activeIndex.Store(newBufferIndex)
}

func (lm *RingBufferConcurrentLogManager) drainFlushRequests() {
	for {
		select {
		case buf := <-lm.requestFlush:
			flushedUntil, err := buf.append(lm.logFile)
			if err != nil {
				lm.setError(err)
				return
			}

			if err := lm.logFile.Sync(); err != nil {
				lm.setError(err)
				return
			}

			// Update global flushed LSN and wake up everyone
			if flushedUntil > common.LSN(lm.flushedLSN.Load()) {
				lm.flushedLSN.Store(int64(flushedUntil))
				lm.flushCond.L.Lock()
				lm.flushCond.Broadcast()
				lm.flushCond.L.Unlock()
			}
		default:
			return
		}
	}
}

func (lm *RingBufferConcurrentLogManager) flushLoop() {
	defer lm.done.Done()

	for {
		select {
		case <-lm.shutdown:
			lm.drainFlushRequests()
			return

		case buf := <-lm.requestFlush:
			// Process this buffer (and any others pending)
			// Re-inject buf into drain logic or handle explicitly
			// For simplicity reusing drain logic, but passing buf manually:

			flushedUntil, err := buf.append(lm.logFile)
			if err != nil {
				lm.setError(err)
				return
			}
			// Check if more are ready to batch sync
			more := true
			for more {
				select {
				case nextBuf := <-lm.requestFlush:
					f, err := nextBuf.append(lm.logFile)
					if err != nil {
						lm.setError(err)
						return
					}
					if f > flushedUntil {
						flushedUntil = f
					}
				default:
					more = false
				}
			}

			if err := lm.logFile.Sync(); err != nil {
				lm.setError(err)
				return
			}

			lm.flushedLSN.Store(int64(flushedUntil))
			lm.flushCond.L.Lock()
			lm.flushCond.Broadcast()
			lm.flushCond.L.Unlock()

		case <-time.After(flushInterval):
			// Force flush of partial buffer
			idx := lm.activeIndex.Load()
			buf := lm.buffers[idx]
			if buf.currentOffset.Load() > 0 {
				lm.maybeRotate(idx, buf)
			}
		}
	}
}

func (lm *RingBufferConcurrentLogManager) WaitUntilFlushed(lsn common.LSN) error {
	if err := lm.getError(); err != nil {
		return err
	}
	if lm.flushedLSN.Load() >= int64(lsn) {
		return nil
	}
	lm.flushCond.L.Lock()
	defer lm.flushCond.L.Unlock()
	for lm.flushedLSN.Load() < int64(lsn) {
		if err := lm.getError(); err != nil {
			return err
		}
		lm.flushCond.Wait()
	}
	return nil
}

func (lm *RingBufferConcurrentLogManager) Close() error {
	// Seal the last buffer without rotating to quiesce the system
	for {
		buf := lm.buffers[lm.activeIndex.Load()]
		if success, _ := buf.seal(); success {
			lm.requestFlush <- buf
			break
		}
		runtime.Gosched()
	}

	close(lm.shutdown)
	lm.done.Wait()

	// At this point the system is quiescent: everything has been flushed and nobody is allowed to append
	if err := lm.getError(); err != nil {
		_ = lm.logFile.Close()
		return err
	}
	lm.setError(common.GoDBError{Code: common.LogClosedError, ErrString: "Log closed"})
	return lm.logFile.Close()
}

func (lm *RingBufferConcurrentLogManager) Iterator(startLSN common.LSN) (LogIterator, error) {
	return NewLogFileIterator(lm.logFile.Name(), startLSN)
}

func (lm *RingBufferConcurrentLogManager) FlushedUntil() common.LSN {
	return common.LSN(lm.flushedLSN.Load())
}
