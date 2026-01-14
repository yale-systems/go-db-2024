package logging

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"mit.edu/dsg/godb/common"
)

const (
	logBufferSize = 1 << 16 // 64KB
	flushInterval = 5 * time.Millisecond
)

type buffer struct {
	data    [logBufferSize]byte
	offset  int        // How many bytes currently used
	baseLSN common.LSN // The LSN of the first byte in this buffer
}

func (b *buffer) reset(startLSN common.LSN) {
	b.offset = 0
	b.baseLSN = startLSN
}

func (b *buffer) available() int {
	return logBufferSize - b.offset
}

type DoubleBufferLogManager struct {
	// File State
	logFile    *os.File
	nextLSN    common.LSN // The next LSN to assign (tail of the log)
	flushedLSN common.LSN // Global Flushed State (for Commit)

	// Double Buffering
	// activeBuf: The buffer strictly for memory writes (Append)
	// flushBuf:  The buffer strictly for disk writes (FlushLoop)
	activeBuf *buffer
	flushBuf  *buffer

	// Synchronization
	// flushCond: Guards access to buffers and waits for conditions (space avail, flush done)
	flushCond *sync.Cond
	sync.Mutex
	// requestFlush: Channel to signal the background thread.
	// We use a channel of size 1. If it's full, the flusher is already awake.
	requestFlush chan struct{}
	// flushPending: Explicit flag to coordinate ownership of flushBuf.
	// If true, the flusher owns flushBuf. If false, it's available for swapping.
	flushPending bool

	// Lifecycle & Error Handling
	shutdown chan struct{}
	done     sync.WaitGroup
	asyncErr atomic.Value
}

func NewDoubleBufferLogManager(logPath string) (*DoubleBufferLogManager, error) {
	f, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}

	startLSN := common.LSN(stat.Size())

	lm := &DoubleBufferLogManager{
		logFile:      f,
		nextLSN:      startLSN,
		flushedLSN:   startLSN,
		activeBuf:    &buffer{},
		flushBuf:     &buffer{},
		requestFlush: make(chan struct{}, 1),
		shutdown:     make(chan struct{}),
	}

	lm.activeBuf.reset(startLSN)
	lm.flushBuf.reset(startLSN) // Initially empty

	// init condition variable with the embedded Mutex
	lm.flushCond = sync.NewCond(&lm.Mutex)

	lm.done.Add(1)
	go lm.flushLoop()

	return lm, nil
}

// Helper to check for background IO errors
func (lm *DoubleBufferLogManager) getError() error {
	if val := lm.asyncErr.Load(); val != nil {
		return val.(error)
	}
	return nil
}

// Helper to broadcast error state
func (lm *DoubleBufferLogManager) setError(err error) {
	lm.asyncErr.Store(err)
	lm.flushCond.Broadcast() // Wake up anyone waiting
}

func (lm *DoubleBufferLogManager) signalFlush() {
	select {
	case lm.requestFlush <- struct{}{}:
	default:
		// Flusher already notified
	}
}

func (lm *DoubleBufferLogManager) Append(record LogRecord) (common.LSN, error) {
	if record.Size() > MaxLogRecordSize {
		return 0, common.GoDBError{Code: common.GoDBErrorCode(0), ErrString: "log record exceeds maximum size"}
	}
	lm.Lock()
	defer lm.Unlock()

	if err := lm.getError(); err != nil {
		return -1, err
	}

	recSize := record.Size()

	// 1. Check if we have space in activeBuf
	if lm.activeBuf.available() < recSize {
		// Buffer is full! We need to swap.
		// Wait until the flushBuf is available (flushPending == false)
		for lm.flushPending {
			if err := lm.getError(); err != nil {
				return -1, err
			}
			lm.flushCond.Wait()
		}
		lm.activeBuf, lm.flushBuf = lm.flushBuf, lm.activeBuf
		lm.activeBuf.reset(lm.nextLSN)
		lm.flushPending = true
		lm.signalFlush()
	}

	// 2. Write to active buffer
	lsn := lm.nextLSN
	record.WriteToLog(lm.activeBuf.data[lm.activeBuf.offset:])
	lm.activeBuf.offset += recSize
	lm.nextLSN += common.LSN(recSize)

	return lsn, nil
}

func (lm *DoubleBufferLogManager) flushLoop() {
	defer lm.done.Done()
	for {
		if lm.getError() != nil {
			return
		}
		select {
		case <-lm.shutdown:
			lm.Lock()
			lm.flushPendingBuffer()
			lm.Unlock()
			return
		case <-lm.requestFlush:
			lm.Lock()
			lm.flushPendingBuffer()
			lm.Unlock()
		case <-time.After(flushInterval):
			lm.Lock()
			if lm.activeBuf.offset > 0 && !lm.flushPending {
				// Force Swap
				lm.activeBuf, lm.flushBuf = lm.flushBuf, lm.activeBuf
				lm.activeBuf.reset(lm.nextLSN)
				lm.flushPending = true
				lm.flushPendingBuffer()
			}
			lm.Unlock()
		}
	}
}

// Should always be called LOCKED
func (lm *DoubleBufferLogManager) flushPendingBuffer() {
	common.Assert(lm.flushPending, "flushPendingBuffer should only be called with a pending flush")

	bytesToWrite := lm.flushBuf.data[:lm.flushBuf.offset]
	currentFlushedLSN := lm.flushBuf.baseLSN + common.LSN(lm.flushBuf.offset)
	lm.Unlock()
	// Perform IO without holding the lock
	if _, err := lm.logFile.Write(bytesToWrite); err != nil {
		lm.setError(err)
	}
	if err := lm.logFile.Sync(); err != nil {
		lm.setError(err)
	}
	lm.Lock()
	// Re-acquire lock to update state
	lm.flushedLSN = currentFlushedLSN
	lm.flushPending = false
	lm.flushCond.Broadcast() // Wake up writers waiting for space & commit waiters
}

func (lm *DoubleBufferLogManager) WaitUntilFlushed(lsn common.LSN) error {
	lm.Lock()
	defer lm.Unlock()

	if err := lm.getError(); err != nil {
		return err
	}

	// Optimization: If the requested LSN is in the *active* buffer, trigger a flush now
	if lsn > lm.flushedLSN {
		if lm.activeBuf.baseLSN <= lsn && lm.activeBuf.offset > 0 && !lm.flushPending {
			// Force Swap
			lm.activeBuf, lm.flushBuf = lm.flushBuf, lm.activeBuf
			lm.activeBuf.reset(lm.nextLSN)
			lm.flushPending = true
			lm.signalFlush()
		}
	}

	for lsn > lm.flushedLSN {
		if err := lm.getError(); err != nil {
			return err
		}
		lm.flushCond.Wait()
	}
	return nil
}

func (lm *DoubleBufferLogManager) Close() error {
	lm.Lock()
	// Force swap of any remaining data
	if lm.activeBuf.offset > 0 {
		// If flush is already pending, we have to wait for it to finish first
		// so we can swap the *next* chunk in.
		for lm.flushPending {
			lm.flushCond.Wait()
		}

		lm.activeBuf, lm.flushBuf = lm.flushBuf, lm.activeBuf
		lm.activeBuf.reset(lm.nextLSN)
		lm.flushPending = true
		lm.signalFlush()
	}
	lm.Unlock()

	// Signal background thread to stop
	close(lm.shutdown)

	// Wait for the background thread to finish writing
	lm.done.Wait()
	if err := lm.getError(); err != nil {
		_ = lm.logFile.Close()
		return err
	}
	lm.setError(common.GoDBError{Code: common.LogClosedError, ErrString: "Log closed"})
	return lm.logFile.Close()
}

func (lm *DoubleBufferLogManager) Iterator(startLSN common.LSN) (LogIterator, error) {
	return NewLogFileIterator(lm.logFile.Name(), startLSN)
}

func (lm *DoubleBufferLogManager) FlushedUntil() common.LSN {
	lm.Lock()
	defer lm.Unlock()
	return lm.flushedLSN
}
