package recovery

import (
	"sync"
	"time"

	"mit.edu/dsg/godb/logging"
	"mit.edu/dsg/godb/storage"
)

// BackgroundFlusher is a standalone component responsible for periodically
// flushing dirty pages from the BufferPool to disk.
// This helps keep the checkpoint/recovery time bounded by ensuring the
// difference between the FlushedLSN and the current LSN does not grow indefinitely.
type BackgroundFlusher struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	bufferPool *storage.BufferPool
	logManager logging.LogManager
	interval   time.Duration
	shutdown   chan struct{}
	done       sync.WaitGroup
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

// NewBackgroundFlusher creates a new flusher instance.
func NewBackgroundFlusher(bp *storage.BufferPool, lm logging.LogManager, interval time.Duration) *BackgroundFlusher {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &BackgroundFlusher{
		bufferPool: bp,
		logManager: lm,
		interval:   interval,
		shutdown:   make(chan struct{}),
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Start initiates background flushing.
func (bf *BackgroundFlusher) Start() {
	// <silentstrip lab1|lab2|lab3|lab4>
	bf.done.Add(1)
	go bf.flushLoop()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Stop signals the flusher to shut down and blocks until the final flush is complete.
func (bf *BackgroundFlusher) Stop() {
	// <silentstrip lab1|lab2|lab3|lab4>
	close(bf.shutdown)
	bf.done.Wait()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>
func (bf *BackgroundFlusher) flushLoop() {
	defer bf.done.Done()
	ticker := time.NewTicker(bf.interval)
	defer ticker.Stop()

	for {
		maxLSN := bf.logManager.FlushedUntil()

		select {
		case <-ticker.C:
			// Periodically flush all dirty pages to prevent log starvation.
			// We ignore errors as this is a best-effort maintenance task.
			_ = bf.bufferPool.FlushAllPages(maxLSN)

		case <-bf.shutdown:
			// On shutdown, perform one final full flush to ensure a clean state.
			_ = bf.bufferPool.FlushAllPages(maxLSN)
			return
		}
	}
}

// </silentstrip>
