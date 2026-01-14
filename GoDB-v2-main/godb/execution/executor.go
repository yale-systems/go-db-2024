package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// Executor is the interface that all physical execution nodes must implement.
type Executor interface {
	PlanNode() planner.PlanNode

	// Init initializes the executor with a specific execution context.
	// This binds the executor to a transaction.
	Init(ctx *ExecutorContext) error

	// Next retrieves the next tuple from the executor.
	Next() bool

	// Current returns the tuple most recently read by Next().
	Current() storage.Tuple

	// Error returns the last error encountered by the executor, if any.
	Error() error

	// Close cleans up any resources held by the executor.
	Close() error
}
