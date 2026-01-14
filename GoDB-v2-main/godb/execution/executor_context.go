package execution

import (
	"mit.edu/dsg/godb/transaction"
)

// ExecutorContext holds all the state and resources required for query execution.
// It is passed to every Executor during construction.
type ExecutorContext struct {
	txn *transaction.TransactionContext
	// A production system would have many more fields here
}

func NewExecutorContext(txn *transaction.TransactionContext, operatorMemLimit int) *ExecutorContext {
	return &ExecutorContext{
		txn: txn,
	}
}

func (ctx *ExecutorContext) GetTransaction() *transaction.TransactionContext {
	return ctx.txn
}
