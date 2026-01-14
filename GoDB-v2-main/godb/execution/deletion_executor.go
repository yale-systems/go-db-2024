package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

type DeletionExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan      *planner.DeletionNode
	child     Executor
	tableHeap *TableHeap
	indexes   []indexing.Index

	// Runtime state
	keyBuffer storage.RawTuple
	executed  bool
	cnt       int
	ctx       *ExecutorContext
	err       error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

func NewDeleteExecutor(plan *planner.DeletionNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *DeletionExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &DeletionExecutor{
		plan:      plan,
		child:     child,
		tableHeap: tableHeap,
		indexes:   indexes,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *DeletionExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *DeletionExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.keyBuffer = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	e.ctx = ctx
	e.executed = false
	e.cnt = 0
	e.err = nil
	return e.child.Init(ctx)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *DeletionExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if !e.executed {
		for e.child.Next() {
			tuple := e.child.Current()
			rid := tuple.RID()
			common.Assert(!rid.IsNil(), "RID to delete should not be nil")

			if err := e.tableHeap.DeleteTuple(e.ctx.GetTransaction(), rid); err != nil {
				e.err = err
				return false
			}

			for _, index := range e.indexes {
				if err := e.deleteFromIndex(index, rid); err != nil {
					e.err = err
					return false
				}
			}
			e.cnt++
		}
		if err := e.child.Error(); err != nil {
			e.err = err
			return false
		}
		e.executed = true
	}
	return !e.executed
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>
func (e *DeletionExecutor) deleteFromIndex(index indexing.Index, rid common.RecordID) error {
	for i, col := range index.Metadata().ProjectionList {
		// The child should hold the full buffer that we are trying to delete
		current := e.child.Current()
		value := current.GetValue(col)
		index.Metadata().KeySchema.SetValue(e.keyBuffer, i, value)
	}
	return index.DeleteEntry(index.Metadata().AsKey(e.keyBuffer), rid, e.ctx.GetTransaction())
}

// </silentstrip>

func (e *DeletionExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromValues(common.NewIntValue(int64(e.cnt)))
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *DeletionExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *DeletionExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
