package execution

import (
	"errors"

	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

type UpdateExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan                     *planner.UpdateNode
	tableHeap                *TableHeap
	child                    Executor
	indexes, affectedIndexes []indexing.Index

	executed                             bool
	cnt                                  int
	oldTuple                             storage.RawTuple
	newTuple, keyBufferOld, keyBufferNew []byte

	ctx *ExecutorContext
	err error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>
func indexKeyChanged(index indexing.Index, updateList []int) bool {
	for _, keyCol := range index.Metadata().ProjectionList {
		for _, deltaCol := range updateList {
			if deltaCol == keyCol {
				return true
			}
		}
	}
	return false
}

// </silentstrip>

func NewUpdateExecutor(plan *planner.UpdateNode, tableHeap *TableHeap, child Executor, indexes []indexing.Index) *UpdateExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	affectedIndexes := make([]indexing.Index, 0, len(indexes))
	for _, index := range indexes {
		if indexKeyChanged(index, plan.ProjectionList) {
			affectedIndexes = append(affectedIndexes, index)
		}
	}
	return &UpdateExecutor{
		plan:      plan,
		tableHeap: tableHeap,
		child:     child,
		indexes:   affectedIndexes,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *UpdateExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>

}

func (e *UpdateExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	tupleSize := e.tableHeap.StorageSchema().BytesPerTuple()
	// Initialize buffers
	e.oldTuple = make([]byte, tupleSize)
	e.newTuple = make([]byte, tupleSize)
	e.keyBufferOld = make([]byte, tupleSize)
	e.keyBufferNew = make([]byte, tupleSize)

	e.executed = false
	e.cnt = 0
	e.ctx = ctx
	e.err = nil
	return e.child.Init(ctx)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *UpdateExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if !e.executed {
		for e.child.Next() {
			deltaTuple := e.child.Current()
			rid := deltaTuple.RID()
			common.Assert(!rid.IsNil(), "RID to update should not be nil")

			if err := e.tableHeap.ReadTuple(e.ctx.GetTransaction(), rid, e.oldTuple, true); err != nil {
				if errors.Is(err, ErrTupleDeleted) {
					// Concurrent deletion is normal, simply move on
					continue
				}
				e.err = err
				return false
			}

			copy(e.newTuple, e.oldTuple)
			for i := 0; i < len(e.plan.ProjectionList); i++ {
				e.tableHeap.StorageSchema().SetValue(e.newTuple, e.plan.ProjectionList[i], deltaTuple.GetValue(i))
			}

			if err := e.tableHeap.UpdateTuple(e.ctx.GetTransaction(), rid, e.newTuple); err != nil {
				e.err = err
				return false
			}

			for _, index := range e.indexes {
				if err := e.updateIndex(index, e.oldTuple, e.newTuple, rid); err != nil {
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
func (e *UpdateExecutor) updateIndex(index indexing.Index, oldTuple, newTuple storage.RawTuple, rid common.RecordID) error {
	// Extract Old Key
	for i, col := range index.Metadata().ProjectionList {
		oldValue := e.tableHeap.StorageSchema().GetValue(oldTuple, col)
		index.Metadata().KeySchema.SetValue(e.keyBufferOld, i, oldValue)
	}
	oldKey := index.Metadata().AsKey(e.keyBufferOld)

	// Extract New Key
	for i, col := range index.Metadata().ProjectionList {
		newValue := e.tableHeap.StorageSchema().GetValue(newTuple, col)
		index.Metadata().KeySchema.SetValue(e.keyBufferNew, i, newValue)
	}
	newKey := index.Metadata().AsKey(e.keyBufferNew)

	if !oldKey.Equals(newKey) {
		if err := index.DeleteEntry(oldKey, rid, e.ctx.GetTransaction()); err != nil {
			return err
		}
		if err := index.InsertEntry(newKey, rid, e.ctx.GetTransaction()); err != nil {
			return err
		}
	}
	return nil
}

// </silentstrip>

func (e *UpdateExecutor) OutputSchema() []common.Type {
	// <silentstrip lab1|lab2|lab3|lab4>
	return []common.Type{common.IntType}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *UpdateExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromValues(common.NewIntValue(int64(e.cnt)))
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *UpdateExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *UpdateExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.err != nil {
		return e.err
	}
	return e.child.Error()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
