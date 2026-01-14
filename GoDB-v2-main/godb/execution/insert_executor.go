package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

type InsertExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan      *planner.InsertNode
	child     Executor
	tableHeap *TableHeap
	indexes   []indexing.Index

	// Runtime state
	rowBuffer storage.RawTuple
	executed  bool
	cnt       int
	ctx       *ExecutorContext
	err       error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

func NewInsertExecutor(plan *planner.InsertNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *InsertExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &InsertExecutor{
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

func (e *InsertExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *InsertExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.rowBuffer = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
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

func (e *InsertExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if !e.executed {
		for e.child.Next() {
			tuple := e.child.Current()
			tuple.WriteToBuffer(e.rowBuffer, e.tableHeap.StorageSchema())

			rid, err := e.tableHeap.InsertTuple(e.ctx.GetTransaction(), e.rowBuffer)
			if err != nil {
				e.err = err
				return false
			}

			for _, index := range e.indexes {
				if err := e.insertIntoIndex(index, tuple, rid); err != nil {
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

func (e *InsertExecutor) insertIntoIndex(index indexing.Index, t storage.Tuple, rid common.RecordID) error {
	for i, col := range index.Metadata().ProjectionList {
		index.Metadata().KeySchema.SetValue(e.rowBuffer, i, t.GetValue(col))
	}
	return index.InsertEntry(index.Metadata().AsKey(e.rowBuffer), rid, e.ctx.GetTransaction())
}

// </silentstrip>

func (e *InsertExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromValues(common.NewIntValue(int64(e.cnt)))
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *InsertExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *InsertExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
