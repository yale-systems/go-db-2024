package execution

import (
	"errors"

	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

type IndexScanExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan      *planner.IndexScanNode
	index     indexing.Index
	tableHeap *TableHeap

	// Runtime state
	iter      indexing.ScanIterator
	rowBuffer []byte
	ctx       *ExecutorContext
	err       error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

func NewIndexScanExecutor(plan *planner.IndexScanNode, index indexing.Index, tableHeap *TableHeap) *IndexScanExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &IndexScanExecutor{
		plan:      plan,
		index:     index,
		tableHeap: tableHeap,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexScanExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexScanExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.rowBuffer = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	e.ctx = ctx
	e.iter, e.err = e.index.Scan(e.plan.StartKey, e.plan.Direction, e.ctx.GetTransaction())
	if e.err != nil {
		return e.err
	}
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexScanExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	common.Assert(e.iter != nil, "IndexScanExecutor.Init() must be called before calling Next()")
	for {
		if !e.iter.Next() {
			e.err = e.iter.Error()
			return false
		}
		rid := e.iter.Value()
		err := e.tableHeap.ReadTuple(e.ctx.GetTransaction(), rid, e.rowBuffer, e.plan.ForUpdate)
		// Concurrent deletions happen, simpl retry
		if errors.Is(err, ErrTupleDeleted) {
			continue
		} else if err != nil {
			e.err = err
			return false
		}
		return true
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromRawTuple(e.rowBuffer, e.tableHeap.StorageSchema(), e.iter.Value())
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexScanExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.iter != nil {
		return e.iter.Close()
	}
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexScanExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
