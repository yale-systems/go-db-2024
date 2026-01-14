package execution

import (
	"errors"

	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

type IndexLookupExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan      *planner.IndexLookupNode
	index     indexing.Index
	tableHeap *TableHeap

	// Runtime state
	ridList   []common.RecordID
	ridIdx    int
	rowBuffer []byte
	ctx       *ExecutorContext
	err       error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

func NewIndexLookupExecutor(plan *planner.IndexLookupNode, index indexing.Index, tableHeap *TableHeap) *IndexLookupExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &IndexLookupExecutor{
		plan:      plan,
		index:     index,
		tableHeap: tableHeap,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexLookupExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexLookupExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.ridList = nil
	e.ridIdx = 0
	e.rowBuffer = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	e.ctx = ctx
	e.err = nil
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexLookupExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	for {
		if len(e.ridList) == 0 {
			var err error
			e.ridList, err = e.index.ScanKey(e.plan.EqualityKey, e.ridList, e.ctx.GetTransaction())
			if err != nil {
				e.err = err
				return false
			}
		}

		if e.ridIdx >= len(e.ridList) {
			return false
		}

		rid := e.ridList[e.ridIdx]
		e.ridIdx++

		err := e.tableHeap.ReadTuple(e.ctx.GetTransaction(), rid, e.rowBuffer, e.plan.ForUpdate)
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

func (e *IndexLookupExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromRawTuple(e.rowBuffer, e.tableHeap.StorageSchema(), e.ridList[e.ridIdx-1])
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexLookupExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.ridList = nil
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexLookupExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
