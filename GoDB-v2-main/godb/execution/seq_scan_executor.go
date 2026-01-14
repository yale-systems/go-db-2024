package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SeqScanExecutor implements a sequential scan over a table.
type SeqScanExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan      *planner.SeqScanNode
	tableHeap *TableHeap

	// Runtime state
	iterator  TableHeapIterator
	rowBuffer []byte
	ctx       *ExecutorContext
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

// NewSeqScanExecutor creates a new SeqScanExecutor.
func NewSeqScanExecutor(plan *planner.SeqScanNode, tableHeap *TableHeap) *SeqScanExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &SeqScanExecutor{
		plan:      plan,
		tableHeap: tableHeap,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SeqScanExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SeqScanExecutor) Init(context *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.ctx = context
	e.rowBuffer = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	var err error
	e.iterator, err = e.tableHeap.Iterator(context.GetTransaction(), e.plan.Mode, e.rowBuffer)
	return err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SeqScanExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	common.Assert(!e.iterator.IsNil(), "SeqScanExecutor.Init() must be called before calling Next()")
	// The iterator populates e.current in-place
	return e.iterator.Next()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SeqScanExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromRawTuple(e.iterator.CurrentTuple(), e.tableHeap.StorageSchema(), e.iterator.currRID)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SeqScanExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.iterator.Error()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SeqScanExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	if !e.iterator.IsNil() {
		return e.iterator.Close()
	}
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
