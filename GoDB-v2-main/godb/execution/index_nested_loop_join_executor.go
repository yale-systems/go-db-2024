package execution

import (
	"errors"

	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// IndexNestedLoopJoinExecutor implements an index nested loop join.
// It iterates over the left child, and for each tuple, probes the index of the right table.
// The expressions given for the left tuple should have the same schema as the right index's key
type IndexNestedLoopJoinExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan           *planner.IndexNestedLoopJoinNode
	left           Executor
	rightIndex     indexing.Index
	rightTableHeap *TableHeap
	joinedSchema   *storage.RawTupleDesc // Schema for the rightIndex key

	// Runtime State
	leftExprBuffer                                     []common.Value
	ridList                                            []common.RecordID // List of matches from the rightIndex for the current key
	ridIdx                                             int               // Current rightIndex in ridList
	leftKeyBuffer, rightTupleBuffer, joinedTupleBuffer []byte
	ctx                                                *ExecutorContext
	err                                                error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

// NewIndexJoinExecutor creates a new IndexNestedLoopJoinExecutor.
// It assumes the left table is accessed via the provided rightIndex and rightTableHeap.
func NewIndexJoinExecutor(plan *planner.IndexNestedLoopJoinNode, left Executor, rightIndex indexing.Index, rightTableHeap *TableHeap) *IndexNestedLoopJoinExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &IndexNestedLoopJoinExecutor{
		plan:           plan,
		left:           left,
		rightIndex:     rightIndex,
		rightTableHeap: rightTableHeap,
		joinedSchema:   storage.NewRawTupleDesc(plan.OutputSchema()),
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.leftExprBuffer = make([]common.Value, len(e.plan.LeftKeys))
	e.ridList = nil
	e.ridIdx = 0
	e.leftKeyBuffer = make([]byte, e.rightIndex.Metadata().KeySize())
	e.rightTupleBuffer = make([]byte, e.rightTableHeap.StorageSchema().BytesPerTuple())
	e.joinedTupleBuffer = make([]byte, e.joinedSchema.BytesPerTuple())
	e.ctx = ctx
	e.err = nil
	return e.left.Init(ctx)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexNestedLoopJoinExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.err != nil {
		return false
	}
Outer:
	for {
		// No more matches from the last scan. Need to probe right with a new left key
		if e.ridIdx >= len(e.ridList) {
			if !e.left.Next() {
				// No more left tuples, we are done with join
				if e.left.Error() != nil {
					e.err = e.left.Error()
				}
				return false
			}

			for i := 0; i < len(e.plan.LeftKeys); i++ {
				val := e.plan.LeftKeys[i].Eval(e.left.Current())
				if val.IsNull() {
					continue Outer
				}
				e.leftExprBuffer[i] = val
			}
			values := storage.FromValues(e.leftExprBuffer...)
			values.WriteToBuffer(e.leftKeyBuffer, e.rightIndex.Metadata().KeySchema)
			e.ridList, e.err = e.rightIndex.ScanKey(e.rightIndex.Metadata().AsKey(e.leftKeyBuffer), e.ridList[:0], e.ctx.GetTransaction())
			if e.err != nil {
				return false
			}
			e.ridIdx = 0
			continue
		}

		// Emit pending matches from last probe
		rid := e.ridList[e.ridIdx]
		e.ridIdx++

		err := e.rightTableHeap.ReadTuple(e.ctx.GetTransaction(), rid, e.rightTupleBuffer, e.plan.ForUpdate)
		if errors.Is(err, ErrTupleDeleted) {
			continue
		} else if err != nil {
			e.err = err
			return false
		}
		storage.MergeTuples(e.joinedTupleBuffer, e.joinedSchema, e.left.Current(), storage.FromRawTuple(e.rightTupleBuffer, e.rightTableHeap.StorageSchema(), common.RecordID{}))
		return true
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexNestedLoopJoinExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromRawTuple(e.joinedTupleBuffer, e.joinedSchema, common.RecordID{})
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexNestedLoopJoinExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *IndexNestedLoopJoinExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.ridList = nil
	return e.left.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
