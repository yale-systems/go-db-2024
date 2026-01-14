package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// The size of block, in bytes, that the join operator is allowed to buffer
const blockSize = 1 << 15

// BlockNestedLoopJoinExecutor implements the block nested loop join algorithm.
// It loads a block of tuples from the left child into memory and then scans the right child
// to find matches. This reduces the number of times the right child is sequentially scanned.
type BlockNestedLoopJoinExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan                     *planner.NestedLoopJoinNode
	left, right              Executor
	leftSchema, joinedSchema *storage.RawTupleDesc

	// Runtime State
	leftBuffer            []storage.Tuple
	leftTupleBuffers      []storage.RawTuple
	leftIndex, leftFilled int
	joinedTupleBuffer     storage.RawTuple
	ctx                   *ExecutorContext
	err                   error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

// NewBlockNestedLoopJoinExecutor creates a new BlockNestedLoopJoinExecutor.
func NewBlockNestedLoopJoinExecutor(plan *planner.NestedLoopJoinNode, left Executor, right Executor) *BlockNestedLoopJoinExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &BlockNestedLoopJoinExecutor{
		plan:         plan,
		left:         left,
		right:        right,
		leftSchema:   storage.NewRawTupleDesc(plan.Left.OutputSchema()),
		joinedSchema: storage.NewRawTupleDesc(append(plan.Left.OutputSchema(), plan.Right.OutputSchema()...)),
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *BlockNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *BlockNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.leftBuffer = make([]storage.Tuple, blockSize/e.leftSchema.BytesPerTuple())
	e.leftTupleBuffers = make([]storage.RawTuple, blockSize/e.leftSchema.BytesPerTuple())
	for i := 0; i < blockSize/e.leftSchema.BytesPerTuple(); i++ {
		e.leftTupleBuffers[i] = make([]byte, e.leftSchema.BytesPerTuple())
	}
	e.leftIndex = 0
	e.leftFilled = 0
	e.joinedTupleBuffer = make([]byte, e.joinedSchema.BytesPerTuple())
	e.ctx = ctx
	e.err = nil
	// Note: We do not Init(right) here immediately because we will Init it
	// every time we load a new block from the left.
	return e.left.Init(ctx)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>
func (e *BlockNestedLoopJoinExecutor) newBlockIteration() bool {
	// re-initialize the right scan
	if err := e.right.Init(e.ctx); err != nil {
		e.err = err
		return false
	}
	// Load the first tuple on the right
	if !e.right.Next() {
		if e.right.Error() != nil {
			e.err = e.right.Error()
		}
		return false
	}
	// fetch the next block of tuples on the left
	for i := 0; i < len(e.leftBuffer); i++ {
		if !e.left.Next() {
			if e.left.Error() != nil {
				e.err = e.left.Error()
				return false
			}
			// We are out of tuples on the left if there is no error, but we filled 0 tuples
			e.leftFilled = i
			return e.leftFilled != 0
		}
		t := e.left.Current()
		e.leftBuffer[i] = t.WriteToBuffer(e.leftTupleBuffers[i], e.leftSchema)
	}
	e.leftFilled = len(e.leftBuffer)
	return true
}

// </silentstrip>

func (e *BlockNestedLoopJoinExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.err != nil {
		return false
	}

	for {
		// If we don't have a left tuple batch, try to get one
		if e.leftFilled == 0 {
			if !e.newBlockIteration() {
				return false
			}
		}

		for e.leftIndex < e.leftFilled {
			leftTuple := e.leftBuffer[e.leftIndex]
			e.leftIndex++
			storage.MergeTuples(e.joinedTupleBuffer, e.joinedSchema, leftTuple, e.right.Current())
			jointTuple := storage.FromRawTuple(e.joinedTupleBuffer, e.joinedSchema, common.RecordID{})

			if planner.ExprIsTrue(e.plan.Predicate.Eval(jointTuple)) {
				return true
			}
		}
		// Keep the left block and get the next right child
		e.leftIndex = 0
		if !e.right.Next() {
			if e.right.Error() != nil {
				e.err = e.right.Error()
				return false
			}
			// Signal that we are completely done with the left block. Time to fetch a new one
			e.leftFilled = 0
		}
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *BlockNestedLoopJoinExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromRawTuple(e.joinedTupleBuffer, e.joinedSchema, common.RecordID{})
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *BlockNestedLoopJoinExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *BlockNestedLoopJoinExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	if err := e.left.Close(); err != nil {
		return err
	}
	return e.right.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
