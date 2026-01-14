package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

type SortMergeJoinExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan                                  *planner.SortMergeJoinNode
	left, right                           Executor
	leftSchema, rightSchema, joinedSchema *storage.RawTupleDesc

	// Note: This version buffers more than necessary for simplicity of logic
	leftGroup, rightGroup                            []storage.Tuple
	currentGroupKey, currentLeftKey, currentRightKey []common.Value
	leftIndex, rightIndex                            int
	initialized, leftExhausted, rightExhausted       bool
	joinedTupleBuffer                                storage.RawTuple
	ctx                                              *ExecutorContext
	err                                              error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

func NewSortMergeJoinExecutor(plan *planner.SortMergeJoinNode, left, right Executor) *SortMergeJoinExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &SortMergeJoinExecutor{
		plan:         plan,
		left:         left,
		right:        right,
		leftSchema:   storage.NewRawTupleDesc(plan.Left.OutputSchema()),
		rightSchema:  storage.NewRawTupleDesc(plan.Right.OutputSchema()),
		joinedSchema: storage.NewRawTupleDesc(append(plan.Left.OutputSchema(), plan.Right.OutputSchema()...)),
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SortMergeJoinExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SortMergeJoinExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.leftGroup = make([]storage.Tuple, 0, 1)
	e.rightGroup = make([]storage.Tuple, 0, 1)
	e.leftIndex = 0
	e.rightIndex = 0
	e.initialized = false
	e.leftExhausted = false
	e.rightExhausted = false
	e.joinedTupleBuffer = make([]byte, e.joinedSchema.BytesPerTuple())
	e.currentGroupKey = make([]common.Value, len(e.plan.LeftKeys))
	e.currentLeftKey = make([]common.Value, len(e.plan.LeftKeys))
	e.currentRightKey = make([]common.Value, len(e.plan.RightKeys))
	e.ctx = ctx
	e.err = nil

	if err := e.left.Init(ctx); err != nil {
		return err
	}
	return e.right.Init(ctx)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>

func compareKeys(k1, k2 []common.Value) int {
	for i := range k1 {
		if cmp := k1[i].Compare(k2[i]); cmp != 0 {
			return cmp
		}
	}
	return 0
}

func (e *SortMergeJoinExecutor) advanceLeft() bool {
	if e.leftExhausted {
		return false
	}
Outer:
	for {
		if !e.left.Next() {
			e.err = e.left.Error()
			e.leftExhausted = true
			return false
		}
		for i := 0; i < len(e.plan.LeftKeys); i++ {
			val := e.plan.LeftKeys[i].Eval(e.left.Current())
			if val.IsNull() {
				continue Outer
			}
			e.currentLeftKey[i] = val
		}
		return true
	}
}

func (e *SortMergeJoinExecutor) advanceRight() bool {
	if e.rightExhausted {
		return false
	}
Outer:
	for {
		if !e.right.Next() {
			e.err = e.right.Error()
			e.rightExhausted = true
			return false
		}
		for i := 0; i < len(e.plan.RightKeys); i++ {
			val := e.plan.RightKeys[i].Eval(e.right.Current())
			if val.IsNull() {
				continue Outer
			}
			e.currentRightKey[i] = val
		}
		return true
	}
}

func (e *SortMergeJoinExecutor) loadLeftBatch() {
	current := e.left.Current()
	e.leftGroup = append(e.leftGroup[:0], current.DeepCopy(e.leftSchema))
	e.leftIndex = 0
	copy(e.currentGroupKey, e.currentLeftKey)
	for e.advanceLeft() {
		if cmp := compareKeys(e.currentGroupKey, e.currentLeftKey); cmp != 0 {
			break
		}
		current = e.left.Current()
		e.leftGroup = append(e.leftGroup, current.DeepCopy(e.leftSchema))
	}
}

func (e *SortMergeJoinExecutor) loadRightBatch() {
	e.rightGroup = e.rightGroup[:0]
	e.rightIndex = 0
	for {
		cmp := compareKeys(e.currentGroupKey, e.currentRightKey)
		if cmp == 0 {
			current := e.right.Current()
			e.rightGroup = append(e.rightGroup, current.DeepCopy(e.rightSchema))
		} else if cmp < 0 {
			break
		}
		if !e.advanceRight() {
			break
		}
		continue
	}
}

// </silentstrip>

func (e *SortMergeJoinExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.err != nil {
		return false
	}
	if !e.initialized {
		if !e.advanceLeft() || !e.advanceRight() {
			return false
		}
		e.initialized = true
	}

	for {
		if e.leftIndex >= len(e.leftGroup) || e.rightIndex >= len(e.rightGroup) {
			// We don't have a batch. find the next batch of equal keys

			if e.leftExhausted || e.rightExhausted || e.err != nil {
				// No more tuples to join
				return false
			}
			e.loadLeftBatch()
			e.loadRightBatch()
			continue
		}
		storage.MergeTuples(e.joinedTupleBuffer, e.joinedSchema, e.leftGroup[e.leftIndex], e.rightGroup[e.rightIndex])
		e.rightIndex++
		if e.rightIndex >= len(e.rightGroup) {
			e.leftIndex++
			e.rightIndex = 0
			continue
		}
		return true

	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SortMergeJoinExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromRawTuple(e.joinedTupleBuffer, e.joinedSchema, common.RecordID{})
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SortMergeJoinExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SortMergeJoinExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	_ = e.left.Close()
	_ = e.right.Close()
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
