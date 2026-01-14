package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// HashJoinExecutor implements the hash join algorithm.
// It builds a hash table from the left child and probes it with the right child.
// It only supports Equi-Joins.
type HashJoinExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan                                *planner.HashJoinNode
	left, right                         Executor
	keySchema, leftSchema, joinedSchema *storage.RawTupleDesc

	// Runtime State
	keyBuffer         []common.Value
	joinedTupleBuffer storage.RawTuple
	leftHashTable     *ExecutionHashTable[[]storage.Tuple]
	currentMatches    []storage.Tuple // The matching tuples from the left side for the current right tuple
	matchIndex        int             // The index of the next match to emit
	ctx               *ExecutorContext
	err               error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

// NewHashJoinExecutor creates a new HashJoinExecutor.
func NewHashJoinExecutor(plan *planner.HashJoinNode, left Executor, right Executor) *HashJoinExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	keyTypes := make([]common.Type, len(plan.LeftKeys))
	for i, expr := range plan.LeftKeys {
		keyTypes[i] = expr.OutputType()
	}
	return &HashJoinExecutor{
		plan:         plan,
		left:         left,
		right:        right,
		keySchema:    storage.NewRawTupleDesc(keyTypes),
		leftSchema:   storage.NewRawTupleDesc(left.PlanNode().OutputSchema()),
		joinedSchema: storage.NewRawTupleDesc(plan.OutputSchema()),
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *HashJoinExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *HashJoinExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.keyBuffer = make([]common.Value, len(e.plan.LeftKeys))
	e.joinedTupleBuffer = make([]byte, e.joinedSchema.BytesPerTuple())
	e.leftHashTable = nil
	e.currentMatches = nil
	e.matchIndex = 0
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

// buildPhase consumes the entire left child and builds the hash table.
func (e *HashJoinExecutor) buildPhase() error {
	e.leftHashTable = NewExecutionHashTable[[]storage.Tuple](e.keySchema)
Outer:
	for e.left.Next() {
		tuple := e.left.Current()

		// Extract the Join Key from Left
		for i, expr := range e.plan.LeftKeys {
			val := expr.Eval(tuple)
			// Skip any NULL keys
			if val.IsNull() {
				continue Outer
			}
			e.keyBuffer[i] = val
		}

		// Insert into the table (handling duplicates by appending to the slice)
		keyTuple := storage.FromValues(e.keyBuffer...)
		existing, found := e.leftHashTable.Get(keyTuple)
		if !found {
			existing = make([]storage.Tuple, 0, 1)
		}
		e.leftHashTable.Insert(keyTuple, append(existing, tuple.DeepCopy(e.leftSchema)))
	}
	return e.left.Error()
}

// </silentstrip>

func (e *HashJoinExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.err != nil {
		return false
	}
	if e.leftHashTable == nil {
		if err := e.buildPhase(); err != nil {
			e.err = err
			return false
		}
	}

Outer:
	for {
		if e.matchIndex == len(e.currentMatches) {
			// no more matches left in the last scan, need to fetch the next right tuple
			if !e.right.Next() {
				if e.right.Error() != nil {
					e.err = e.right.Error()
				}
				return false
			}
			rightTuple := e.right.Current()
			for i, expr := range e.plan.RightKeys {
				val := expr.Eval(rightTuple)
				if val.IsNull() {
					continue Outer
				}
				e.keyBuffer[i] = val
			}
			matches, found := e.leftHashTable.Get(storage.FromValues(e.keyBuffer...))
			if !found {
				continue
			}
			e.currentMatches = matches
			e.matchIndex = 0
		}
		leftTuple := e.currentMatches[e.matchIndex]
		e.matchIndex++
		storage.MergeTuples(e.joinedTupleBuffer, e.joinedSchema, leftTuple, e.right.Current())
		return true
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *HashJoinExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromRawTuple(e.joinedTupleBuffer, e.joinedSchema, common.RecordID{})
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *HashJoinExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *HashJoinExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	err1 := e.right.Close()
	err2 := e.left.Close()
	if err1 != nil {
		return err1
	}
	return err2
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
