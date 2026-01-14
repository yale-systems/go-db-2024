package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// AggregateExecutor implements hash-based aggregation.
type AggregateExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan  *planner.AggregateNode
	child Executor

	// Runtime state
	tuples       []storage.Tuple
	currentIndex int
	ctx          *ExecutorContext
	err          error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

func NewAggregateExecutor(plan *planner.AggregateNode, child Executor) *AggregateExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &AggregateExecutor{
		child:        child,
		plan:         plan,
		currentIndex: -1,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *AggregateExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *AggregateExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.tuples = nil
	e.currentIndex = -1
	e.ctx = ctx
	e.err = nil
	return e.child.Init(ctx)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>

func (e *AggregateExecutor) updateAggregateState(state []common.Value, tuple storage.Tuple) {
	for i, agg := range e.plan.AggClauses {
		val := agg.Expr.Eval(tuple)

		// Standard SQL aggregate rules: ignore NULLs (except Count(*), which is usually handled by expr)
		if val.IsNull() {
			continue
		}

		switch agg.Type {
		case planner.AggCount:
			if state[i].IsNil() {
				state[i] = common.NewIntValue(1)
			} else {
				curr := state[i].IntValue()
				state[i] = common.NewIntValue(curr + 1)
			}
		case planner.AggSum:
			if state[i].IsNil() {
				state[i] = val.Copy()
				continue
			} else {
				curr := state[i].IntValue()
				state[i] = common.NewIntValue(curr + val.IntValue())
			}
		case planner.AggMin:
			if state[i].IsNil() || val.Compare(state[i]) < 0 {
				state[i] = val.Copy()
			}
		case planner.AggMax:
			if state[i].IsNil() || val.Compare(state[i]) > 0 {
				state[i] = val.Copy()
			}
		}
	}
}

func (e *AggregateExecutor) buildHashTable() bool {
	// Define the schema for the GroupBy key based on the grouping expressions
	keyFields := make([]common.Type, len(e.plan.GroupByClause))
	for i, expr := range e.plan.GroupByClause {
		keyFields[i] = expr.OutputType()
	}

	keySchema := storage.NewRawTupleDesc(keyFields)
	hashTable := NewExecutionHashTable[[]common.Value](keySchema)

	keyTupleBuffer := make([]common.Value, len(e.plan.GroupByClause))
	for e.child.Next() {
		tuple := e.child.Current()
		for i, expr := range e.plan.GroupByClause {
			keyTupleBuffer[i] = expr.Eval(tuple)
		}
		// If group by is empty, this will produce an empty tuple, which is the intended behavior
		// as we will perform a global aggregation and return exactly one row.
		keyTuple := storage.FromValues(keyTupleBuffer...)
		state, found := hashTable.Get(keyTuple)
		if !found {
			state = make([]common.Value, len(e.plan.AggClauses))
			hashTable.Insert(keyTuple, state)
		}

		e.updateAggregateState(state, tuple)
	}

	if err := e.child.Error(); err != nil {
		e.err = err
		return false
	}

	hashTable.Iterate(func(t storage.Tuple, values []common.Value) {
		for i, v := range values {
			if v.IsNil() {
				// Convert sentinel IsNil to actual SQL NULL of the correct type
				switch e.plan.AggClauses[i].Expr.OutputType() {
				case common.IntType:
					values[i] = common.NewNullInt()
				case common.StringType:
					values[i] = common.NewNullString()
				}
			}
		}
		e.tuples = append(e.tuples, t.Extend(values))
	})
	return true
}

// </silentstrip>

func (e *AggregateExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.tuples == nil {
		if !e.buildHashTable() {
			return false
		}
	}
	e.currentIndex++
	return e.currentIndex < len(e.tuples)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *AggregateExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.tuples[e.currentIndex]
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *AggregateExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *AggregateExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
