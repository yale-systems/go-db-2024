package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// MaterializeExecutor acts as a pipeline barrier.
// It consumes all tuples from its child during the first execution and stores them.
// Subsequent calls to Init/Next iterate over the stored tuples.
type MaterializeExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan        *planner.MaterializeNode
	child       Executor
	childSchema *storage.RawTupleDesc

	// Runtime state
	tuples       []storage.Tuple
	childInit    bool
	currentIndex int
	ctx          *ExecutorContext
	err          error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

func NewMaterializeExecutor(plan *planner.MaterializeNode, child Executor) *MaterializeExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &MaterializeExecutor{
		plan:        plan,
		child:       child,
		childSchema: storage.NewRawTupleDesc(plan.Child.OutputSchema()),
		tuples:      make([]storage.Tuple, 0),
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *MaterializeExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *MaterializeExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.currentIndex = -1
	e.err = nil

	if len(e.tuples) == 0 && !e.childInit {
		e.childInit = true
		return e.child.Init(ctx)
	}
	return nil
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *MaterializeExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.err != nil {
		return false
	}
	e.currentIndex++

	if e.currentIndex < len(e.tuples) {
		return true
	}

	if e.child.Next() {
		t := e.child.Current()
		e.tuples = append(e.tuples, t.DeepCopy(e.childSchema))
		return true
	}
	return false
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *MaterializeExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.tuples[e.currentIndex]
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *MaterializeExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Error()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *MaterializeExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
