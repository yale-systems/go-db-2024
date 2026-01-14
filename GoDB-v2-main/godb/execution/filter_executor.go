package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// FilterExecutor filters tuples from its child executor based on a predicate.
type FilterExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan  *planner.FilterNode
	child Executor
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

// NewFilter creates a new FilterExecutor executor.
func NewFilter(plan *planner.FilterNode, child Executor) *FilterExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &FilterExecutor{
		plan:  plan,
		child: child,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *FilterExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Init initializes the child.
func (e *FilterExecutor) Init(context *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Init(context)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *FilterExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	for e.child.Next() {
		res := e.plan.Predicate.Eval(e.child.Current())

		if planner.ExprIsTrue(res) {
			return true
		}
	}
	return false
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *FilterExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Current()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *FilterExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Error()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *FilterExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
