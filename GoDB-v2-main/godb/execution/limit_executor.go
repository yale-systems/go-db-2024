package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// LimitExecutor limits the number of tuples returned by the child executor.
type LimitExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan  *planner.LimitNode
	child Executor

	numEmitted int
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

func NewLimitExecutor(plan *planner.LimitNode, child Executor) *LimitExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &LimitExecutor{
		plan:  plan,
		child: child,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *LimitExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *LimitExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.numEmitted = 0
	return e.child.Init(ctx)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *LimitExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.numEmitted >= e.plan.Limit {
		return false
	}

	if e.child.Next() {
		e.numEmitted++
		return true
	}
	return false
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *LimitExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Current()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *LimitExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Error()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *LimitExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
