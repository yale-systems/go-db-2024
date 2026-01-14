package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// ProjectionExecutor evaluates a list of expressions on the input tuples
// and produces a new tuple containing the results of those expressions.
type ProjectionExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan  *planner.ProjectionNode
	child Executor

	// Runtime state
	projectionExprs []common.Value
	err             error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

// NewProjectionExecutor creates a new ProjectionExecutor.
func NewProjectionExecutor(plan *planner.ProjectionNode, child Executor) *ProjectionExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &ProjectionExecutor{
		child: child,
		plan:  plan,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *ProjectionExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *ProjectionExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.projectionExprs = make([]common.Value, len(e.plan.Expressions))
	return e.child.Init(ctx)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *ProjectionExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if !e.child.Next() {
		e.err = e.child.Error()
		return false
	}

	childTuple := e.child.Current()
	for i, expr := range e.plan.Expressions {
		e.projectionExprs[i] = expr.Eval(childTuple)
	}
	return true
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *ProjectionExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return storage.FromValues(e.projectionExprs...)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *ProjectionExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *ProjectionExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
