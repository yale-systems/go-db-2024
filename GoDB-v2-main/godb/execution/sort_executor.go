package execution

import (
	"sort"

	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SortExecutor sorts the input tuples based on the provided ordering expressions.
// It is a blocking operator but uses lazy evaluation (sorts on first Next).
type SortExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan  *planner.SortNode
	child Executor

	// Runtime state
	sortedTuples []storage.Tuple
	currentIndex int
	ctx          *ExecutorContext
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

func NewSortExecutor(plan *planner.SortNode, child Executor) *SortExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &SortExecutor{
		plan:  plan,
		child: child,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SortExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SortExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.sortedTuples = nil
	e.currentIndex = -1
	e.ctx = ctx
	return e.child.Init(ctx)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// <silentstrip lab1|lab2|lab3|lab4>
func (e *SortExecutor) sortAllRows() bool {
	schema := storage.NewRawTupleDesc(e.plan.OutputSchema())

	for e.child.Next() {
		t := e.child.Current()
		e.sortedTuples = append(e.sortedTuples, t.DeepCopy(schema))
	}

	if e.child.Error() != nil {
		return false
	}

	sort.Slice(e.sortedTuples, func(i, j int) bool {
		t1 := e.sortedTuples[i]
		t2 := e.sortedTuples[j]
		for _, order := range e.plan.OrderBy {
			v1 := order.Expr.Eval(t1)
			v2 := order.Expr.Eval(t2)
			cmp := v1.Compare(v2)
			if cmp == 0 {
				continue
			}
			if order.Direction == planner.SortOrderAscending {
				return cmp < 0
			}
			return cmp > 0
		}
		return false
	})
	return true
}

// </silentstrip>

func (e *SortExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.sortedTuples == nil {
		if !e.sortAllRows() {
			return false
		}
	}
	e.currentIndex++
	return e.currentIndex < len(e.sortedTuples)
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SortExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.sortedTuples[e.currentIndex]
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SortExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.child.Error()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *SortExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.sortedTuples = nil
	return e.child.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
