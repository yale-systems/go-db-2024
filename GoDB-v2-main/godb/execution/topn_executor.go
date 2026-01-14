package execution

import (
	"container/heap"
	"sort"

	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// TopNExecutor uses a heap to keep the top N tuples.
type TopNExecutor struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	plan  *planner.TopNNode
	child Executor

	sortedTuples []storage.Tuple
	computed     bool
	currentIndex int
	ctx          *ExecutorContext
	err          error
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Fill me in!
	// </insert>
}

func NewTopNExecutor(plan *planner.TopNNode, child Executor) *TopNExecutor {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &TopNExecutor{
		plan:  plan,
		child: child,
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *TopNExecutor) PlanNode() planner.PlanNode {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.plan
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *TopNExecutor) Init(ctx *ExecutorContext) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.sortedTuples = nil
	e.computed = false
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
func compareTuples(t1, t2 storage.Tuple, orderBy []planner.OrderByClause) int {
	for _, order := range orderBy {
		v1 := order.Expr.Eval(t1)
		v2 := order.Expr.Eval(t2)
		cmp := v1.Compare(v2)
		if cmp == 0 {
			continue
		}

		if order.Direction == planner.SortOrderAscending {
			// ASC: Smaller values come first.
			return cmp
		}
		// DESC: Larger values come first.
		return -cmp
	}
	return 0
}

// tupleHeap implements heap.Interface
type tupleHeap struct {
	tuples  []storage.Tuple
	orderBy []planner.OrderByClause
}

func (h *tupleHeap) Len() int { return len(h.tuples) }

func (h *tupleHeap) Swap(i, j int) { h.tuples[i], h.tuples[j] = h.tuples[j], h.tuples[i] }

func (h *tupleHeap) Less(i, j int) bool {
	// Heap logic is backwards from normal -- to keep smallest elements, use a max heap, and vice versa
	return compareTuples(h.tuples[i], h.tuples[j], h.orderBy) > 0
}

func (h *tupleHeap) Push(x any) {
	h.tuples = append(h.tuples, x.(storage.Tuple))
}

func (h *tupleHeap) Pop() any {
	old := h.tuples
	n := len(old)
	x := old[n-1]
	h.tuples = old[0 : n-1]
	return x
}

func (e *TopNExecutor) computeTopN() error {
	schema := storage.NewRawTupleDesc(e.plan.OutputSchema())
	h := &tupleHeap{
		tuples:  make([]storage.Tuple, 0, e.plan.Limit+1),
		orderBy: e.plan.OrderBy,
	}
	heap.Init(h)

	for e.child.Next() {
		current := e.child.Current()
		tuple := current.DeepCopy(schema)

		if h.Len() < e.plan.Limit {
			heap.Push(h, tuple)
		} else {
			heap.Push(h, tuple)
			heap.Pop(h)
		}
	}

	if err := e.child.Error(); err != nil {
		return err
	}

	// heap is not sorted but in tree order -- sort this in the correct order, which is the reverse of the heap order
	sort.Slice(h.tuples, func(i, j int) bool {
		return compareTuples(h.tuples[i], h.tuples[j], e.plan.OrderBy) < 0
	})
	e.sortedTuples = h.tuples
	return nil
}

// </silentstrip>

func (e *TopNExecutor) Next() bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	if e.err != nil {
		return false
	}
	if !e.computed {
		e.err = e.computeTopN()
		e.computed = true
		if e.err != nil {
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

func (e *TopNExecutor) Current() storage.Tuple {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.sortedTuples[e.currentIndex]
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

func (e *TopNExecutor) Error() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	return e.err
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert
}

func (e *TopNExecutor) Close() error {
	// <silentstrip lab1|lab2|lab3|lab4>
	e.sortedTuples = nil
	return e.child.Close()
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert
}
