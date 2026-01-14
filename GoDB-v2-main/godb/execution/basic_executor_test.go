package execution

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/logging"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// setupTestHeap creates a table with columns (id int, name string) and populates it with 'n' tuples.
func setupTestHeap(t *testing.T, n int) (*storage.BufferPool, *TableHeap) {
	testDir := t.TempDir()
	sm := storage.NewDiskStorageManager(testDir)
	bp := storage.NewBufferPool(20, sm)
	lm := transaction.NewLockManager()

	tableSchema := &catalog.Table{
		Oid:  1,
		Name: "test_table",
		Columns: []catalog.Column{
			{Name: "id", Type: common.IntType},
			{Name: "name", Type: common.StringType},
		},
	}

	th, err := NewTableHeap(tableSchema, bp, logging.NoopLogManager{}, lm)
	require.NoError(t, err)

	desc := th.StorageSchema()
	for i := 0; i < n; i++ {
		tup := storage.FromValues(
			common.NewIntValue(int64(i)),
			common.NewStringValue(fmt.Sprintf("row-%d", i)),
		)
		raw := make([]byte, desc.BytesPerTuple())
		tup.WriteToBuffer(raw, desc)
		_, err := th.InsertTuple(nil, raw)
		require.NoError(t, err)
	}

	return bp, th
}

func TestBasicExecutor_SeqScan(t *testing.T) {
	_, th := setupTestHeap(t, 10)

	// 2. Create Scan Node
	// We request a Shared (S) lock for reading.
	scanNode := planner.NewSeqScanNode(th.oid, th.StorageSchema().GetFieldTypes(), transaction.LockModeS)
	scanExec := NewSeqScanExecutor(scanNode, th)
	ctx := NewExecutorContext(nil, 0)

	require.NoError(t, scanExec.Init(ctx))

	count1 := 0
	for scanExec.Next() {
		tup := scanExec.Current()

		// Verify ID Column (Index 0)
		expectedID := int64(count1)
		valID := tup.GetValue(0)
		assert.Equal(t, expectedID, valID.IntValue(), "Pass 1: Tuple ID mismatch at row %d", count1)

		// Verify Name Column (Index 1)
		expectedName := fmt.Sprintf("row-%d", count1)
		valName := tup.GetValue(1)
		assert.Equal(t, expectedName, valName.StringValue(), "Pass 1: Tuple Name mismatch at row %d", count1)

		count1++
	}
	assert.Equal(t, 10, count1, "Pass 1: SeqScan failed to return all tuples")

	// Calling init again should reset the cursor and scan multiple times
	require.NoError(t, scanExec.Init(ctx))

	count2 := 0
	for scanExec.Next() {
		tup := scanExec.Current()
		expectedID := int64(count2)
		valID := tup.GetValue(0)
		assert.Equal(t, expectedID, valID.IntValue(), "Pass 2: Tuple ID mismatch at row %d", count2)

		count2++
	}
	assert.Equal(t, 10, count2, "Pass 2: Re-initialized SeqScan failed to return all tuples")
}

func TestBasicExecutor_Filter(t *testing.T) {
	_, th := setupTestHeap(t, 10)

	scanNode := planner.NewSeqScanNode(th.oid, []common.Type{common.IntType, common.StringType}, transaction.LockModeS)
	scanExec := NewSeqScanExecutor(scanNode, th)

	colExpr := planner.NewColumnValueExpression(0, scanNode.OutputSchema(), "id")
	constExpr := planner.NewConstantValueExpression(common.NewIntValue(5))
	predicate := planner.NewComparisonExpression(colExpr, constExpr, planner.GreaterThan)

	filterExec := NewFilter(planner.NewFilterNode(scanNode, predicate), scanExec)

	require.NoError(t, filterExec.Init(NewExecutorContext(nil, 0)))

	count := 0
	for filterExec.Next() {
		current := filterExec.Current()
		assert.True(t, current.GetValue(0).IntValue() > 5)
		count++
	}
	assert.Equal(t, 4, count, "Should match IDs 6, 7, 8, 9")
}

func TestBasicExecutor_Projection(t *testing.T) {
	_, th := setupTestHeap(t, 5)

	scanNode := planner.NewSeqScanNode(th.oid, th.StorageSchema().GetFieldTypes(), transaction.LockModeS)
	scanExec := NewSeqScanExecutor(scanNode, th)

	idCol := planner.NewColumnValueExpression(0, th.StorageSchema().GetFieldTypes(), "id")
	nameCol := planner.NewColumnValueExpression(1, th.StorageSchema().GetFieldTypes(), "name")
	constVal := planner.NewConstantValueExpression(common.NewStringValue("static"))

	// Project: ["static", name, id, id]
	exprs := []planner.Expr{constVal, nameCol, idCol, idCol}

	projExec := NewProjectionExecutor(planner.NewProjectionNode(scanNode, exprs), scanExec)
	require.NoError(t, projExec.Init(NewExecutorContext(nil, 0)))

	for projExec.Next() {
		tup := projExec.Current()
		require.Equal(t, 4, tup.NumColumns())

		v0 := tup.GetValue(0)
		assert.Equal(t, "static", v0.StringValue())

		v1 := tup.GetValue(1)
		assert.Contains(t, v1.StringValue(), "row-")

		v2 := tup.GetValue(2)
		v3 := tup.GetValue(3)
		assert.Equal(t, v2.IntValue(), v3.IntValue())
	}
}

func TestBasicExecutor_Limit(t *testing.T) {
	_, th := setupTestHeap(t, 10)

	scanNode := planner.NewSeqScanNode(th.oid, th.StorageSchema().GetFieldTypes(), transaction.LockModeS)

	scanExec := NewSeqScanExecutor(scanNode, th)
	limitExec := NewLimitExecutor(planner.NewLimitNode(scanNode, 5), scanExec)
	require.NoError(t, limitExec.Init(NewExecutorContext(nil, 0)))

	count := 0
	for limitExec.Next() {
		count++
	}
	assert.Equal(t, 5, count)

	scanExec = NewSeqScanExecutor(scanNode, th)
	limitExec0 := NewLimitExecutor(planner.NewLimitNode(scanNode, 0), scanExec)
	require.NoError(t, limitExec0.Init(NewExecutorContext(nil, 0)))
	assert.False(t, limitExec0.Next())

	scanExec = NewSeqScanExecutor(scanNode, th)
	limitExecMax := NewLimitExecutor(planner.NewLimitNode(scanNode, 100), scanExec)
	require.NoError(t, limitExecMax.Init(NewExecutorContext(nil, 0)))
	count = 0
	for limitExecMax.Next() {
		count++
	}
	assert.Equal(t, 10, count)
}

func TestBasicExecutor_BasicPipeline(t *testing.T) {
	_, th := setupTestHeap(t, 20)

	scanNode := planner.NewSeqScanNode(th.oid, []common.Type{common.IntType, common.StringType}, transaction.LockModeS)
	scanExec := NewSeqScanExecutor(scanNode, th)

	// 1. Filter: id > 5
	idCol := planner.NewColumnValueExpression(0, scanNode.OutputSchema(), "id")
	f1 := NewFilter(planner.NewFilterNode(scanNode,
		planner.NewComparisonExpression(idCol, planner.NewConstantValueExpression(common.NewIntValue(5)), planner.GreaterThan)),
		scanExec)

	// 2. Project: Swap to (name, id)
	nameCol := planner.NewColumnValueExpression(1, scanNode.OutputSchema(), "name")
	projNode := planner.NewProjectionNode(nil, []planner.Expr{nameCol, idCol})
	projExec := NewProjectionExecutor(projNode, f1)

	// 3. Filter: id < 15 (Note: id is now index 1)
	idProjCol := planner.NewColumnValueExpression(1, []common.Type{common.StringType, common.IntType}, "id")
	f2 := NewFilter(planner.NewFilterNode(nil,
		planner.NewComparisonExpression(idProjCol, planner.NewConstantValueExpression(common.NewIntValue(15)), planner.LessThan)),
		projExec)

	// 4. Limit: 3
	limitExec := NewLimitExecutor(planner.NewLimitNode(nil, 3), f2)

	require.NoError(t, limitExec.Init(NewExecutorContext(nil, 0)))

	// Matches > 5 and < 15: 6, 7, 8, 9, 10, 11, 12, 13, 14.
	// Limit 3 -> Should get 6, 7, 8.

	expectedIDs := []int64{6, 7, 8}
	var results []int64
	for limitExec.Next() {
		// Output is (name, id)
		current := limitExec.Current()
		results = append(results, current.GetValue(1).IntValue())
	}
	assert.Equal(t, expectedIDs, results)
}

func TestBasicExecutor_Restart(t *testing.T) {
	_, th := setupTestHeap(t, 10)

	scanExec := NewSeqScanExecutor(planner.NewSeqScanNode(th.oid, th.StorageSchema().GetFieldTypes(), transaction.LockModeS), th)
	limitExec := NewLimitExecutor(planner.NewLimitNode(nil, 2), scanExec)
	ctx := NewExecutorContext(nil, 0)

	// Run 1
	require.NoError(t, limitExec.Init(ctx))
	assert.True(t, limitExec.Next()) // 0
	assert.True(t, limitExec.Next()) // 1
	assert.False(t, limitExec.Next())

	// Run 2 (Restart)
	require.NoError(t, limitExec.Init(ctx))
	assert.True(t, limitExec.Next()) // 0
	current := limitExec.Current()
	assert.Equal(t, int64(0), current.GetValue(0).IntValue())
}
