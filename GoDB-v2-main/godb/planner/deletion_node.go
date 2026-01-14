package planner

import (
	"fmt"

	"mit.edu/dsg/godb/common"
)

// DeletionNode represents a deletion from a table.
type DeletionNode struct {
	TableOid common.ObjectID
	Child    PlanNode
}

func NewDeleteNode(tableOid common.ObjectID, child PlanNode) *DeletionNode {
	return &DeletionNode{
		TableOid: tableOid,
		Child:    child,
	}
}

func (n *DeletionNode) OutputSchema() []common.Type {
	return []common.Type{common.IntType} // Returns count of deleted rows
}

func (n *DeletionNode) Children() []PlanNode {
	return []PlanNode{n.Child}
}

func (n *DeletionNode) String() string {
	return fmt.Sprintf("Delete: TableOID(%d)", n.TableOid)
}
