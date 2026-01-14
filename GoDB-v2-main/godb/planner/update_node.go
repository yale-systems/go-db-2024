package planner

import (
	"fmt"

	"mit.edu/dsg/godb/common"
)

// UpdateNode represents an update to a table.
type UpdateNode struct {
	TableOid       common.ObjectID
	ProjectionList []int
	Child          PlanNode
}

func NewUpdateNode(tableOid common.ObjectID, child PlanNode, projectionList []int) *UpdateNode {
	return &UpdateNode{
		TableOid:       tableOid,
		ProjectionList: projectionList,
		Child:          child,
	}
}

func (n *UpdateNode) OutputSchema() []common.Type {
	return []common.Type{common.IntType}
}

func (n *UpdateNode) Children() []PlanNode {
	return []PlanNode{n.Child}
}

func (n *UpdateNode) String() string {
	return fmt.Sprintf("Update: TableOID(%d)", n.TableOid)
}
