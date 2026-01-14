package transaction

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
)

// IndexCleanupCallback is the interface implemented by indexes to support rolling back changes (undo) in the event of
// a transaction abort. It's necessary because the current version of GoDB has in-memory indexes, which means that our
// ARIES-integrated undo code path does not work for indexes.
//
// YOU SHOULD NOT NEED TO IMPLEMENT OR MODIFY THIS INTERFACE.
type IndexCleanupCallback interface {
	Invoke(opType CleanupType, key storage.RawTuple, rid common.RecordID)
}

type CleanupType int

const (
	CleanupTypeUndoInsert CleanupType = iota
	CleanupTypeUndoDelete
)

// IndexCleanupTask represents a single undo action for an index (e.g., removing a key
// that was inserted by the current transaction).
// It is a value struct (not a pointer) to avoid heap allocation per op.
//
// YOU SHOULD NOT NEED TO MANIPULATE THIS STRUCT.
type IndexCleanupTask struct {
	Target IndexCleanupCallback // The Index instance (as an interface)
	Type   CleanupType
	Key    storage.RawTuple
	RID    common.RecordID
}
