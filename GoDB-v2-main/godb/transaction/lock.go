package transaction

import (
	"fmt"
	"sync"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
)

// DBLockTag identifies a unique resource (Table or Tuple). It represents Tuple if it has a full RecordID, and
// represents a table if only the oid is set and the rest are set to -1
type DBLockTag struct {
	common.RecordID
}

// NewTableLockTag creates a DBLockTag representing a whole table.
func NewTableLockTag(oid common.ObjectID) DBLockTag {
	return DBLockTag{
		RecordID: common.RecordID{
			PageID: common.PageID{
				Oid:     oid,
				PageNum: -1,
			},
			Slot: -1,
		},
	}
}

// NewTupleLockTag creates a DBLockTag representing a specific tuple (row).
func NewTupleLockTag(rid common.RecordID) DBLockTag {
	return DBLockTag{
		RecordID: rid,
	}
}

func (t DBLockTag) String() string {
	if t.PageNum == -1 {
		return fmt.Sprintf("Table(%d)", t.Oid)
	}
	return fmt.Sprintf("Tuple(%d, %d, %d)", t.Oid, t.PageNum, t.Slot)
}

// DBLockMode represents the type of access a transaction is requesting.
// GoDB supports a standard Multi-Granularity Locking hierarchy.
type DBLockMode int

const (
	// LockModeS (Shared) allows reading a resource. Multiple transactions can hold S locks simultaneously.
	LockModeS DBLockMode = iota
	// LockModeX (Exclusive) allows modification. It is incompatible with all other modes.
	LockModeX
	// LockModeIS (Intent Shared) indicates the intention to read resources at a lower level (e.g., locking a table IS to read tuples).
	LockModeIS
	// LockModeIX (Intent Exclusive) indicates the intention to modify resources at a lower level (e.g., locking a table IX to modify tuples).
	LockModeIX
	// LockModeSIX (Shared Intent Exclusive) allows reading the resource (like S) AND the intention to modify lower-level resources (like IX).
	LockModeSIX
)

func (m DBLockMode) String() string {
	switch m {
	case LockModeS:
		return "LockModeS"
	case LockModeX:
		return "LockModeX"
	case LockModeIS:
		return "LockModeIS"
	case LockModeIX:
		return "LockModeIX"
	case LockModeSIX:
		return "LockModeSIX"
	}
	return "Unknown lock mode"
}

// <silentstrip lab1|lab2|lab3|lab4>

const NumDBLockModes = 5

var compatibilityMatrix = [NumDBLockModes][NumDBLockModes]bool{
	//         LockModeS      LockModeX      LockModeIS     LockModeIX     LockModeSIX
	/* LockModeS */ {true, false, true, false, false},
	/* LockModeX */ {false, false, false, false, false},
	/* LockModeIS */ {true, false, true, true, true},
	/* LockModeIX */ {false, false, true, true, false},
	/* LockModeSIX */ {false, false, true, false, false},
}

func Compatible(req, held DBLockMode) bool {
	return compatibilityMatrix[req][held]
}

// lockCoverageMatrix defines if a held lock suffices for a requested lock.
var lockCoverageMatrix = [NumDBLockModes][NumDBLockModes]bool{
	//             Held S  Held X  Held IS Held IX Held SIX
	/* Req S */ {true, true, false, false, true},
	/* Req X */ {false, true, false, false, false},
	/* Req IS */ {true, true, true, true, true},
	/* Req IX */ {false, true, false, true, true},
	/* Req SIX */ {false, true, false, false, true},
}

// CoveredBy returns true if the 'held' lock is strong enough to satisfy the 'req' lock.
// This returns true for identity (e.g., CoveredBy(LockModeX, LockModeX) is true).
func CoveredBy(req, held DBLockMode) bool {
	return lockCoverageMatrix[req][held]
}

func Upgradeable(req, held DBLockMode) bool {
	switch held {
	case LockModeIS:
		return req == LockModeS || req == LockModeIX || req == LockModeSIX || req == LockModeX
	case LockModeS:
		return req == LockModeSIX || req == LockModeX
	case LockModeIX:
		return req == LockModeSIX || req == LockModeX
	case LockModeSIX:
		return req == LockModeX
	default: // LockModeX cannot upgrade
		return false
	}
}

type dbLockHolder struct {
	txnID common.TransactionID
	mode  DBLockMode
}

type dbLockRequest struct {
	dbLockHolder
	// For use when upgrading
	selfIdx int
	granted bool
	cond    *sync.Cond
}

type dbLock struct {
	tag        DBLockTag
	heldCounts [NumDBLockModes]int
	holders    []dbLockHolder
	waiters    []*dbLockRequest
	upgraders  []*dbLockRequest

	mutex sync.Mutex
}

func (l *dbLock) initialize(tag DBLockTag) {
	l.tag = tag
	l.heldCounts = [NumDBLockModes]int{}
	l.holders = l.holders[:0]
	l.waiters = l.waiters[:0]
	l.upgraders = l.upgraders[:0]
}

func (l *dbLock) invalidate() {
	l.tag = DBLockTag{}
}

func (l *dbLock) outOfScope() bool {
	if len(l.waiters) != 0 || len(l.upgraders) != 0 {
		return false
	}
	for _, h := range l.holders {
		if h.txnID != common.InvalidTransactionID {
			return false
		}
	}
	return true
}

func (l *dbLock) grantLock(request *dbLockRequest) {
	if request.selfIdx != -1 {
		// This is an upgrade
		l.heldCounts[l.holders[request.selfIdx].mode]--
		l.holders[request.selfIdx].mode = request.mode
	} else {
		slot := -1
		for i := range l.holders {
			if l.holders[i].txnID == common.InvalidTransactionID {
				slot = i
				break
			}
		}
		if slot != -1 {
			// Reuse slot
			l.holders[slot] = request.dbLockHolder
			request.selfIdx = slot
		} else {
			// Append new slot
			request.selfIdx = len(l.holders)
			l.holders = append(l.holders, request.dbLockHolder)
		}
	}
	l.heldCounts[request.mode]++
	request.granted = true
	if request.cond != nil {
		request.cond.Signal()
	}
}

func (l *dbLock) canGrant(r *dbLockRequest) bool {
	for m, c := range l.heldCounts {
		if c == 0 || Compatible(r.mode, DBLockMode(m)) {
			continue
		}
		// Need to check if we're upgrading our own lock'
		if r.selfIdx != -1 && c == 1 && l.holders[r.selfIdx].mode == DBLockMode(m) {
			continue
		}
		return false
	}
	return true
}

func (l *dbLock) lock(txnID common.TransactionID, mode DBLockMode) error {
	selfIdx := -1
	blocked := false

	for i, h := range l.holders {
		if h.txnID == common.InvalidTransactionID {
			continue
		} else if h.txnID == txnID {
			selfIdx = i
			if !Upgradeable(mode, h.mode) {
				panic(fmt.Sprintf("invalid lock upgrade %s -> %s", h.mode, mode))
			}
		} else if !Compatible(mode, h.mode) {
			if txnID > h.txnID {
				return common.GoDBError{
					Code:      common.DeadlockError,
					ErrString: fmt.Sprintf("deadlock (wait-die): txn %d aborting for holder %d", txnID, h.txnID),
				}
			}
			blocked = true
		}
	}

	for _, u := range l.upgraders {
		if u.txnID == txnID {
			panic("attempting to upgrade a transaction that is still waiting")
		}
		if txnID > u.txnID && !Compatible(mode, u.mode) {
			return common.GoDBError{
				Code:      common.DeadlockError,
				ErrString: fmt.Sprintf("deadlock (wait-die): txn %d aborting for waiter %d", txnID, u.txnID),
			}
		}
		blocked = true
	}

	// Upgrades ignore the wait queue because they are logically ahead of the waiters in the queue
	if selfIdx == -1 {
		for _, w := range l.waiters {
			if w.txnID == txnID {
				panic("attempting to upgrade a transaction that is still waiting")
			}
			if txnID > w.txnID && !Compatible(mode, w.mode) {
				return common.GoDBError{
					Code:      common.DeadlockError,
					ErrString: fmt.Sprintf("deadlock (wait-die): txn %d aborting for waiter %d", txnID, w.txnID),
				}
			}
			blocked = true
		}
	} else {

	}

	if !blocked {
		l.grantLock(&dbLockRequest{
			dbLockHolder: dbLockHolder{
				txnID: txnID,
				mode:  mode,
			},
			selfIdx: selfIdx,
		})
		return nil
	}

	request := dbLockRequest{
		dbLockHolder: dbLockHolder{
			txnID: txnID,
			mode:  mode,
		},
		selfIdx: selfIdx,
		cond:    sync.NewCond(&l.mutex),
	}
	if selfIdx == -1 {
		l.waiters = append(l.waiters, &request)
	} else {
		l.upgraders = append(l.upgraders, &request)
	}

	for {
		request.cond.Wait()
		// If the lock has been granted, we can unblock
		if request.granted {
			return nil
		}
	}
	return nil
}

// unlock releases the lock.
func (l *dbLock) unlock(tid common.TransactionID) {
	for i, h := range l.holders {
		if h.txnID == tid {
			l.heldCounts[h.mode]--
			l.holders[i].txnID = common.InvalidTransactionID
			break
		}
	}

	// Grant locks
	i := 0
	for i < len(l.upgraders) {
		u := l.upgraders[i]
		if !l.canGrant(u) {
			break
		}
		l.grantLock(u)
		l.upgraders[i] = nil
		i++
	}
	l.upgraders = l.upgraders[i:]

	if len(l.upgraders) != 0 {
		return
	}

	i = 0
	for i < len(l.waiters) {
		w := l.waiters[i]
		if !l.canGrant(w) {
			break
		}
		l.grantLock(w)
		l.waiters[i] = nil
		i++
	}
	l.waiters = l.waiters[i:]
}

// </silentstrip>

// LockManager manages the granting, releasing, and waiting of locks on database resources.
type LockManager struct {
	// <silentstrip lab1|lab2|lab3|lab4>
	lockTable  *xsync.MapOf[DBLockTag, *dbLock]
	dbLockPool sync.Pool
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// // Add fields here
	// </insert>
}

// NewLockManager initializes a new LockManager.
func NewLockManager() *LockManager {
	// <silentstrip lab1|lab2|lab3|lab4>
	return &LockManager{
		lockTable: xsync.NewMapOf[DBLockTag, *dbLock](),
		dbLockPool: sync.Pool{
			New: func() any {
				return &dbLock{
					tag:        DBLockTag{},
					heldCounts: [5]int{},
					holders:    make([]dbLockHolder, 0, 16),
					waiters:    make([]*dbLockRequest, 0, 16),
					upgraders:  make([]*dbLockRequest, 0, 4),
					mutex:      sync.Mutex{},
				}
			},
		},
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Lock acquires a lock on a specific resource (Table or Tuple) with the requested mode. If the lock cannot be acquired
// immediately, the transaction blocks until it is granted or aborted. It returns nil if the lock is successfully
// acquired, or GoDBError(DeadlockError) in case of a (potential or detected) deadlock.
func (lm *LockManager) Lock(tid common.TransactionID, tag DBLockTag, mode DBLockMode) error {
	// <silentstrip lab1|lab2|lab3|lab4>
	for {
		lock, ok := lm.lockTable.Load(tag)
		if !ok {
			newLock := lm.dbLockPool.Get().(*dbLock)
			newLock.mutex.Lock()
			newLock.initialize(tag)
			actualLock, loaded := lm.lockTable.LoadOrStore(tag, newLock)
			if loaded {
				newLock.invalidate()
				newLock.mutex.Unlock()
				lm.dbLockPool.Put(newLock)
				lock = actualLock
				lock.mutex.Lock()
			} else {
				lock = newLock
			}
		} else {
			lock.mutex.Lock()
		}

		// Stale check
		if lock.tag != tag {
			lock.mutex.Unlock()
			continue
		}

		err := lock.lock(tid, mode)
		lock.mutex.Unlock()
		return err
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// Unlock releases the lock held by the transaction on the specified resource.
func (lm *LockManager) Unlock(tid common.TransactionID, tag DBLockTag) {
	// <silentstrip lab1|lab2|lab3|lab4>
	lock, ok := lm.lockTable.Load(tag)
	if !ok {
		return
	}

	lock.mutex.Lock()
	defer lock.mutex.Unlock()

	// Stale check
	if lock.tag != tag {
		panic("lock manager unlock called on stale lock")
	}

	lock.unlock(tid)

	if lock.outOfScope() {
		lock.invalidate()
		lm.lockTable.Delete(tag)
		lm.dbLockPool.Put(lock)
	}
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}

// LockHeld checks if any transaction currently holds a lock on the given resource.
func (lm *LockManager) LockHeld(tag DBLockTag) bool {
	// <silentstrip lab1|lab2|lab3|lab4>
	lock, ok := lm.lockTable.Load(tag)
	if !ok {
		return false
	}
	lock.mutex.Lock()
	defer lock.mutex.Unlock()
	if lock.tag != tag {
		return false
	}
	return len(lock.holders) != 0
	// </silentstrip>
	// <insert lab1|lab2|lab3|lab4>
	// panic("unimplemented")
	// </insert>
}
