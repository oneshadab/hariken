package database

import "sync"

type lockManager struct {
	lockers map[string]*tableLocker
}

type tableLocker struct {
	tableLock *txLock
	rowLock   map[string]*txLock
}

type txLock struct {
	txId *string
	lock sync.RWMutex
}

func newLockManager() *lockManager {
	return &lockManager{
		lockers: make(map[string]*tableLocker),
	}
}

func newTableLocker() *tableLocker {
	return &tableLocker{
		rowLock: make(map[string]*txLock),
	}
}

func newTxLock() *txLock {
	return &txLock{}
}

func (lm *lockManager) getTableLock(tableId string) *txLock {
	locker := lm.getLocker(tableId)

	// Ensure lock exists
	if locker.tableLock == nil {
		locker.tableLock = newTxLock()
	}

	return locker.tableLock
}

func (lm *lockManager) getRowLock(tableId string, rowId string) *txLock {
	locker := lm.getLocker(tableId)

	// Ensure lock exists
	if locker.rowLock[rowId] == nil {
		locker.rowLock[rowId] = newTxLock()
	}

	return locker.rowLock[rowId]
}

func (lm *lockManager) getLocker(tableId string) *tableLocker {
	// Ensure locker exists
	if lm.lockers[tableId] == nil {
		lm.lockers[tableId] = newTableLocker()
	}

	return lm.lockers[tableId]
}
