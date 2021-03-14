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

func (lm *lockManager) getTableLock(tableId string) *txLock {
	locker := lm.getLocker(tableId)

	// Ensure lock exists
	if locker.tableLock == nil {
		locker.tableLock = &txLock{}
	}

	return locker.tableLock
}

func (lm *lockManager) getRowLock(tableId string, rowId string) *txLock {
	locker := lm.getLocker(tableId)

	// Ensure lock exists
	if locker.rowLock[rowId] == nil {
		locker.rowLock[rowId] = &txLock{}
	}

	return locker.rowLock[rowId]
}

func (lm *lockManager) getLocker(tableId string) *tableLocker {
	if lm.lockers[tableId] == nil {
		lm.lockers[tableId] = &tableLocker{}
	}

	return lm.lockers[tableId]
}

func (tl *txLock) lockForTx(txId string) {
	if *tl.txId == txId {
		// Lock is already owned by current transaction so nothing to lock
		return
	}

	// Lock and set owner to current transaction
	tl.lock.Lock()
	tl.txId = &txId
}

func (tl *txLock) unlockForTx(txId string) {
	if *tl.txId != txId {
		// Lock is not owned by current transaction so cannot release
		return
	}

	// Unlock and remove owner
	tl.lock.Unlock()
	tl.txId = nil
}
