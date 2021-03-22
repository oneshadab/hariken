package database

import (
	"strconv"
)

// Todo: Add type for QueryResult and return cursor
type QueryResult [](*Row)

type Transaction struct {
	Id     string
	Result QueryResult
	Err    error // Query will fall-through on error
	Table  *Table

	locks []*txLock
	db    *Database
}

func (tx *Transaction) UseTable(tableName string) {
	if tx.Err != nil {
		return
	}

	tx.Table, tx.Err = tx.db.Table(tableName)
}

func (tx *Transaction) FetchAll() {
	tx.Result = QueryResult{}

	if tx.Err != nil {
		return
	}

	lastUsedIdStr, err := tx.Table.GetLastUsedId()
	if err != nil {
		return
	}

	if lastUsedIdStr == "" {
		return
	}

	lastUsedId, err := strconv.Atoi(lastUsedIdStr)
	if err != nil {
		return
	}

	for id := 0; id <= lastUsedId; id++ {
		idStr := strconv.Itoa(id)
		var row *Row
		row, tx.Err = tx.Table.Get(idStr)
		if tx.Err != nil {
			return
		}

		if row != nil {
			tx.Result = append(tx.Result, row)
		}
	}
}

func (tx *Transaction) FetchRow(rowId string) {
	if tx.Err != nil {
		return
	}

	var row *Row
	row, tx.Err = tx.Table.Get(rowId)

	tx.Result = QueryResult{row}
}

func (tx *Transaction) InsertRow(entries map[string]string) {
	if tx.Err != nil {
		return
	}

	tx.lockTable()

	var row *Row
	row, tx.Err = tx.Table.Insert(entries)

	tx.Result = QueryResult{row}
}

func (tx *Transaction) Filter(key string, expectedValue string) {
	if tx.Err != nil {
		return
	}

	filteredResult := QueryResult{}
	for _, row := range tx.Result {
		rowValue, _ := row.Column[key]
		if rowValue == expectedValue {
			filteredResult = append(filteredResult, row)
		}
	}

	tx.Result = filteredResult
}

func (tx *Transaction) UpdateAll(entries map[string]string) {
	if tx.Err != nil {
		return
	}

	for i := range tx.Result {
		rowId := tx.Result[i].Id()

		tx.lockRow(rowId)
		tx.Result[i], tx.Err = tx.Table.Update(rowId, entries)

		if tx.Err != nil {
			return
		}
	}
}

func (tx *Transaction) DeleteRow(rowId string) {
	if tx.Err != nil {
		return
	}

	tx.lockRow(rowId)
	tx.Err = tx.Table.Delete(rowId)
}

// Dummy commit method that only calls cleanup
func (tx *Transaction) Commit() {
	tx.Cleanup()
}

func (tx *Transaction) Cleanup() {
	// Release all held locks
	for i := range tx.locks {
		tx.releaseLock(tx.locks[i])
	}
}

func (tx *Transaction) lockTable() {
	tableLock := tx.db.lockManager.getTableLock(tx.Table.name)
	tx.acquireLock(tableLock)
}

func (tx *Transaction) lockRow(rowId string) {
	rowLock := tx.db.lockManager.getRowLock(tx.Table.name, rowId)
	tx.acquireLock(rowLock)
}

func (tx *Transaction) acquireLock(lock *txLock) {
	if lock.txId != nil && *lock.txId == tx.Id {
		// Lock is already owned by current transaction so nothing to lock
		return
	}

	// Lock and set owner to current transaction
	lock.lock.Lock()
	lock.txId = &tx.Id

	// Add lock to list of acquiredLocks
	tx.locks = append(tx.locks, lock)
}

func (tx *Transaction) releaseLock(lock *txLock) {
	if lock.txId == nil || *lock.txId != tx.Id {
		// Lock is not owned by current transaction so cannot release
		return
	}

	// Unlock and remove lock's owner
	lock.lock.Unlock()
	lock.txId = nil
}
