package database

import (
	"strconv"
)

// Todo: Add type for QueryResult and return cursor
type QueryResult [](*Row)

type Transaction struct {
	Id                    string
	Result                QueryResult
	Err                   error // Query will fall-through on error
	Table                 *Table
	ProcessedCommandTypes map[string]bool

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

func (tx *Transaction) Cleanup() {
	// Release all held locks

	for i := range tx.locks {
		tx.locks[i].unlockForTx(tx.Id)
	}
}

func (tx *Transaction) AddLock(lock *txLock) {
	tx.locks = append(tx.locks, lock)
}

func (tx *Transaction) lockTable() {
	lock := tx.db.lockManager.getTableLock(tx.Table.name)
	lock.lockForTx(tx.Id)
	tx.AddLock(lock)
}

func (tx *Transaction) lockRow(rowId string) {
	lock := tx.db.lockManager.getRowLock(tx.Table.name, rowId)
	lock.lockForTx(tx.Id)
	tx.AddLock(lock)
}
