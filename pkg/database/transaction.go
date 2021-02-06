package database

import(
	"strconv"
)

// Todo: Add type for QueryResult and return cursor
type QueryResult [](*Row)

type Transaction struct {
	Result QueryResult
	Err    error // Query will fall-through on error
	Table  *Table

	db *Database
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

func (tx *Transaction) UpsertRow(entries map[string]string) {
	if tx.Err != nil {
		return
	}

	var row *Row

	rowId, rowIdExists := entries["id"]
	if rowIdExists {
		row, tx.Err = tx.Table.Update(rowId, entries)
	} else {
		row, tx.Err = tx.Table.Insert(entries)
	}

	tx.Result = QueryResult{row}
}

func (tx *Transaction) DeleteRow(rowId string) {
	if tx.Err != nil {
		return
	}

	tx.Err = tx.Table.Delete(rowId)
}
