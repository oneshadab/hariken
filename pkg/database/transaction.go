package database

// Todo: Add type for QueryResult and return cursor
type QueryResult [](*Row)

type Transaction struct {
	Result QueryResult
	Err    error // Query will fall-through on error

	db    *Database
	table *Table
}

func (tx *Transaction) UseTable(tableName string) {
	if tx.Err != nil {
		return
	}

	tx.table, tx.Err = tx.db.Table(tableName)
}

func (tx *Transaction) FetchRow(rowId string) {
	if tx.Err != nil {
		return
	}

	var row *Row
	row, tx.Err = tx.table.Get(rowId)

	tx.Result = QueryResult{row}
}

func (tx *Transaction) UpsertRow(entries map[string]string) {
	if tx.Err != nil {
		return
	}

	var row *Row

	rowId, rowIdExists := entries["id"]
	if rowIdExists {
		row, tx.Err = tx.table.Update(rowId, entries)
	} else {
		row, tx.Err = tx.table.Insert(entries)
	}

	tx.Result = QueryResult{row}
}

func (tx *Transaction) DeleteRow(rowId string) {
	if tx.Err != nil {
		return
	}

	tx.Err = tx.table.Delete(rowId)
}
