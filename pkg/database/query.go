package database

// Todo: Add type for QueryResult
type QueryResult [](*Row)

type Query struct {
	Result QueryResult
	Err    error

	table *Table
}

func NewQuery(db *Database, tableName string) *Query {
	query := &Query{}
	query.table, query.Err = db.Table(tableName)
	return query
}

func (q *Query) Get(rowId RowId) *Query {
	if q.Err != nil {
		return q
	}

	var row *Row
	row, q.Err = q.table.Get(rowId)
	if q.Err != nil {
		return q
	}

	q.Result = QueryResult{row}
	return q
}

func (q *Query) Exec() (QueryResult, error) {
	return q.Result, q.Err
}
