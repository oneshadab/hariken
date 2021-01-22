package database

// Todo: Add type for QueryResult/return cursor
type QueryResult [](*Row)

type Query struct {
	Result QueryResult
	Err    error // Query will fall-through on error

	table *Table
}

func NewQuery(db *Database, tableName string) *Query {
	query := &Query{}
	query.table, query.Err = db.Table(tableName)
	return query
}

func (q *Query) Get(rowId string) *Query {
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

func (q *Query) Insert(data map[string]string) *Query {
	if q.Err != nil {
		return q
	}

	var row *Row

	row, q.Err = q.table.Insert(data)
	if q.Err != nil {
		return q
	}

	q.Result = QueryResult{row}
	return q
}

func (q *Query) Delete(rowId string) *Query {
	if q.Err != nil {
		return q
	}

	q.Err = q.table.Delete(rowId)
	if q.Err != nil {
		return q
	}

	return q
}

func (q *Query) Exec() (QueryResult, error) {
	return q.Result, q.Err
}
