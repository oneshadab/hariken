package database

import (
	"fmt"
	"strconv"

	"github.com/oneshadab/hariken/pkg/storage"
)

type Table struct {
	store      *storage.Store
	lastUsedId int
}

func LoadTable(filepath string) (*Table, error) {
	var err error

	table := &Table{}
	table.store, err = storage.NewStore(filepath)

	if err != nil {
		return nil, err
	}

	return table, nil
}

func (T *Table) Get(rowId RowId) (*Row, error) {
	rowData, err := T.store.Get(string(rowId))
	if err != nil {
		return nil, err
	}

	row := &Row{}
	err = row.Deserialize(rowData)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (T *Table) Upsert(row *Row) error {
	if row.Id == nil {
		// Ensure row.Id exists
		row.Id = T.NextId()
	}

	rowData, err := row.Serialize()
	if err != nil {
		return err
	}

	err = T.store.Set(string(*row.Id), *rowData)
	if err != nil {
		return err
	}

	return nil
}

func (T *Table) Remove(row *Row) error {
	rowExists, err := T.store.Has(string(*row.Id))
	if err != nil {
		return err
	}

	if !rowExists {
		return fmt.Errorf("Row with id `%v` not found", row.Id)
	}

	err = T.store.Delete(string(*row.Id))
	if err != nil {
		return err
	}

	return nil
}

func (T *Table) NextId() *RowId {
	id := RowId(strconv.Itoa(T.lastUsedId))
	T.lastUsedId++
	return &id
}
