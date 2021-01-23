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

func (T *Table) Get(rowId string) (*Row, error) {
	rowData, err := T.store.Get(string(rowId))
	if err != nil {
		return nil, err
	}
	if rowData == nil {
		return nil, nil
	}

	row := NewRow()
	err = row.Deserialize(rowData)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (T *Table) Insert(entries map[string]string) (*Row, error) {
	row := NewRow()
	for k, v := range entries {
		row.Column[k] = v
	}
	row.setId(T.NextId())

	rowData, err := row.Serialize()
	if err != nil {
		return nil, err
	}

	err = T.store.Set(row.Id(), *rowData)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (T *Table) Update(rowId string, entries map[string]string) (*Row, error) {
	row, err := T.Get(rowId)
	if err != nil {
		return nil, err
	}

	if row == nil {
		return nil, fmt.Errorf("Row with id `%v` not found", rowId)
	}

	for k, v := range entries {
		row.Column[k] = v
	}

	rowData, err := row.Serialize()
	if err != nil {
		return nil, err
	}

	err = T.store.Set(row.Id(), *rowData)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (T *Table) Delete(rowId string) error {
	rowExists, err := T.store.Has(rowId)
	if err != nil {
		return err
	}

	if !rowExists {
		return fmt.Errorf("Row with id `%v` not found", rowId)
	}

	err = T.store.Delete(rowId)
	if err != nil {
		return err
	}

	return nil
}

func (T *Table) NextId() string {
	id := strconv.Itoa(T.lastUsedId)
	T.lastUsedId++
	return id
}
