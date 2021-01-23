package database

import (
	"fmt"
	"path/filepath"
	"strconv"

	"github.com/oneshadab/hariken/pkg/storage"
)

type Table struct {
	metaDataStore *storage.Store
	rowStore      *storage.Store
}

func LoadTable(tableDir string) (*Table, error) {
	var err error

	table := &Table{}

	table.metaDataStore, err = storage.NewStore(filepath.Join(tableDir, "metadata"))
	if err != nil {
		return nil, err
	}

	table.rowStore, err = storage.NewStore(filepath.Join(tableDir, "data"))
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (T *Table) Get(rowId string) (*Row, error) {
	rowData, err := T.rowStore.Get(rowId)
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

	rowId, err := T.NextId()
	if err != nil {
		return nil, err
	}
	row.setId(rowId)

	rowData, err := row.Serialize()
	if err != nil {
		return nil, err
	}

	err = T.rowStore.Set(row.Id(), *rowData)
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

	err = T.rowStore.Set(row.Id(), *rowData)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (T *Table) Delete(rowId string) error {
	rowExists, err := T.rowStore.Has(rowId)
	if err != nil {
		return err
	}

	if !rowExists {
		return fmt.Errorf("Row with id `%v` not found", rowId)
	}

	err = T.rowStore.Delete(rowId)
	if err != nil {
		return err
	}

	return nil
}

func (T *Table) NextId() (string, error) {
	const primaryKey = "lastUsedId" // Todo: move to constant

	idStr, err := T.metaDataStore.Get(primaryKey)
	if err != nil {
		return "", err
	}

	var lastUsedId int
	if idStr == nil {
		lastUsedId = 0
		tmp := "0"
		idStr = &tmp
	} else {
		lastUsedId, err = strconv.Atoi(*idStr)
		if err != nil {
			return "", err
		}
	}

	lastUsedId++

	newIdStr := strconv.Itoa(lastUsedId)
	err = T.metaDataStore.Set(primaryKey, newIdStr)
	if err != nil {
		return "", nil
	}

	return *idStr, nil
}
