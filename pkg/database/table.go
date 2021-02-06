package database

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/oneshadab/hariken/pkg/storage"
)

// Todo: Find something better than this
var metadataKeys = struct {
	lastUsedId storage.StoreKey
	columnList storage.StoreKey
}{
	lastUsedId: storage.StoreKey{0},
	columnList: storage.StoreKey{1},
}

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
	rowKey, err := storage.ParseKey(rowId)
	if err != nil {
		return nil, err
	}

	rowData, err := T.rowStore.Get(rowKey)
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
		err := T.AddColumn(k)
		if err != nil {
			return nil, err
		}
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

	rowKey, err := storage.ParseKey(row.Id())
	if err != nil {
		return nil, err
	}

	err = T.rowStore.Set(rowKey, rowData)
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
		err := T.AddColumn(k)
		if err != nil {
			return nil, err
		}
	}

	rowData, err := row.Serialize()
	if err != nil {
		return nil, err
	}

	rowKey, err := storage.ParseKey(row.Id())
	if err != nil {
		return nil, err
	}

	err = T.rowStore.Set(rowKey, rowData)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (T *Table) Delete(rowId string) error {
	rowKey, err := storage.ParseKey(rowId)
	if err != nil {
		return err
	}

	rowExists, err := T.rowStore.Has(rowKey)
	if err != nil {
		return err
	}

	if !rowExists {
		return fmt.Errorf("Row with id `%v` not found", rowId)
	}

	err = T.rowStore.Delete(rowKey)
	if err != nil {
		return err
	}

	return nil
}

func (T *Table) Columns() ([]string, error) {
	colData, err := T.metaDataStore.Get(metadataKeys.columnList)
	if err != nil {
		return nil, err
	}

	columns := strings.Split(string(colData), ",")
	if colData == nil {
		// Always make sure that id exists
		columns = []string{"id"}
	}

	return columns, nil
}

func (T *Table) AddColumn(colName string) error {
	cols, err := T.Columns()
	if err != nil {
		return err
	}

	// Todo: Replace with set
	for _, col := range cols {
		if col == colName {
			// Column already exists to skip insertion
			return nil
		}
	}

	cols = append(cols, colName)

	err = T.metaDataStore.Set(metadataKeys.columnList, []byte(strings.Join(cols, ",")))
	if err != nil {
		return err
	}

	return nil
}

func (T *Table) GetLastUsedId() (string, error) {
	idData, err := T.metaDataStore.Get(metadataKeys.lastUsedId)
	if err != nil {
		return "", err
	}

	if idData == nil {
		return "", nil
	}

	return string(idData), nil
}

func (T *Table) NextId() (string, error) {
	lastUsedIdStr, err := T.GetLastUsedId()
	if err != nil {
		return "", err
	}

	var newId int
	if lastUsedIdStr == "" {
		newId = 0
	} else {
		lastUsedId, err := strconv.Atoi(lastUsedIdStr)
		if err != nil {
			return "", err
		}

		newId = lastUsedId + 1
	}

	newIdStr := strconv.Itoa(newId)
	err = T.metaDataStore.Set(metadataKeys.lastUsedId, []byte(newIdStr))
	if err != nil {
		return "", nil
	}

	return newIdStr, nil
}
