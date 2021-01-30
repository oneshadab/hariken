package database

import (
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/oneshadab/hariken/pkg/storage"
)

const (
	keySize = 4 // Make keySize configurable
)

// Todo: Find something better than this
var keyConstants = struct {
	lastUsedId []byte
	columnList []byte
}{
	lastUsedId: []byte{0},
	columnList: []byte{1},
}

type Table struct {
	metaDataStore *storage.Store
	rowStore      *storage.Store
}

func LoadTable(tableDir string) (*Table, error) {
	var err error

	table := &Table{}

	table.metaDataStore, err = storage.NewStore(filepath.Join(tableDir, "metadata"), keySize)
	if err != nil {
		return nil, err
	}

	table.rowStore, err = storage.NewStore(filepath.Join(tableDir, "data"), keySize)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (T *Table) Get(rowId string) (*Row, error) {
	rowData, err := T.rowStore.Get([]byte(rowId))
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

	err = T.rowStore.Set([]byte(row.Id()), rowData)
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

	err = T.rowStore.Set([]byte(row.Id()), rowData)
	if err != nil {
		return nil, err
	}

	return row, nil
}

func (T *Table) Delete(rowId string) error {
	rowExists, err := T.rowStore.Has([]byte(rowId))
	if err != nil {
		return err
	}

	if !rowExists {
		return fmt.Errorf("Row with id `%v` not found", rowId)
	}

	err = T.rowStore.Delete([]byte(rowId))
	if err != nil {
		return err
	}

	return nil
}

func (T *Table) Columns() ([]string, error) {
	colData, err := T.metaDataStore.Get(keyConstants.columnList)
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

	err = T.metaDataStore.Set(keyConstants.columnList, []byte(strings.Join(cols, ",")))
	if err != nil {
		return err
	}

	return nil
}

func (T *Table) NextId() (string, error) {
	idData, err := T.metaDataStore.Get(keyConstants.lastUsedId)
	if err != nil {
		return "", err
	}

	idStr := string(idData)
	if idData == nil {
		idStr = "0"
	}

	lastUsedId, err := strconv.Atoi(idStr)
	if err != nil {
		return "", err
	}
	lastUsedId++

	newIdStr := strconv.Itoa(lastUsedId)
	err = T.metaDataStore.Set(keyConstants.lastUsedId, []byte(newIdStr))
	if err != nil {
		return "", nil
	}

	return idStr, nil
}
