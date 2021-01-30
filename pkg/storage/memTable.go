package storage

import (
	"fmt"

	"github.com/oneshadab/hariken/pkg/utils"
)

type MemTable struct {
	entries map[string][]byte
}

// A new memtable is created from a commit log
func NewMemTable() (*MemTable, error) {
	table := MemTable{
		entries: make(map[string][]byte),
	}

	return &table, nil
}

func (table *MemTable) Get(key []byte) ([]byte, error) {
	hasKey, err := table.Has(key)

	if err != nil {
		return nil, err
	}

	if !hasKey {
		return nil, nil
	}

	val := table.entries[utils.KeyToStr(key)]
	return val, nil
}

func (table *MemTable) Set(key []byte, val []byte) error {
	table.entries[utils.KeyToStr(key)] = val
	return nil
}

func (table *MemTable) Has(key []byte) (bool, error) {
	_, ok := table.entries[utils.KeyToStr(key)]
	return ok, nil
}

func (table *MemTable) Delete(key []byte) error {
	hasKey, err := table.Has(key)
	if err != nil {
		return err
	}

	if !hasKey {
		return fmt.Errorf("key `%s` not found", key)
	}

	delete(table.entries, utils.KeyToStr(key))

	return nil
}

func (table *MemTable) Keys() ([][]byte, error) {
	keys := make([][]byte, 0, len(table.entries))
	for k, _ := range table.entries {
		keys = append(keys, []byte(k))
	}
	return keys, nil
}
