package storage

import (
	"fmt"
)

type MemTable struct {
	entries map[StoreKey][]byte
}

// A new memtable is created from a commit log
func NewMemTable() (*MemTable, error) {
	table := MemTable{
		entries: make(map[StoreKey][]byte),
	}

	return &table, nil
}

func (table *MemTable) Get(key StoreKey) ([]byte, error) {
	hasKey, err := table.Has(key)

	if err != nil {
		return nil, err
	}

	if !hasKey {
		return nil, nil
	}

	val := table.entries[key]
	return val, nil
}

func (table *MemTable) Set(key StoreKey, val []byte) error {
	table.entries[key] = val
	return nil
}

func (table *MemTable) Has(key StoreKey) (bool, error) {
	_, ok := table.entries[key]
	return ok, nil
}

func (table *MemTable) Delete(key StoreKey) error {
	hasKey, err := table.Has(key)
	if err != nil {
		return err
	}

	if !hasKey {
		return fmt.Errorf("key `%s` not found", key)
	}

	delete(table.entries, key)

	return nil
}

func (table *MemTable) Keys() ([]StoreKey, error) {
	keys := make([]StoreKey, 0, len(table.entries))
	for k, _ := range table.entries {
		keys = append(keys, k)
	}
	return keys, nil
}
