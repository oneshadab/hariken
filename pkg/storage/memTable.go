package storage

import (
	"fmt"
)

type MemTable struct {
	entries map[StoreKey]*LogEntry
}

// A new memtable is created from a commit log
func NewMemTable() (*MemTable, error) {
	table := MemTable{
		entries: make(map[StoreKey]*LogEntry),
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

	return table.entries[key].Data, nil
}

func (table *MemTable) Set(key StoreKey, val []byte) error {
	table.entries[key] = &LogEntry{
		Key:       key,
		Data:      val,
		IsDeleted: false,
	}
	return nil
}

func (table *MemTable) Has(key StoreKey) (bool, error) {
	entry, keyExists := table.entries[key]
	if keyExists {
		return !entry.IsDeleted, nil
	}

	return false, nil
}

func (table *MemTable) Delete(key StoreKey) error {
	hasKey, err := table.Has(key)
	if err != nil {
		return err
	}

	if !hasKey {
		return fmt.Errorf("key `%s` not found", key)
	}

	table.entries[key].IsDeleted = true

	return nil
}

func (table *MemTable) Keys() ([]StoreKey, error) {
	keys := make([]StoreKey, 0, len(table.entries))
	for k, _ := range table.entries {
		keys = append(keys, k)
	}
	return keys, nil
}
