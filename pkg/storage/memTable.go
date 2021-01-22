package storage

import (
	"fmt"
)

type MemTable struct {
	entries map[string]string
}

// A new memtable is created from a commit log
func NewMemTable() (*MemTable, error) {
	table := MemTable{
		entries: make(map[string]string),
	}

	return &table, nil
}

func (table *MemTable) Get(key string) (*string, error) {
	hasKey, err := table.Has(key)

	if err != nil {
		return nil, err
	}

	if !hasKey {
		return nil, nil
	}

	val := table.entries[key]
	return &val, nil
}

func (table *MemTable) Set(key string, val string) error {
	table.entries[key] = val
	return nil
}

func (table *MemTable) Has(key string) (bool, error) {
	_, ok := table.entries[key]
	return ok, nil
}

func (table *MemTable) Delete(key string) error {
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
