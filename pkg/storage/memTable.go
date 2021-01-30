package storage

import (
	"fmt"
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

	val := table.entries[string(key)]
	return val, nil
}

func (table *MemTable) Set(key []byte, val []byte) error {
	table.entries[string(key)] = val
	return nil
}

func (table *MemTable) Has(key []byte) (bool, error) {
	_, ok := table.entries[string(key)]
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

	delete(table.entries, string(key))

	return nil
}
