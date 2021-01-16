package storage

import (
	"fmt"
)

type MemTable struct {
	entries   map[string]string
	commitLog *CommitLog
}

// A new memtable is created from a commit log
func NewMemTable(commitLog *CommitLog) (*MemTable, error) {
	table := MemTable{
		entries:   make(map[string]string),
		commitLog: commitLog,
	}

	err := table.loadFromLog()
	if err != nil {
		return nil, err
	}

	return &table, nil
}

func (table *MemTable) loadFromLog() error {
	for {
		entry, err := table.commitLog.Read()
		if err != nil {
			return err
		}

		if entry == nil {
			break
		}

		if entry.IsDeleted {
			delete(table.entries, entry.Key)
		} else {
			table.entries[entry.Key] = entry.Val
		}
	}

	return nil
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
	err := table.commitLog.Write(LogEntry{
		Key: key,
		Val: val,
	})
	if err != nil {
		return err
	}

	table.entries[key] = val
	return nil
}

func (table *MemTable) Has(key string) (bool, error) {
	_, ok := table.entries[key]
	return ok, nil
}

func (table *MemTable) Delete(key string) error {
	err := table.commitLog.Write(LogEntry{
		Key:       key,
		IsDeleted: true,
	})

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
