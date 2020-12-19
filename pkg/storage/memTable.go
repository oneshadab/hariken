package storage

import (
	"fmt"
)

type MemTable struct {
	entries   map[string]string
	commitLog *CommitLog
}

func NewMemTable(filePath string) (*MemTable, error) {
	var err error

	store := MemTable{}

	store.commitLog, err = NewCommitLog(filePath)
	if err != nil {
		return nil, err
	}

	err = store.loadFromLog()
	if err != nil {
		return nil, err
	}

	return &store, nil
}

func (store *MemTable) loadFromLog() error {
	store.entries = make(map[string]string)

	for {
		entry, err := store.commitLog.Read()
		if err != nil {
			return err
		}

		if entry == nil {
			break
		}

		if entry.IsDeleted {
			delete(store.entries, entry.Key)
		} else {
			store.entries[entry.Key] = entry.Val
		}
	}

	return nil
}

func (store *MemTable) Get(key string) (*string, error) {
	hasKey, err := store.Has(key)

	if err != nil {
		return nil, err
	}

	if !hasKey {
		return nil, nil
	}

	val := store.entries[key]
	return &val, nil
}

func (store *MemTable) Set(key string, val string) error {
	err := store.commitLog.Write(LogEntry{
		Key: key,
		Val: val,
	})
	if err != nil {
		return err
	}

	store.entries[key] = val
	return nil
}

func (store *MemTable) Has(key string) (bool, error) {
	_, ok := store.entries[key]
	return ok, nil
}

func (store *MemTable) Delete(key string) error {
	err := store.commitLog.Write(LogEntry{
		Key:       key,
		IsDeleted: true,
	})

	hasKey, err := store.Has(key)
	if err != nil {
		return err
	}

	if !hasKey {
		return fmt.Errorf("key `%s` not found", key)
	}

	delete(store.entries, key)

	return nil
}
