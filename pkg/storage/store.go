package storage

import (
	"path"
)

// A Generic Persistent Key-Value store
type Store struct {
	memTable     *MemTable
	commitLog    *CommitLog
	sstableGroup *SSTableGroup
}

func NewStore(dir string) (*Store, error) {
	var err error
	store := &Store{}

	store.memTable, err = NewMemTable()
	if err != nil {
		return nil, err
	}

	store.commitLog, err = NewCommitLog(path.Join(dir, "commitLog"))
	if err != nil {
		return nil, err
	}

	store.sstableGroup, err = NewSSTableGroup(path.Join(dir, "ssTables"))
	if err != nil {
		return nil, err
	}

	err = store.syncMemtableWithLog()
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (S *Store) Has(key StoreKey) (bool, error) {
	entry, err := S.find(key)
	if err != nil {
		return false, err
	}

	if entry != nil {
		return !entry.IsDeleted, nil
	}

	return false, nil
}

func (S *Store) Get(key StoreKey) ([]byte, error) {
	entry, err := S.find(key)
	if err != nil {
		return nil, err
	}

	if entry == nil || entry.IsDeleted {
		return nil, nil
	}

	return entry.Data, nil
}

func (S *Store) Set(key StoreKey, val []byte) error {
	entry := &LogEntry{
		Key:       key,
		Data:      val,
		IsDeleted: false,
	}

	err := S.commitLog.Write(entry)

	if err != nil {
		return err
	}

	err = S.memTable.Set(key, entry)
	if err != nil {
		return err
	}

	return nil
}

func (S *Store) Delete(key StoreKey) error {
	entry := &LogEntry{
		Key:       key,
		IsDeleted: true,
	}

	err := S.commitLog.Write(entry)
	if err != nil {
		return err
	}

	err = S.memTable.Set(key, entry)
	if err != nil {
		return err
	}

	return nil
}

func (S *Store) Flush() error {
	// Add new SSTable from commitlog/memtable
	err := S.sstableGroup.addNew(S.memTable)
	if err != nil {
		return err
	}

	// Clear the commit log
	err = S.commitLog.Reset()
	if err != nil {
		return err
	}

	// Sync memtable with commit log
	err = S.syncMemtableWithLog()
	if err != nil {
		return err
	}

	return nil
}

func (S *Store) syncMemtableWithLog() error {
	err := S.commitLog.SeekToStart()
	if err != nil {
		return err
	}

	err = S.memTable.Reset()
	if err != nil {
		return err
	}

	for {
		// Read entry from commit log
		entry, err := S.commitLog.Read()
		if err != nil {
			return err
		}

		// nil entry signifies that there's no more data to read
		if entry == nil {
			break
		}

		S.memTable.Set(entry.Key, entry)
	}

	return nil
}

func (S *Store) find(key StoreKey) (*LogEntry, error) {
	// First look for the key in the memTable
	found, err := S.memTable.hasKey(key)
	if err != nil {
		return nil, err
	}

	if found {
		return S.memTable.Get(key)
	}

	// Next look for the key in the ssTables
	return S.sstableGroup.find(key)
}
