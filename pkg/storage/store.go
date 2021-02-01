package storage

import (
	"path"
	"strconv"
)

// A Generic Persistent Key-Value store
type Store struct {
	dir string // Todo: use something better

	memTable  *MemTable
	commitLog *CommitLog

	SSTables []*SSTable // List of sstables, youngest to oldest
}

func NewStore(dir string) (*Store, error) {
	var err error
	store := &Store{
		dir: dir,
	}

	commitLogPath := path.Join(dir, "commitLog")
	store.commitLog, err = NewCommitLog(commitLogPath)
	if err != nil {
		return nil, err
	}

	store.memTable, err = NewMemTable()
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
	// Create new SSTable from commit log
	sstable, err := S.genNewSSTable()
	if err != nil {
		return err
	}

	// Clear the commit log
	err = S.commitLog.Reset()
	if err != nil {
		return err
	}

	// Load memtable from commit log
	err = S.syncMemtableWithLog()
	if err != nil {
		return err
	}

	// Add sstable to list of sstables
	S.SSTables = append([]*SSTable{sstable}, S.SSTables...)

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
	// First look in memTable
	found, err := S.memTable.hasKey(key)
	if err != nil {
		return nil, err
	}

	if found {
		return S.memTable.Get(key)
	}

	// Then look for youngest sstable which has key
	for _, table := range S.SSTables {
		found, err := table.hasKey(key)
		if err != nil {
			return nil, err
		}

		if found {
			return table.Get(key)
		}
	}

	return nil, nil
}

func (S *Store) genNewSSTable() (*SSTable, error) {
	tableId := strconv.Itoa(len(S.SSTables))
	ssTableDir := path.Join(S.dir, "ssTables", tableId)

	table, err := NewSSTable(ssTableDir)
	if err != nil {
		return nil, err
	}

	err = table.Build(S.memTable)
	if err != nil {
		return nil, err
	}

	return table, nil
}
