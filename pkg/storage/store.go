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

	SSTables [](*SSTable)
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
	return S.memTable.Has(key)
}

func (S *Store) Get(key StoreKey) ([]byte, error) {
	return S.memTable.Get(key)
}

func (S *Store) Set(key StoreKey, val []byte) error {
	err := S.commitLog.Write(LogEntry{
		Key: key,
		Val: val,
	})

	if err != nil {
		return err
	}

	err = S.memTable.Set(key, val)
	if err != nil {
		return err
	}

	return nil
}

func (S *Store) Delete(key StoreKey) error {
	err := S.commitLog.Write(LogEntry{
		Key:       key,
		IsDeleted: true,
	})

	if err != nil {
		return err
	}

	err = S.memTable.Delete(key)
	if err != nil {
		return err
	}

	return nil
}

func (S *Store) syncMemtableWithLog() error {
	err := S.commitLog.Reset()
	if err != nil {
		return err
	}

	for {
		// Read entry from commit log
		entry, err := S.commitLog.Read()

		if err != nil {
			return err
		}

		if entry == nil {
			break
		}

		// Write to memTable
		if entry.IsDeleted {
			err = S.memTable.Delete(entry.Key)
			if err != nil {
				return err
			}
		} else {
			err = S.memTable.Set(entry.Key, entry.Val)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (S *Store) Flush() error {
	// Create new SSTable from commit log
	err := S.genNewSSTable()
	if err != nil {
		return err
	}

	// Clear the commit log
	err = S.commitLog.Flush()
	if err != nil {
		return err
	}

	// Load memtable from commit log
	err = S.syncMemtableWithLog()
	if err != nil {
		return err
	}

	return nil
}

func (S *Store) genNewSSTable() error {
	tableId := strconv.Itoa(len(S.SSTables))
	ssTableDir := path.Join(S.dir, "ssTables", tableId)

	table, err := NewSSTable(ssTableDir)
	if err != nil {
		return err
	}

	err = table.Build(S.memTable)
	if err != nil {
		return err
	}

	S.SSTables = append(S.SSTables, table)

	return nil
}
