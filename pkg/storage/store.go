package storage

import "fmt"

// A Generic Persistent Key-Value store
type Store struct {
	keySize   int
	memTable  *MemTable
	commitLog *CommitLog
}

func NewStore(filepath string, keySize int) (*Store, error) {
	var err error
	store := &Store{
		keySize: keySize,
	}

	store.commitLog, err = NewCommitLog(filepath)
	if err != nil {
		return nil, err
	}

	store.memTable, err = NewMemTable()
	if err != nil {
		return nil, err
	}

	err = store.loadFromLog()
	if err != nil {
		return nil, err
	}

	return store, nil
}

func (S *Store) Has(key []byte) (bool, error) {
	if len(key) != S.keySize {
		return false, fmt.Errorf("Expected key of size %d got %d", S.keySize, len(key))
	}

	return S.memTable.Has(key)
}

func (S *Store) Get(key []byte) ([]byte, error) {
	if len(key) != S.keySize {
		return nil, fmt.Errorf("Expected key of size %d got %d", S.keySize, len(key))
	}

	return S.memTable.Get(key)
}

func (S *Store) Set(key []byte, val []byte) error {
	if len(key) != S.keySize {
		return fmt.Errorf("Expected key of size %d got %d", S.keySize, len(key))
	}

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

func (S *Store) Delete(key []byte) error {
	if len(key) != S.keySize {
		return fmt.Errorf("Expected key of size %d got %d", S.keySize, len(key))
	}

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

func (S *Store) loadFromLog() error {
	for {
		entry, err := S.commitLog.Read()

		if err != nil {
			return err
		}

		if entry == nil {
			break
		}

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
