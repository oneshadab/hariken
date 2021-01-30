package storage

// A Generic Persistent Key-Value store
type Store struct {
	memTable  *MemTable
	commitLog *CommitLog
}

func NewStore(filepath string) (*Store, error) {
	var err error
	store := &Store{}

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
	return S.memTable.Has(key)
}

func (S *Store) Get(key []byte) ([]byte, error) {
	return S.memTable.Get(key)
}

func (S *Store) Set(key []byte, val []byte) error {
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
