package storage

type Store interface {
	Get(key string) (*string, error)
	Set(key string, val string) error
	Has(key string) (bool, error)
	Delete(key string) error
}

// Initializes a Store with the db stored at `filepath`
func NewStore(filepath string) (*MemTable, error) {
	commitLog, err := NewCommitLog(filepath)
	if err != nil {
		return nil, err
	}

	table, err := NewMemTable(commitLog)
	if err != nil {
		return nil, err
	}

	return table, nil
}
