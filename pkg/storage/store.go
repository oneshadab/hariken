package storage

type Store interface {
	Get(key string) (*string, error)
	Set(key string, val string) error
	Has(key string) (bool, error)
	Delete(key string) error
}

func NewStore() (*MemTable, error) {
	tempFilePath := "temp/temp.db"

	commitLog, err := NewCommitLog(tempFilePath)
	if err != nil {
		return nil, err
	}

	table, err := NewMemTable(commitLog)
	if err != nil {
		return nil, err
	}

	return table, nil
}
