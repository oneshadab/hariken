package storage

type Store interface {
	Get(key string) (*string, error)
	Set(key string, val string) error
	Has(key string) (bool, error)
	Delete(key string) error
}

func NewStore() (*MemTable, error) {
	tempFile := "temp.db"
	return NewMemTable(tempFile)
}
