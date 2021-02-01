package storage

type LogEntry struct {
	Key       StoreKey
	Data      []byte
	IsDeleted bool
}
