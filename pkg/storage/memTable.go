package storage

type MemTable struct {
	entries map[StoreKey]*LogEntry
}

// A new memtable is created from a commit log
func NewMemTable() (*MemTable, error) {
	table := MemTable{
		entries: make(map[StoreKey]*LogEntry),
	}

	return &table, nil
}

func (table *MemTable) Get(key StoreKey) (*LogEntry, error) {
	return table.entries[key], nil
}

func (table *MemTable) Set(key StoreKey, entry *LogEntry) error {
	table.entries[key] = entry
	return nil
}

func (table *MemTable) hasKey(key StoreKey) (bool, error) {
	_, ok := table.entries[key]
	return ok, nil
}

func (table *MemTable) Keys() ([]StoreKey, error) {
	keys := make([]StoreKey, 0, len(table.entries))
	for k := range table.entries {
		keys = append(keys, k)
	}
	return keys, nil
}

func (table *MemTable) Clear() error {
	table.entries = make(map[StoreKey]*LogEntry)
	return nil
}
