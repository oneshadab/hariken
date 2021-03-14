package database

import (
	"path/filepath"

	"github.com/google/uuid"
)

type Database struct {
	tableCache      map[string](*Table)
	storageLocation string

	lockManager *lockManager
}

// Initializes a Database with the db databased at `filepath`
func LoadDatabase(dbDir string) (*Database, error) {
	db := Database{
		storageLocation: dbDir,
		tableCache:      make(map[string](*Table)),
		lockManager:     newLockManager(),
	}
	return &db, nil
}

func (db *Database) Table(tableName string) (*Table, error) {
	table, ok := db.tableCache[tableName]
	if ok {
		// Already loaded table so return from cache
		return table, nil
	}

	tableDir := filepath.Join(db.storageLocation, "tables", tableName)
	table, err := LoadTable(tableName, tableDir)
	if err != nil {
		return nil, err
	}

	db.tableCache[tableName] = table

	return table, nil
}

func (db *Database) NewTransaction() *Transaction {
	tx := &Transaction{
		Id:                    uuid.NewString(),
		db:                    db,
		ProcessedCommandTypes: make(map[string]bool),
	}
	return tx
}
