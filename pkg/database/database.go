package database

import "path/filepath"

type Database struct {
	tableCache      map[string](*Table)
	storageLocation string
}

// Initializes a Database with the db databased at `filepath`
func LoadDatabase(dbDir string) (*Database, error) {
	db := Database{
		storageLocation: dbDir,
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
	table, err := LoadTable(tableDir)
	if err != nil {
		return nil, err
	}

	db.tableCache[tableName] = table

	return table, nil
}

func (db *Database) Query(tableName string) *Query {
	return NewQuery(db, tableName)
}
