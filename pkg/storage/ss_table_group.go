package storage

import (
	"io/ioutil"
	"os"
	"path"
	"strconv"
)

type SSTableGroup struct {
	dirPath string
	tables  []*SSTable // List of sstables, sorted by most recent
}

func NewSSTableGroup(dirPath string) (*SSTableGroup, error) {
	tableGroup := &SSTableGroup{
		dirPath: dirPath,
	}

	err := tableGroup.loadSSTables()
	if err != nil {
		return nil, err
	}

	return tableGroup, nil
}

func (g *SSTableGroup) find(key StoreKey) (*LogEntry, error) {
	// Then find the most recent sstable with the key
	for i := len(g.tables) - 1; i >= 0; i-- {
		table := g.tables[i]

		found, err := table.hasKey(key)
		if err != nil {
			return nil, err
		}

		if found {
			return table.Get(key)
		}
	}

	return nil, nil
}

func (g *SSTableGroup) addNew(mt *MemTable) error {
	table, err := g.genNewSSTable(mt)
	if err != nil {
		return err
	}

	g.tables = append(g.tables, table)
	return nil
}

func (g *SSTableGroup) genNewSSTable(mt *MemTable) (*SSTable, error) {
	tableId := strconv.Itoa(len(g.tables))

	table, err := NewSSTable(path.Join(g.dirPath, tableId))
	if err != nil {
		return nil, err
	}

	err = table.Build(mt)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (g *SSTableGroup) loadSSTables() error {
	tableFiles, err := ioutil.ReadDir(g.dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Directory empty so nothing to load
			return nil
		}
		return err
	}

	g.tables = make([]*SSTable, 0)

	for _, tableFile := range tableFiles {
		table, err := NewSSTable(path.Join(g.dirPath, tableFile.Name()))
		if err != nil {
			return err
		}

		g.tables = append(g.tables, table)
	}

	return nil
}
