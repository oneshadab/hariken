package storage

import (
	"os"
	"path"
	"sort"
)

type SSTable struct {
	index *ssIndex
	data  *ssData
}

func NewSSTable(dir string) (*SSTable, error) {
	var err error

	table := &SSTable{}

	// Load index file
	indexFilePath := path.Join(dir, "index")
	table.index, err = newSSIndex(indexFilePath)
	if err != nil {
		return nil, err
	}

	// Load data file
	dataFilePath := path.Join(dir, "data")
	table.data, err = newSSData(dataFilePath)
	if err != nil {
		return nil, err
	}

	return table, nil
}

func (S *SSTable) hasKey(key StoreKey) (bool, error) {
	indexEntry, err := S.index.Get(key)
	if err != nil {
		return false, err
	}

	if indexEntry == nil {
		// No Entry found in sstable
		return false, nil
	}

	return true, nil
}

func (S *SSTable) Get(key StoreKey) (*LogEntry, error) {
	indexEntry, err := S.index.Get(key)
	if err != nil {
		return nil, err
	}

	if indexEntry == nil {
		// No Entry found in sstable
		return nil, nil
	}

	logEntry, err := S.data.readAt(indexEntry.DataFilePos)
	if err != nil {
		return nil, err
	}

	return logEntry, nil
}

func (S *SSTable) Build(mt *MemTable) error {
	keys, err := mt.Keys()
	if err != nil {
		return err
	}

	// Sort the keys
	sort.Slice(keys, func(i, j int) bool {
		return keys[i].isLess(keys[j])
	})

	for _, key := range keys {
		// Write index entry
		dataFilePos, err := S.data.dataFile.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}
		err = S.index.write(key, dataFilePos)
		if err != nil {
			return err
		}

		// Write corresponding data entry
		entry, err := mt.Get(key)
		if err != nil {
			return err
		}
		err = S.data.write(entry)
		if err != nil {
			return err
		}
	}
	return nil
}
