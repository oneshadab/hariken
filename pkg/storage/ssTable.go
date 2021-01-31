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

func (S *SSTable) Get(key StoreKey) ([]byte, error) {
	indexEntry, err := S.index.Get(key)
	if err != nil {
		return nil, err
	}

	if indexEntry == nil {
		// No Entry found in sstable
		return nil, nil
	}

	dataEntry, err := S.data.ReadAt(indexEntry.dataFilePos)
	if err != nil {
		return nil, err
	}

	return dataEntry.data, nil
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
		data, err := mt.Get(key)
		if err != nil {
			return err
		}
		err = S.data.write(data)
		if err != nil {
			return err
		}
	}
	return nil
}
