package storage

import (
	"encoding/binary"
	"os"
	"path"
	"sort"
)

type SSTable struct {
	indexFile *os.File
	dataFile  *os.File
}

type IndexEntry struct {
	key         StoreKey
	dataFilePos int64
}

func NewSSTable(dir string) (*SSTable, error) {
	table := &SSTable{}

	// Load index file
	indexFilePath := path.Join(dir, "index")
	err := os.MkdirAll(indexFilePath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	table.indexFile, err = os.Open(indexFilePath)
	if err != nil {
		return nil, err
	}

	// Load data file
	dataFilePath := path.Join(dir, "data")
	err = os.MkdirAll(dataFilePath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	table.dataFile, err = os.Open(dataFilePath)
	if err != nil {
		return nil, err
	}

	return table, nil
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
		dataFilePos, err := S.dataFile.Seek(0, os.SEEK_CUR)
		if err != nil {
			return err
		}

		indexEntry := IndexEntry{
			key:         key,
			dataFilePos: dataFilePos,
		}

		_, err = S.indexFile.Write(indexEntry.Bytes())
		if err != nil {
			return err
		}

		// Write corresponding datafile entry
		data, err := mt.Get(key)
		if err != nil {
			return err
		}

		_, err = S.dataFile.Write(data)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *IndexEntry) Bytes() []byte {
	var buf [16]byte

	// First 8 bytes are the key
	copy(buf[:], e.key[:])

	// Next 8 bytes are the Position
	binary.LittleEndian.PutUint64(buf[8:], uint64(e.dataFilePos))

	return buf[:]
}
