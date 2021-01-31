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

type IndexFileEntry struct {
	key         StoreKey
	dataFilePos int64
}

type DataFileEntry struct {
	dataLen int64
	data    []byte
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

		indexEntry := IndexFileEntry{
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

		dataFileEntry := DataFileEntry{
			dataLen: int64(len(data)),
			data:    data,
		}

		_, err = S.dataFile.Write(dataFileEntry.Bytes())
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *IndexFileEntry) Bytes() []byte {
	var buf [16]byte

	// First 8 bytes are the key
	copy(buf[:], e.key[:])

	// Next 8 bytes are the Position
	binary.LittleEndian.PutUint64(buf[8:], uint64(e.dataFilePos))

	return buf[:]
}

func (e *DataFileEntry) Bytes() []byte {
	buf := make([]byte, 8)

	// First 8 bytes are the length
	binary.LittleEndian.PutUint64(buf, uint64(e.dataLen))

	// Next bytes are the data
	buf = append(buf, e.data...)

	return buf
}
