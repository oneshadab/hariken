package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
)

type ssIndex struct {
	indexFile *os.File
}

type IndexFileEntry struct {
	key         StoreKey
	dataFilePos int64
}

func newSSIndex(indexFilePath string) (*ssIndex, error) {
	err := os.MkdirAll(filepath.Dir(indexFilePath), os.ModePerm)
	if err != nil {
		return nil, err
	}

	indexFile, err := os.OpenFile(indexFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	ss := &ssIndex{
		indexFile: indexFile,
	}

	return ss, nil
}

func (ss *ssIndex) Get(key StoreKey) (*IndexFileEntry, error) {
	fileInfo, err := ss.indexFile.Stat()
	if err != nil {
		return nil, err
	}

	numEntries := fileInfo.Size() / sizeofIndexEntry()
	for i := int64(0); i < numEntries; i++ {
		entry, err := ss.ReadAt(i * sizeofIndexEntry())
		if err != nil {
			return nil, err
		}

		if entry.key == key {
			return entry, nil
		}
	}

	return nil, nil
}

func (ss *ssIndex) ReadAt(filePos int64) (*IndexFileEntry, error) {
	_, err := ss.indexFile.Seek(filePos, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	entry := &IndexFileEntry{}
	err = binary.Read(ss.indexFile, binary.LittleEndian, entry)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (ss ssIndex) write(key StoreKey, dataFilePos int64) error {
	indexEntry := IndexFileEntry{
		key:         key,
		dataFilePos: dataFilePos,
	}

	_, err := ss.indexFile.Write(indexEntry.Bytes())
	if err != nil {
		return err
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

func sizeofIndexEntry() int64 {
	return 16
}
