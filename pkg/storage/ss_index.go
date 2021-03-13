package storage

import (
	"os"
	"path/filepath"

	"github.com/oneshadab/hariken/pkg/utils"
)

type ssIndex struct {
	indexFile *os.File
}

type IndexFileEntry struct {
	Key         StoreKey
	DataFilePos int64
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
		entry, err := ss.readAt(i * sizeofIndexEntry())

		if err != nil {
			return nil, err
		}

		if entry.Key == key {
			return entry, nil
		}
	}

	return nil, nil
}

func (ss *ssIndex) readAt(filePos int64) (*IndexFileEntry, error) {
	_, err := ss.indexFile.Seek(filePos, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	entry := &IndexFileEntry{}
	err = utils.ReadEntry(ss.indexFile, entry)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (ss ssIndex) write(key StoreKey, dataFilePos int64) error {
	indexEntry := &IndexFileEntry{
		Key:         key,
		DataFilePos: dataFilePos,
	}

	err := utils.WriteFixedWidthEntry(ss.indexFile, indexEntry, int32(sizeofIndexEntry()))
	if err != nil {
		return err
	}

	return nil
}

func sizeofIndexEntry() int64 {
	return 200
}
