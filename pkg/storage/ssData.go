package storage

import (
	"os"
)

type ssData struct {
	dataFile *os.File
}

func newSSData(dataFilePath string) (*ssData, error) {
	err := os.MkdirAll(dataFilePath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	dataFile, err := os.Open(dataFilePath)
	if err != nil {
		return nil, err
	}

	ssData := &ssData{
		dataFile: dataFile,
	}
	return ssData, nil
}

func (ss *ssData) ReadAt(filePos int64) (*LogEntry, error) {
	_, err := ss.dataFile.Seek(filePos, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	entry := &LogEntry{}
	err = entry.Deserialize(ss.dataFile)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (ss *ssData) write(entry *LogEntry) error {
	err := entry.Serialize(ss.dataFile)
	if err != nil {
		return err
	}

	return nil
}
