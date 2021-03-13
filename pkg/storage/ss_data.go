package storage

import (
	"os"
	"path/filepath"

	"github.com/oneshadab/hariken/pkg/utils"
)

type ssData struct {
	dataFile *os.File
}

func newSSData(dataFilePath string) (*ssData, error) {
	err := os.MkdirAll(filepath.Dir(dataFilePath), os.ModePerm)
	if err != nil {
		return nil, err
	}

	dataFile, err := os.OpenFile(dataFilePath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	ssData := &ssData{
		dataFile: dataFile,
	}
	return ssData, nil
}

func (ss *ssData) readAt(filePos int64) (*LogEntry, error) {
	_, err := ss.dataFile.Seek(filePos, os.SEEK_SET)
	if err != nil {
		return nil, err
	}

	entry := &LogEntry{}
	err = utils.ReadEntry(ss.dataFile, entry)
	if err != nil {
		return nil, err
	}

	return entry, nil
}

func (ss *ssData) write(entry *LogEntry) error {
	err := utils.WriteEntry(ss.dataFile, entry)
	if err != nil {
		return err
	}

	return nil
}
