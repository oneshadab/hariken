package storage

import (
	"io"
	"os"
	"path/filepath"

	"github.com/oneshadab/hariken/pkg/utils"
)

type CommitLog struct {
	logFile *os.File
}

func NewCommitLog(path string) (*CommitLog, error) {
	var err error

	commitLog := CommitLog{}

	err = os.MkdirAll(filepath.Dir(path), os.ModePerm)
	if err != nil {
		return nil, err
	}

	commitLog.logFile, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	return &commitLog, nil
}

func (cl *CommitLog) Write(entry *LogEntry) error {
	err := utils.WriteEntry(cl.logFile, entry)

	if err != nil {
		return err
	}

	return nil
}

func (cl *CommitLog) Read() (*LogEntry, error) {
	var entry LogEntry

	err := utils.ReadEntry(cl.logFile, &entry)

	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	return &entry, nil
}

func (cl *CommitLog) Reset() error {
	err := cl.SeekToStart()
	if err != nil {
		return err
	}

	err = cl.logFile.Truncate(0)
	if err != nil {
		return err
	}

	return nil
}

func (cl *CommitLog) SeekToStart() error {
	_, err := cl.logFile.Seek(0, os.SEEK_SET)
	if err != nil {
		return err
	}

	return nil
}
