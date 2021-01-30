package storage

import (
	"encoding/binary"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
)

type CommitLog struct {
	logFile *os.File
}

type LogEntry struct {
	Key       []byte
	Val       []byte
	IsDeleted bool
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

func (commitLog *CommitLog) Write(entry LogEntry) error {
	payload, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	payloadLen := int32(len(payload))
	err = binary.Write(commitLog.logFile, binary.LittleEndian, payloadLen)
	if err != nil {
		return err
	}

	_, err = commitLog.logFile.Write(payload)
	if err != nil {
		return err
	}

	return nil
}

func (commitLog *CommitLog) Read() (*LogEntry, error) {
	var payloadLen int32

	err := binary.Read(commitLog.logFile, binary.LittleEndian, &payloadLen)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	payload := make([]byte, payloadLen)
	_, err = commitLog.logFile.Read(payload)
	if err != nil {
		return nil, err
	}

	var entry LogEntry
	err = json.Unmarshal(payload, &entry)
	if err != nil {
		return nil, err
	}

	return &entry, nil
}
