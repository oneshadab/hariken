package storage

import (
	"encoding/json"
	"io"
	"os"
)

type CommitLog struct {
	logFile *os.File
}

type LogEntry struct {
	Key       string
	Val       string
	IsDeleted bool
}

func NewCommitLog(filePath string) (*CommitLog, error) {
	var err error

	commitLog := CommitLog{}

	commitLog.logFile, err = os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0644)
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
	_, err = commitLog.logFile.Write(IntToByteArray(len(payload)))
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
	payloadLengthPayload := make([]byte, 4)
	_, err := commitLog.logFile.Read(payloadLengthPayload)
	if err != nil {
		if err == io.EOF {
			return nil, nil
		}
		return nil, err
	}

	payloadLength := ByteArrayToInt(payloadLengthPayload)
	payload := make([]byte, payloadLength)

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

// Copied for testing
func IntToByteArray(num int) []byte {
	size := 4
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		arr[i] = byte(num & 255)
		num >>= 8
	}
	return arr
}

func ByteArrayToInt(arr []byte) int {
	num := 0
	size := 4
	for i := 0; i < size; i++ {
		num <<= 8
		num |= int(arr[i])
	}
	return num
}
