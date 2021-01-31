package storage

import (
	"encoding/binary"
	"os"
)

type ssData struct {
	dataFile *os.File
}

type DataFileEntry struct {
	dataLen int64
	data    []byte
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

func (ss *ssData) write(data []byte) error {
	dataFileEntry := DataFileEntry{
		dataLen: int64(len(data)),
		data:    data,
	}

	_, err := ss.dataFile.Write(dataFileEntry.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (e *DataFileEntry) Bytes() []byte {
	buf := make([]byte, 8)

	// First 8 bytes are the length
	binary.LittleEndian.PutUint64(buf, uint64(e.dataLen))

	// Next bytes are the data
	buf = append(buf, e.data...)

	return buf
}
