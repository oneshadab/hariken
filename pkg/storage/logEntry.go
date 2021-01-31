package storage

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
)

var byteOrder = binary.LittleEndian

type LogEntry struct {
	Key       StoreKey
	Data      []byte
	IsDeleted bool
}

func (e *LogEntry) Serialize(writer io.Writer) error {
	// Create payload from logEntry
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(io.Writer(buf)).Encode(e)
	if err != nil {
		return err
	}
	payload := buf.Bytes()

	// First write the length of the payload
	err = binary.Write(writer, byteOrder, int32(len(payload)))
	if err != nil {
		return err
	}

	// Then write payload
	_, err = writer.Write(payload)
	if err != nil {
		return err
	}

	return nil
}

func (e *LogEntry) Deserialize(reader io.Reader) error {
	// First read the length of the payload
	var numBytes int32
	err := binary.Read(reader, byteOrder, &numBytes)
	if err != nil {
		return err
	}

	// Then read the payload
	payload := make([]byte, numBytes)
	_, err = reader.Read(payload)
	if err != nil {
		return err
	}

	// Create logEntry from payLoad
	buf := bytes.NewBuffer(payload)
	err = gob.NewDecoder(io.Reader(buf)).Decode(e)
	if err != nil {
		return err
	}

	return nil
}
