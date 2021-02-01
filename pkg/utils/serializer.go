package utils

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"io"
)

var byteOrder = binary.LittleEndian

// WriteEntry writes an `entry` in the format payloadSize:payload to `writer`
func WriteEntry(writer io.Writer, entry interface{}) error {
	// Create payload from entry
	buf := new(bytes.Buffer)
	err := gob.NewEncoder(io.Writer(buf)).Encode(entry)
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

// ReadEntry reads an `entry` in the format payloadSize:payload from `reader`
func ReadEntry(reader io.Reader, entry interface{}) error {
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

	// Create entry from payLoad
	buf := bytes.NewBuffer(payload)
	err = gob.NewDecoder(io.Reader(buf)).Decode(entry)
	if err != nil {
		return err
	}

	return nil
}
