package protocol

import (
	"bufio"
	"encoding/binary"
)

func WriteMessage(writer *bufio.Writer, msg string) error {
	msgLen := int32(len(msg))

	err := binary.Write(writer, binary.LittleEndian, msgLen)
	if err != nil {
		return err
	}

	_, err = writer.WriteString(msg)
	if err != nil {
		return err
	}

	err = writer.Flush()
	if err != nil {
		return err
	}

	return nil
}

func ReadMessage(reader *bufio.Reader) (string, error) {
	var msgLen int32

	err := binary.Read(reader, binary.LittleEndian, &msgLen)
	if err != nil {
		return "", err
	}

	buf := make([]byte, msgLen)
	_, err = reader.Read(buf)
	if err != nil {
		return "", err
	}

	msg := string(buf)
	return msg, err
}
