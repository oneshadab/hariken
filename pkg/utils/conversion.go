package utils

import (
	"encoding/binary"
	"fmt"
)

func NumToKey(num int, keySize int) ([]byte, error) {
	buf := make([]byte, binary.MaxVarintLen64)

	n := binary.PutVarint(buf, int64(num))
	if n > keySize {
		return nil, fmt.Errorf("number does not fit in key")
	}

	return buf[:keySize], nil
}

func KeyToNum(key []byte) (int, error) {
	num, n := binary.Varint(key)
	if n > binary.MaxVarintLen32 {
		return 0, fmt.Errorf("key does not fit in number")
	}

	return int(num), nil
}

func StrToKey(s string, keySize int) ([]byte, error) {
	if len(s) > keySize {
		return nil, fmt.Errorf("string does not fit in key")
	}
	return []byte(s), nil
}

func KeyToStr(key []byte) string {
	return string(key)
}
