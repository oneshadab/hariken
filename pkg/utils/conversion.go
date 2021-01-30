package utils

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/oneshadab/hariken/pkg/storage"
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

func ParseKey(s string) (storage.StoreKey, error) {
	var key storage.StoreKey

	n, err := strconv.Atoi(s)
	if err != nil {
		return key, err
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(n))
	return key, nil
}

func KeyToStr(key []byte) string {
	return string(key)
}
