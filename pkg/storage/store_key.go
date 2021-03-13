package storage

import (
	"encoding/binary"
	"strconv"
)

const (
	storeKeyLen = 8
)

type StoreKey [storeKeyLen]byte

func (key StoreKey) isLess(otherKey StoreKey) bool {
	for i := 0; i < storeKeyLen; i++ {
		if key[i] < otherKey[i] {
			return true
		}
	}
	return false
}

func ParseKey(s string) (StoreKey, error) {
	var key StoreKey

	n, err := strconv.Atoi(s)
	if err != nil {
		return key, err
	}

	binary.BigEndian.PutUint64(key[:], uint64(n))
	return key, nil
}

func (key StoreKey) bytes() []byte {
	return []byte(key[:])
}
