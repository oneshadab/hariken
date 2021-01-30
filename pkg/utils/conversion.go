package utils

import (
	"encoding/binary"
	"strconv"

	"github.com/oneshadab/hariken/pkg/storage"
)

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
