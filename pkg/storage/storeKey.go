package storage

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

func (key StoreKey) bytes() []byte {
	return []byte(key[:])
}
