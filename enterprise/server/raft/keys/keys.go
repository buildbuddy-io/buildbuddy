package keys

import (
	"bytes"
	"math"
)

const (
	MinByte = 0
	MaxByte = math.MaxUint8
)

type Key []byte

func MakeKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}

func (k Key) Next() Key {
	nk := make([]byte, len(k)+1)
	copy(nk, k)
	nk[len(nk)-1] = 0
	return nk
}

func RangeMetaKey(key Key) Key {
	return MakeKey([]byte{'\x02'}, key)
}

func SystemKey(key Key) Key {
	return MakeKey([]byte{'\x03'}, key)
}

func IsLocalKey(key Key) bool {
	if len(key) == 0 {
		return false
	}
	return key[0] == '\x01'
}

func Range(key []byte) ([]byte, []byte) {
	return MakeKey(key, []byte{MinByte}), MakeKey(key, []byte{MaxByte})
}
