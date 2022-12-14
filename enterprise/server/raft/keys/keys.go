package keys

import (
	"bytes"
	"math"
)

type Key []byte

var (
	MinByte Key = []byte{0}
	MaxByte Key = []byte{math.MaxUint8}
)

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

// Range returns a pair of keys that represent the upper and lower bounds of a
// range identified by the given key prefix.
func Range(key []byte) ([]byte, []byte) {
	return MakeKey(key, MinByte), MakeKey(key, MaxByte)
}
