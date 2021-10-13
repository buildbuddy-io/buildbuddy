package keys

import (
	"bytes"
)
	
type Key []byte

func MakeKey(keys ...[]byte) []byte {
	return bytes.Join(keys, nil)
}
