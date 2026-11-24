package random

import (
	"io"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"

	crand "crypto/rand"
	mrand "math/rand"
)

var once sync.Once

func init() {
	// Check that rand is working -- bail if not.
	buf := make([]byte, 1)
	if _, err := io.ReadFull(crand.Reader, buf); err != nil {
		log.Fatalf("crypto/rand is unavailable: %s", err)
	}
}

func RandUint64() uint64 {
	return mrand.Uint64()
}

func RandomString(stringLength int) (string, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	// Bytes at or above the largest multiple of len(letters) are discarded
	// so that every letter is equally likely.
	const limit = 256 - 256%len(letters)
	out := make([]byte, 0, stringLength)
	buf := make([]byte, stringLength)
	for len(out) < stringLength {
		buf = buf[:stringLength-len(out)]
		if _, err := crand.Read(buf); err != nil {
			return "", err
		}
		for _, b := range buf {
			if int(b) < limit {
				out = append(out, letters[int(b)%len(letters)])
			}
		}
	}
	return string(out), nil
}
