package random

import (
	"io"
	"sync"
	"time"

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
	once.Do(func() {
		mrand.Seed(time.Now().UnixNano())
		log.Debugf("Seeded random with current time!")
	})
	return mrand.Uint64()
}

func RandomString(stringLength int) (string, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, stringLength)
	if _, err := crand.Read(bytes); err != nil {
		return "", err
	}
	// Run through bytes; replacing each with the equivalent random char.
	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}
	return string(bytes), nil
}
