package random

import (
	"crypto/rand"
	"math/big"
)

func RandUint64() (uint64, error) {
	max := big.NewInt(0)
	// SetBytes is BE   0  1  2  3  4  5  6  7  8
	max.SetBytes([]byte{1, 0, 0, 0, 0, 0, 0, 0, 0})
	rand.Int(rand.Reader, max)
	n, err := rand.Int(rand.Reader, max)
	if err != nil {
		return 0, err
	}
	return n.Uint64(), nil
}

func RandomString(stringLength int) (string, error) {
	const letters = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	bytes := make([]byte, stringLength)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	// Run through bytes; replacing each with the equivalent random char.
	for i, b := range bytes {
		bytes[i] = letters[b%byte(len(letters))]
	}
	return string(bytes), nil
}
