package hash_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/stretchr/testify/assert"
)

func TestHashString(t *testing.T) {
	emptyHash := hash.String("")
	assert.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", emptyHash)

	fooHash := hash.String("foo")
	assert.Equal(t, "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae", fooHash)
}

func TestMemHash(t *testing.T) {
	bytesHash := hash.MemHash([]byte{'b', 'y', 't', 'e'})
	assert.Greater(t, bytesHash, uint64(0))

	bytesHash2 := hash.MemHash([]byte{'b', 'y', 't', 'e'})
	assert.Equal(t, bytesHash, bytesHash2)
}

func TestMemHashString(t *testing.T) {
	stringHash := hash.MemHashString("string")
	assert.Greater(t, stringHash, uint64(0))

	stringHash2 := hash.MemHashString("string")
	assert.Equal(t, stringHash, stringHash2)
}
