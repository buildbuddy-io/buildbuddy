package hash_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/stretchr/testify/assert"
)

func TestHashBytes(t *testing.T) {
	emptyHash := hash.Bytes([]byte(""))
	assert.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", emptyHash)

	fooHash := hash.Bytes([]byte("foo"))
	assert.Equal(t, "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae", fooHash)
}

func TestHashString(t *testing.T) {
	emptyHash := hash.String("")
	assert.Equal(t, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", emptyHash)

	fooHash := hash.String("foo")
	assert.Equal(t, "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae", fooHash)
}

func TestHashStrings(t *testing.T) {
	cases := []struct {
		inputs       []string
		expectedHash string
	}{
		{
			inputs:       []string{""},
			expectedHash: "cd372fb85148700fa88095e3492d3f9f5beb43e555e5ff26d95f5a6adc36f8e6",
		},
		{
			inputs:       []string{"foo"},
			expectedHash: "505221025f9701f8a05cc22cbafeec897598b2924a9d665cbc10f0073d66da20",
		},
		{
			inputs:       []string{"bar"},
			expectedHash: "bd142ccf5968384068077c58de4d3ad833204a151d3e9f1182703f07b69125b8",
		},
		{
			inputs:       []string{"foo", "bar"},
			expectedHash: "ec321de56af3b66fb49e89cfe346562388af387db689165d6f662a3950286a57",
		},
		{
			inputs:       []string{"bar", "foo"},
			expectedHash: "ec5134518944504462951c1e23b4728a144542776bc9477d785d2fd3064aaf76",
		},
		{
			inputs:       []string{"foo", "bar", "baz"},
			expectedHash: "9462ae47a25296a8341fb0bf14a52a0cedfcecaf9d75b33f8a1e9d61d4e1b6d2",
		},
		{
			inputs:       []string{"baz", "bar", "foo"},
			expectedHash: "3745ceb009bd639f22c207acaa86e78221465c71dd46c7c5c91b4449a321c8f2",
		},
	}
	for _, tc := range cases {
		assert.Equal(t, tc.expectedHash, hash.Strings(tc.inputs...))
	}
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
