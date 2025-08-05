package crypter_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdata"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestEncryptDecrypt(t *testing.T) {
	groupID := "GR123"
	digest := &repb.Digest{Hash: "foo", SizeBytes: 123}

	// Generate a few random keys to encrypt/decrypt with.
	numKeys := 10
	keys := make([]*crypter.Key, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = &crypter.Key{Key: make([]byte, 32)}
		_, err := rand.Read(keys[i].Key)
		require.NoError(t, err)
	}

	out := bytes.NewBuffer(nil)
	for _, key := range keys {
		for _, size := range []int64{1, 10, 100, 1000, 1000 * 1000} {
			t.Run(fmt.Sprint(size), func(t *testing.T) {
				e, err := crypter.NewEncryptor(t.Context(), key, digest, ioutil.NewCustomCommitWriteCloser(out), groupID, 1024)
				require.NoError(t, err)

				testData := make([]byte, size)
				_, err = rand.Read(testData)
				require.NoError(t, err)

				// Write the test data in random chunk sizes. The input chunk sizes should
				// not affect the final result.
				testdata.WriteInRandomChunks(t, e, testData)

				d, err := crypter.NewDecryptor(t.Context(), key, digest, io.NopCloser(out), e.Metadata(), groupID, 1024)
				require.NoError(t, err)
				decrypted, err := io.ReadAll(d)

				require.NoError(t, err)

				if !bytes.Equal(decrypted, testData) {
					require.FailNow(t, "original plaintext and decrypted plaintext do not match")
				}
			})
		}
	}
}

func TestDecryptWrongKey(t *testing.T) {
	groupID := "GR123"
	digest := &repb.Digest{Hash: "foo", SizeBytes: 123}
	firstKey := &crypter.Key{Key: []byte(strings.Repeat("a", 32))}

	out := bytes.NewBuffer(nil)
	e, err := crypter.NewEncryptor(t.Context(), firstKey, digest, ioutil.NewCustomCommitWriteCloser(out), groupID, 1024)
	require.NoError(t, err)

	testData := make([]byte, 1024)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	// Write the test data in random chunk sizes. The input chunk sizes should
	// not affect the final result.
	testdata.WriteInRandomChunks(t, e, testData)

	// Decrypting using a different key should not work.
	secondKey := &crypter.Key{Key: []byte(strings.Repeat("f", 32))}
	d, err := crypter.NewDecryptor(t.Context(), secondKey, digest, io.NopCloser(out), e.Metadata(), groupID, 1024)
	require.NoError(t, err)
	_, err = io.ReadAll(d)

	require.Error(t, err)
}

func TestDecryptWrongDigest(t *testing.T) {
	groupID := "GR123"
	digest := &repb.Digest{Hash: "foo", SizeBytes: 123}
	key := &crypter.Key{Key: []byte(strings.Repeat("a", 32))}

	out := bytes.NewBuffer(nil)
	e, err := crypter.NewEncryptor(t.Context(), key, digest, ioutil.NewCustomCommitWriteCloser(out), groupID, 1024)
	require.NoError(t, err)

	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	// Write the test data in random chunk sizes. The input chunk sizes should
	// not affect the final result.
	testdata.WriteInRandomChunks(t, e, testData)

	d, err := crypter.NewDecryptor(t.Context(), key, digest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata(), groupID, 1024)
	require.NoError(t, err)
	decrypted, err := io.ReadAll(d)
	require.NoError(t, err)
	require.Equal(t, decrypted, testData)

	wrongHashDigest := &repb.Digest{Hash: "badhash", SizeBytes: digest.SizeBytes}
	d, err = crypter.NewDecryptor(t.Context(), key, wrongHashDigest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata(), groupID, 1024)
	require.NoError(t, err)
	_, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")

	wrongSizeDigest := &repb.Digest{Hash: digest.Hash, SizeBytes: 9999999999999999}
	d, err = crypter.NewDecryptor(t.Context(), key, wrongSizeDigest, io.NopCloser(bytes.NewReader(out.Bytes())), e.Metadata(), groupID, 1024)
	require.NoError(t, err)
	_, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")
}
