package crypter_test

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"io"
	"strings"
	"sync"
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
	keys := make([]*crypter.DerivedKey, numKeys)
	for i := 0; i < numKeys; i++ {
		keys[i] = &crypter.DerivedKey{Key: make([]byte, 32)}
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
				require.NoError(t, e.Close())

				d, err := crypter.NewDecryptor(t.Context(), key, digest, io.NopCloser(out), groupID, 1024)
				require.NoError(t, err)
				decrypted, err := io.ReadAll(d)

				require.NoError(t, err)
				require.NoError(t, d.Close())

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
	firstKey := &crypter.DerivedKey{Key: []byte(strings.Repeat("a", 32))}

	out := bytes.NewBuffer(nil)
	e, err := crypter.NewEncryptor(t.Context(), firstKey, digest, ioutil.NewCustomCommitWriteCloser(out), groupID, 1024)
	require.NoError(t, err)

	testData := make([]byte, 1024)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	// Write the test data in random chunk sizes. The input chunk sizes should
	// not affect the final result.
	testdata.WriteInRandomChunks(t, e, testData)
	require.NoError(t, e.Close())

	// Decrypting using a different key should not work.
	secondKey := &crypter.DerivedKey{Key: []byte(strings.Repeat("f", 32))}
	d, err := crypter.NewDecryptor(t.Context(), secondKey, digest, io.NopCloser(out), groupID, 1024)
	require.NoError(t, err)
	_, err = io.ReadAll(d)

	require.Error(t, err)
	require.NoError(t, d.Close())
}

func TestDecryptWrongDigest(t *testing.T) {
	groupID := "GR123"
	digest := &repb.Digest{Hash: "foo", SizeBytes: 123}
	key := &crypter.DerivedKey{Key: []byte(strings.Repeat("a", 32))}

	out := bytes.NewBuffer(nil)
	e, err := crypter.NewEncryptor(t.Context(), key, digest, ioutil.NewCustomCommitWriteCloser(out), groupID, 1024)
	require.NoError(t, err)

	testData := make([]byte, 1000)
	_, err = rand.Read(testData)
	require.NoError(t, err)

	// Write the test data in random chunk sizes. The input chunk sizes should
	// not affect the final result.
	testdata.WriteInRandomChunks(t, e, testData)
	require.NoError(t, e.Close())

	d, err := crypter.NewDecryptor(t.Context(), key, digest, io.NopCloser(bytes.NewReader(out.Bytes())), groupID, 1024)
	require.NoError(t, err)
	decrypted, err := io.ReadAll(d)
	require.NoError(t, err)
	require.Equal(t, decrypted, testData)
	require.NoError(t, d.Close())

	wrongHashDigest := &repb.Digest{Hash: "badhash", SizeBytes: digest.SizeBytes}
	d, err = crypter.NewDecryptor(t.Context(), key, wrongHashDigest, io.NopCloser(bytes.NewReader(out.Bytes())), groupID, 1024)
	require.NoError(t, err)
	_, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")
	require.NoError(t, d.Close())

	wrongSizeDigest := &repb.Digest{Hash: digest.Hash, SizeBytes: 9999999999999999}
	d, err = crypter.NewDecryptor(t.Context(), key, wrongSizeDigest, io.NopCloser(bytes.NewReader(out.Bytes())), groupID, 1024)
	require.NoError(t, err)
	_, err = io.ReadAll(d)
	require.ErrorContains(t, err, "authentication failed")
	require.NoError(t, d.Close())
}

// TestEncryptDecryptConcurrentClose exercises the buffer pool by running many
// concurrent encrypt/decrypt round-trips, each of which closes the
// (de)crypter. Run under `-race` to detect buffer aliasing from incorrect pool
// reuse or double-return.
func TestEncryptDecryptConcurrentClose(t *testing.T) {
	groupID := "GR123"
	digest := &repb.Digest{Hash: "foo", SizeBytes: 123}
	key := &crypter.DerivedKey{Key: []byte(strings.Repeat("a", 32))}

	const goroutines = 8
	const iterations = 50
	// Vary the size to hit different power-of-2 buckets in the variable-size
	// pool. Include sizes that span multiple chunks so the encryptor exercises
	// the encrypt-in-place buf across flushes.
	sizes := []int{1, 100, 1023, 1024, 4096, 32 * 1024}

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				size := sizes[(g+i)%len(sizes)]
				testData := make([]byte, size)
				_, err := rand.Read(testData)
				require.NoError(t, err)

				out := bytes.NewBuffer(nil)
				e, err := crypter.NewEncryptor(t.Context(), key, digest, ioutil.NewCustomCommitWriteCloser(out), groupID, 1024)
				require.NoError(t, err)
				_, err = e.Write(testData)
				require.NoError(t, err)
				require.NoError(t, e.Commit())
				require.NoError(t, e.Close())

				d, err := crypter.NewDecryptor(t.Context(), key, digest, io.NopCloser(out), groupID, 1024)
				require.NoError(t, err)
				decrypted, err := io.ReadAll(d)
				require.NoError(t, err)
				require.NoError(t, d.Close())

				if !bytes.Equal(decrypted, testData) {
					require.FailNowf(t, "mismatch", "goroutine=%d iter=%d size=%d", g, i, size)
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestDoubleClose verifies that calling Close twice is safe and does not
// double-return buffers to the pool.
func TestDoubleClose(t *testing.T) {
	groupID := "GR123"
	digest := &repb.Digest{Hash: "foo", SizeBytes: 123}
	key := &crypter.DerivedKey{Key: []byte(strings.Repeat("a", 32))}

	out := bytes.NewBuffer(nil)
	e, err := crypter.NewEncryptor(t.Context(), key, digest, ioutil.NewCustomCommitWriteCloser(out), groupID, 1024)
	require.NoError(t, err)
	_, err = e.Write([]byte("hello"))
	require.NoError(t, err)
	require.NoError(t, e.Commit())
	require.NoError(t, e.Close())
	// Second Close must not panic or re-Put the buffer.
	require.NoError(t, e.Close())

	d, err := crypter.NewDecryptor(t.Context(), key, digest, io.NopCloser(out), groupID, 1024)
	require.NoError(t, err)
	_, err = io.ReadAll(d)
	require.NoError(t, err)
	require.NoError(t, d.Close())
	// Second Close must not panic or re-Put the buffer.
	require.NoError(t, d.Close())
}
