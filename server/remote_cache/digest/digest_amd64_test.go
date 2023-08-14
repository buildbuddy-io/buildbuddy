//go:build amd64

package digest_test

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"math/rand"
	"testing"

	sha256simd "github.com/minio/sha256-simd"
	"github.com/stretchr/testify/require"
)

func hasherWithServer() hash.Hash {
	server := sha256simd.NewAvx512Server()
	return sha256simd.NewAvx512(server)
}

func BenchmarkSIMDDigestCompute(b *testing.B) {
	for _, size := range []int{1, 10, 100, 1000, 10_000, 100_000, 1_000_000} {
		for _, tc := range []struct {
			newHasher func() hash.Hash
			name      string
		}{
			{sha256.New, "without_SIMD"},
			{sha256simd.New, "with_SIMD_no_server"},
			{hasherWithServer, "with_SIMD_with_server"},
		} {
			b.Run(fmt.Sprintf("%s/%d", tc.name, size), func(b *testing.B) {
				buf := make([]byte, size)
				_, err := rand.Read(buf)
				require.NoError(b, err)

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					h := tc.newHasher()

					_, err := io.Copy(h, bytes.NewReader(buf))
					require.NoError(b, err)
					h.Sum([]byte{})
				}
			})
		}
	}
}

func TestHashStableOutput(t *testing.T) {
	for _, size := range []int{1, 10, 100, 1000, 10_000, 100_000} {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			buf := make([]byte, size)
			_, err := rand.Read(buf)
			require.NoError(t, err)

			lastSum := []byte{}
			for i, h := range []hash.Hash{sha256.New(), sha256simd.New(), hasherWithServer()} {
				_, err := io.Copy(h, bytes.NewReader(buf))
				require.NoError(t, err)

				curSum := h.Sum([]byte{})
				if i > 0 {
					require.Equal(t, lastSum, curSum)
				}
				lastSum = curSum
			}
		})
	}
}
