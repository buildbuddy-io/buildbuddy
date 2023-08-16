package digest_test

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"runtime"
	"strings"
	"testing"

	"github.com/klauspost/cpuid/v2"
	sha256simd "github.com/minio/sha256-simd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func BenchmarkDigestCompute(b *testing.B) {
	for sizeStr, size := range map[string]int64{
		"8B":    8,
		"10B":   10,
		"100B":  100,
		"1KB":   1 * 1000,
		"10KB":  10 * 1000,
		"100KB": 100 * 1000,
		"1MB":   1 * 1000 * 1000,
		"10MB":  10 * 1000 * 1000,
	} {
		buf := make([]byte, size)
		_, err := rand.Read(buf)
		require.NoError(b, err)

		for hashName, hashFn := range map[string]func() hash.Hash{
			"stdlib": sha256.New,
			"simd":   sha256simd.New,
		} {
			b.Run(fmt.Sprintf("%s/%s", hashName, sizeStr), func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					hashFn().Sum(buf)
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
			for i, h := range []hash.Hash{sha256.New(), sha256simd.New()} {
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

func TestCPUArch(t *testing.T) {
	fmt.Println("Name:", cpuid.CPU.BrandName)
	fmt.Println("PhysicalCores:", cpuid.CPU.PhysicalCores)
	fmt.Println("ThreadsPerCore:", cpuid.CPU.ThreadsPerCore)
	fmt.Println("LogicalCores:", cpuid.CPU.LogicalCores)
	fmt.Println("Family", cpuid.CPU.Family, "Model:", cpuid.CPU.Model, "Vendor ID:", cpuid.CPU.VendorID)
	fmt.Println("Features:", strings.Join(cpuid.CPU.FeatureSet(), ","))
	fmt.Println("Cacheline bytes:", cpuid.CPU.CacheLine)
	fmt.Println("L1 Data Cache:", cpuid.CPU.Cache.L1D, "bytes")
	fmt.Println("L1 Instruction Cache:", cpuid.CPU.Cache.L1I, "bytes")
	fmt.Println("L2 Cache:", cpuid.CPU.Cache.L2, "bytes")
	fmt.Println("L3 Cache:", cpuid.CPU.Cache.L3, "bytes")
	fmt.Println("Frequency", cpuid.CPU.Hz, "hz")

	switch runtime.GOARCH {
	case "arm64":
		assert.True(t, cpuid.CPU.Has(cpuid.SHA2), "SHA2 is not supported")
	case "amd64":
		assert.True(t, cpuid.CPU.Supports(cpuid.SHA), "SHA is not supported")
		assert.True(t, cpuid.CPU.Supports(cpuid.SSSE3), "SSSE3 is not supported")
		assert.True(t, cpuid.CPU.Supports(cpuid.SSE4), "SSE4 is not supported")
		assert.True(t, cpuid.CPU.Supports(cpuid.AVX512F, cpuid.AVX512DQ, cpuid.AVX512BW, cpuid.AVX512VL))
	default:
	}
}
