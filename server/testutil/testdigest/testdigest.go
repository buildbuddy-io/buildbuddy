package testdigest

import (
	"bytes"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

var (
	randomSeedOnce sync.Once
	randomGen      *digest.Generator
)

func compressibleBlobOfSize(sizeBytes int) []byte {
	out := make([]byte, 0, sizeBytes)
	for len(out) < sizeBytes {
		runEnd := len(out) + 100 + rand.Intn(100)
		if runEnd > sizeBytes {
			runEnd = sizeBytes
		}

		runChar := byte(rand.Intn('Z'-'A'+1)) + 'A'
		for len(out) < runEnd {
			out = append(out, runChar)
		}
	}
	return out
}

func NewRandomDigestReader(t testing.TB, sizeBytes int64) (*repb.Digest, io.ReadSeeker) {
	randomSeedOnce.Do(func() {
		randomGen = digest.RandomGenerator(time.Now().Unix())
	})
	d, r, err := randomGen.RandomDigestReader(sizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	return d, r
}

func newRandomCompressibleDigestBuf(t testing.TB, sizeBytes int64) (*repb.Digest, []byte) {
	blob := compressibleBlobOfSize(int(sizeBytes))
	d, err := digest.Compute(bytes.NewReader(blob), repb.DigestFunction_SHA256)
	if err != nil {
		t.Fatal(err)
	}
	return d, blob
}

func newRandomDigestBuf(t testing.TB, sizeBytes int64) (*repb.Digest, []byte) {
	d, rs := NewRandomDigestReader(t, sizeBytes)
	buf, err := io.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}
	return d, buf
}

func NewRandomResourceAndBuf(t testing.TB, sizeBytes int64, cacheType rspb.CacheType, instanceName string) (*rspb.ResourceName, []byte) {
	d, buf := newRandomDigestBuf(t, sizeBytes)
	return digest.NewResourceName(d, instanceName, cacheType, repb.DigestFunction_SHA256).ToProto(), buf
}

func RandomCompressibleCASResourceBuf(t testing.TB, sizeBytes int64, instanceName string) (*rspb.ResourceName, []byte) {
	d, buf := newRandomCompressibleDigestBuf(t, sizeBytes)
	return digest.NewResourceName(d, instanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto(), buf
}

func RandomCASResourceBuf(t testing.TB, sizeBytes int64) (*rspb.ResourceName, []byte) {
	return NewRandomResourceAndBuf(t, sizeBytes, rspb.CacheType_CAS, "" /*instanceName*/)
}

func RandomACResourceBuf(t testing.TB, sizeBytes int64) (*rspb.ResourceName, []byte) {
	return NewRandomResourceAndBuf(t, sizeBytes, rspb.CacheType_AC, "" /*instanceName*/)
}

func ReadDigestAndClose(t testing.TB, r io.ReadCloser) *repb.Digest {
	defer r.Close()
	d, err := digest.Compute(r, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	return d
}
