package testdigest

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var (
	randomSeedOnce sync.Once
	randomGen      *digest.Generator
)

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

func NewRandomDigestBuf(t testing.TB, sizeBytes int64) (*repb.Digest, []byte) {
	d, rs := NewRandomDigestReader(t, sizeBytes)
	buf, err := io.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}
	return d, buf
}

func ReadDigestAndClose(t *testing.T, r io.ReadCloser) *repb.Digest {
	defer r.Close()
	d, err := digest.Compute(r)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
