package testdigest

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	realdigest "github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing/ctxio"
)

var (
	randomSeedOnce sync.Once
	randomSrc      io.Reader
)

type randomDataMaker struct {
	src rand.Source
}

func (r *randomDataMaker) Read(p []byte) (n int, err error) {
	todo := len(p)
	offset := 0
	for {
		val := int64(r.src.Int63())
		for i := 0; i < 8; i++ {
			p[offset] = byte(val & 0xff)
			todo--
			if todo == 0 {
				return len(p), nil
			}
			offset++
			val >>= 8
		}
	}
}

func NewRandomDigestReader(t testing.TB, ctx context.Context, sizeBytes int64) (*repb.Digest, ctxio.ReadSeeker) {
	randomSeedOnce.Do(func() {
		randomSrc = &randomDataMaker{rand.NewSource(time.Now().Unix())}
	})

	// Read some random bytes.
	buf := new(bytes.Buffer)
	io.CopyN(buf, randomSrc, sizeBytes)
	readSeeker := ctxio.CtxReadSeekerWrapper(bytes.NewReader(buf.Bytes()))

	// Compute a digest for the random bytes.
	d, err := realdigest.Compute(ctx, readSeeker)
	if err != nil {
		t.Fatal(err)
	}
	readSeeker.Seek(ctx, 0, 0)
	return d, readSeeker
}

func NewRandomDigestBuf(t testing.TB, ctx context.Context, sizeBytes int64) (*repb.Digest, []byte) {
	d, rs := NewRandomDigestReader(t, ctx, sizeBytes)
	buf, err := ctxio.ReadAll(ctx, rs)
	if err != nil {
		t.Fatal(err)
	}
	return d, buf
}

func ReadDigestAndClose(t *testing.T, ctx context.Context, r ctxio.ReadCloser) *repb.Digest {
	defer r.Close(ctx)

	d, err := realdigest.Compute(ctx, r)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
