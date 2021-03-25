package testdigest

import (
	"bytes"
	"io"
	"io/ioutil"
	"math/rand"
	"sync"
	"testing"
	"time"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	realdigest "github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
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

	panic("unreachable")
}

func NewRandomDigestReader(t testing.TB, sizeBytes int64) (*repb.Digest, io.ReadSeeker) {
	randomSeedOnce.Do(func() {
		randomSrc = &randomDataMaker{rand.NewSource(time.Now().Unix())}
	})

	// Read some random bytes.
	buf := new(bytes.Buffer)
	io.CopyN(buf, randomSrc, sizeBytes)
	readSeeker := bytes.NewReader(buf.Bytes())

	// Compute a digest for the random bytes.
	d, err := realdigest.Compute(readSeeker)
	if err != nil {
		t.Fatal(err)
	}
	readSeeker.Seek(0, 0)
	return d, readSeeker
}

func NewRandomDigestBuf(t testing.TB, sizeBytes int64) (*repb.Digest, []byte) {
	d, rs := NewRandomDigestReader(t, sizeBytes)
	buf, err := ioutil.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}
	return d, buf
}
