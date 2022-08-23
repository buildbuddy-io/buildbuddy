package testdigest

import (
	"bytes"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	realdigest "github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
)

var (
	randomSeedOnce sync.Once
	randomGen      *Generator
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

type Generator struct {
	randMaker *randomDataMaker
}

func NewGenerator(seed int64) *Generator {
	return &Generator{randMaker: &randomDataMaker{rand.NewSource(seed)}}
}

func (g *Generator) RandomDigestReader(sizeBytes int64) (*repb.Digest, io.ReadSeeker, error) {
	// Read some random bytes.
	buf := new(bytes.Buffer)
	if _, err := io.CopyN(buf, g.randMaker, sizeBytes); err != nil {
		return nil, nil, err
	}
	readSeeker := bytes.NewReader(buf.Bytes())

	// Compute a digest for the random bytes.
	d, err := realdigest.Compute(readSeeker)
	if err != nil {
		return nil, nil, err
	}
	if _, err := readSeeker.Seek(0, 0); err != nil {
		return nil, nil, err
	}
	return d, readSeeker, nil
}

func (g *Generator) RandomDigestBuf(sizeBytes int64) (*repb.Digest, []byte, error) {
	d, rs, err := g.RandomDigestReader(sizeBytes)
	if err != nil {
		return nil, nil, err
	}
	buf, err := io.ReadAll(rs)
	if err != nil {
		return nil, nil, err
	}
	return d, buf, nil
}

func NewRandomDigestReader(t *testing.T, sizeBytes int64) (*repb.Digest, io.ReadSeeker, error) {
	randomSeedOnce.Do(func() {
		randomGen = NewGenerator(time.Now().UnixNano())
	})
	return randomGen.RandomDigestReader(sizeBytes)
}

func NewRandomDigestBuf(t *testing.T, sizeBytes int64) (*repb.Digest, []byte) {
	d, rs, err := NewRandomDigestReader(t, sizeBytes)
	if err != nil {
		t.Fatal(err)
	}
	buf, err := io.ReadAll(rs)
	if err != nil {
		t.Fatal(err)
	}
	return d, buf
}

func ReadDigestAndClose(t *testing.T, r io.ReadCloser) *repb.Digest {
	defer r.Close()

	d, err := realdigest.Compute(r)
	if err != nil {
		t.Fatal(err)
	}
	return d
}
