package commandutil

import (
	"bytes"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// LimitedBuffer is a bytes.Buffer-like type that enforces a maximum size on
// writes. If a write would exceed the limit, the write fails with
// RESOURCE_EXHAUSTED and no bytes are written.
//
// If max is 0, no limit is enforced.
type LimitedBuffer struct {
	buf bytes.Buffer
	max uint64
}

func NewLimitedBuffer(max uint64) *LimitedBuffer {
	return &LimitedBuffer{max: max}
}

func (b *LimitedBuffer) Write(p []byte) (int, error) {
	if b.max == 0 {
		return b.buf.Write(p)
	}
	if uint64(b.buf.Len())+uint64(len(p)) > b.max {
		return 0, status.ResourceExhaustedErrorf("stdout/stderr output size limit exceeded: %d bytes", b.max)
	}
	return b.buf.Write(p)
}

func (b *LimitedBuffer) Read(p []byte) (int, error) { return b.buf.Read(p) }
func (b *LimitedBuffer) Bytes() []byte              { return b.buf.Bytes() }
func (b *LimitedBuffer) String() string             { return b.buf.String() }
func (b *LimitedBuffer) Len() int                   { return b.buf.Len() }
func (b *LimitedBuffer) Reset()                     { b.buf.Reset() }
