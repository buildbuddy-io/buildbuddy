package ioutil

import (
	"bytes"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

// LimitBuffer writes to an internal buffer until the specified limit is reached.
// Once the limit is exceeded, writes return a RESOURCE_EXHAUSTED error. A zero
// limit disables enforcement.
type LimitBuffer struct {
	buf         bytes.Buffer
	limit       uint64
	description string
}

// NewLimitBuffer constructs a LimitBuffer that enforces the provided byte limit.
// If limit is zero, no limit is applied. The description string is used in the
// generated error message and should describe what is being limited.
func NewLimitBuffer(limit uint64, description string) *LimitBuffer {
	return &LimitBuffer{
		limit:       limit,
		description: description,
	}
}

// Write appends p to the buffer respecting the configured limit. When the
// limit is exceeded, it writes up to the limit and returns a
// RESOURCE_EXHAUSTED error describing the overflow.
func (lb *LimitBuffer) Write(p []byte) (int, error) {
	if lb.limit == 0 {
		return lb.buf.Write(p)
	}
	current := uint64(lb.buf.Len())
	if current >= lb.limit {
		return 0, lb.limitError(current + uint64(len(p)))
	}
	remaining := lb.limit - current
	if uint64(len(p)) <= remaining {
		return lb.buf.Write(p)
	}
	n, _ := lb.buf.Write(p[:int(remaining)])
	return n, lb.limitError(current + uint64(len(p)))
}

// Bytes returns a slice of length lb.Len() holding the unread portion of the
// buffer. The slice is valid only until the next write.
func (lb *LimitBuffer) Bytes() []byte {
	return lb.buf.Bytes()
}

// Len returns the number of accumulated bytes.
func (lb *LimitBuffer) Len() int {
	return lb.buf.Len()
}

// Reset discards all the data in the buffer but keeps the underlying storage
// for future writes.
func (lb *LimitBuffer) Reset() {
	lb.buf.Reset()
}

// String returns the contents of the unread portion of the buffer as a string.
func (lb *LimitBuffer) String() string {
	return lb.buf.String()
}

func (lb *LimitBuffer) limitError(totalRequested uint64) error {
	desc := lb.description
	if desc == "" {
		desc = "buffer size"
	}
	return status.ResourceExhaustedErrorf("%s limit exceeded: %d bytes requested (limit: %d bytes)", desc, totalRequested, lb.limit)
}
