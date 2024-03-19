package index

import (
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/codesearch/sparse"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"io"
)

type ByteToken []byte

func (b ByteToken) Field() int32 {
	f := int32(0)
	for i := 0; i < 4; i++ {
		f = ((f << 8) | int32(b[i]))
	}
	return f
}
func (b ByteToken) Ngram() []byte {
	return b[4:]
}
func (b ByteToken) String() string {
	return fmt.Sprintf("fieldID: %d, ngram: %q", b.Field(), b.Ngram())
}

func newByteToken(fieldID types.FieldType, ngram []byte) ByteToken {
	buf := make([]byte, len(ngram)+4)

	// Pack the int32 fieldID into the first bytes of buf.
	buf[0] = byte((fieldID >> 24) & 255)
	buf[1] = byte((fieldID >> 16) & 255)
	buf[2] = byte((fieldID >> 8) & 255)
	buf[3] = byte(fieldID & 255)

	// Copy the ngram itself into buf.
	copy(buf[4:], ngram)
	return ByteToken(buf)
}

type TrigramTokenizer struct {
	// TODO(tylerw): why is this here?
	fieldID types.FieldType
	r       io.ByteReader

	trigrams *sparse.Set
	buf      []byte

	n  int64
	tv uint32
}

func NewTrigramTokenizer(fieldID types.FieldType) *TrigramTokenizer {
	return &TrigramTokenizer{
		trigrams: sparse.NewSet(1 << 24),
		buf:      make([]byte, 16384),
		fieldID:  fieldID,
	}
}

func (tt *TrigramTokenizer) Reset(r io.ByteReader) {
	tt.r = r
	tt.trigrams.Reset()
	tt.buf = tt.buf[:0]
	tt.n = 0
	tt.tv = 0
}

func (tt *TrigramTokenizer) Next() (types.Token, error) {
	for {
		c, err := tt.r.ReadByte()
		if err != nil {
			return nil, err
		}
		tt.tv = (tt.tv << 8) & (1<<24 - 1)
		tt.tv |= uint32(c)
		if !validUTF8((tt.tv>>8)&0xFF, tt.tv&0xFF) {
			return nil, status.FailedPreconditionError("invalid utf8")
		}
		if tt.n++; tt.n < 3 {
			continue
		}

		alreadySeen := tt.trigrams.Has(tt.tv)
		if !alreadySeen && tt.tv != 1<<24-1 {
			tt.trigrams.Add(tt.tv)
			return newByteToken(tt.fieldID, trigramToBytes(tt.tv)), nil
		}
	}
}
