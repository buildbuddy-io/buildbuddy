package index

import (
	"bufio"
	"fmt"
	"io"

	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type Set struct {
	dense  []uint32
	sparse []uint32
}

// This is sparse.Set from:
// https://github.com/google/codesearch/blob/master/sparse/set.go
//
// NewSet returns a new Set with a given maximum size.
// The set can contain numbers in [0, max-1].
func NewSet(max uint32) *Set {
	return &Set{
		sparse: make([]uint32, max),
	}
}

// Init initializes a Set to have a given maximum size.
// The set can contain numbers in [0, max-1].
func (s *Set) Init(max uint32) {
	s.sparse = make([]uint32, max)
}

// Reset clears (empties) the set.
func (s *Set) Reset() {
	s.dense = s.dense[:0]
}

// Add adds x to the set if it is not already there.
func (s *Set) Add(x uint32) {
	v := s.sparse[x]
	if v < uint32(len(s.dense)) && s.dense[v] == x {
		return
	}
	n := len(s.dense)
	s.sparse[x] = uint32(n)
	s.dense = append(s.dense, x)
}

// Has reports whether x is in the set.
func (s *Set) Has(x uint32) bool {
	v := s.sparse[x]
	return v < uint32(len(s.dense)) && s.dense[v] == x
}

// Dense returns the values in the set.
// The values are listed in the order in which they
// were inserted.
func (s *Set) Dense() []uint32 {
	return s.dense
}

// Len returns the number of values in the set.
func (s *Set) Len() int {
	return len(s.dense)
}

type byteToken struct {
	fieldType types.FieldType
	tok       []byte
}

func (b byteToken) Type() types.FieldType {
	return b.fieldType
}
func (b byteToken) Ngram() []byte {
	return b.tok
}
func (b byteToken) String() string {
	return fmt.Sprintf("field type: %s, ngram: %q", b.Type(), b.Ngram())
}
func newByteToken(fieldType types.FieldType, ngram []byte) byteToken {
	return byteToken{fieldType, ngram}
}

// validUTF8 reports whether the byte pair can appear in a
// valid sequence of UTF-8-encoded code points.
func validUTF8(c1, c2 uint32) bool {
	switch {
	case c1 < 0x80:
		// 1-byte, must be followed by 1-byte or first of multi-byte
		return c2 < 0x80 || 0xc0 <= c2 && c2 < 0xf8
	case c1 < 0xc0:
		// continuation byte, can be followed by nearly anything
		return c2 < 0xf8
	case c1 < 0xf8:
		// first of multi-byte, must be followed by continuation byte
		return 0x80 <= c2 && c2 < 0xc0
	}
	return false
}

func trigramToBytes(tv uint32) []byte {
	l := byte((tv >> 16) & 255)
	m := byte((tv >> 8) & 255)
	r := byte(tv & 255)
	return []byte{l, m, r}
}

type TrigramTokenizer struct {
	r io.ByteReader

	trigrams *Set
	buf      []byte

	n  int64
	tv uint32
}

func NewTrigramTokenizer() *TrigramTokenizer {
	return &TrigramTokenizer{
		trigrams: NewSet(1 << 24),
		buf:      make([]byte, 16384),
	}
}

func (tt *TrigramTokenizer) Reset(r io.Reader) {
	if br, ok := r.(io.ByteReader); ok {
		tt.r = br
	} else {
		tt.r = bufio.NewReader(r)
	}
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
			return newByteToken(types.TrigramField, trigramToBytes(tt.tv)), nil
		}
	}
}

type WhitespaceTokenizer struct {
	s *bufio.Scanner

	seen map[string]struct{}
	buf  []byte
}

func NewWhitespaceTokenizer() *WhitespaceTokenizer {
	return &WhitespaceTokenizer{
		seen: make(map[string]struct{}),
	}
}

func (wt *WhitespaceTokenizer) Reset(r io.Reader) {
	wt.s = bufio.NewScanner(r)
	wt.s.Split(bufio.ScanWords) // split on " ".
	wt.seen = make(map[string]struct{})
}

func (wt *WhitespaceTokenizer) Next() (types.Token, error) {
	for wt.s.Scan() {
		buf := wt.s.Bytes()
		tokenBuf := make([]byte, len(buf))
		copy(tokenBuf, buf)
		return newByteToken(types.TrigramField, tokenBuf), nil
	}
	return nil, io.EOF
}
