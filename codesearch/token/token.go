package token

import (
	"bufio"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"strings"

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

	n  uint64
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
	r io.ByteReader
	n uint64

	sb *strings.Builder
}

func NewWhitespaceTokenizer() *WhitespaceTokenizer {
	return &WhitespaceTokenizer{
		sb: &strings.Builder{},
	}
}

func (wt *WhitespaceTokenizer) Reset(r io.Reader) {
	if br, ok := r.(io.ByteReader); ok {
		wt.r = br
	} else {
		wt.r = bufio.NewReader(r)
	}
	wt.sb.Reset()
	wt.n = 0
}

func (wt *WhitespaceTokenizer) Next() (types.Token, error) {
	currentToken := func() types.Token {
		ngram := []byte(wt.sb.String())
		wt.sb.Reset()
		return newByteToken(types.TrigramField, ngram)
	}

	for {
		c, err := wt.r.ReadByte()
		if err != nil {
			if wt.sb.Len() > 0 {
				return currentToken(), nil
			}
			return nil, err
		}
		if c != ' ' {
			wt.sb.WriteByte(c)
			wt.n++
		} else {
			if wt.sb.Len() > 0 {
				tok := currentToken()
				wt.n++
				return tok, nil
			}
			wt.n++
		}
	}
}

// The following algorithm was inspired by github's codesearch blogpost and
// the implementation here: https://github.com/danlark1/sparse_ngrams
func HashBigram(buf []byte) uint32 {
	const kMul1 = uint64(0xc6a4a7935bd1e995)
	const kMul2 = uint64(0x228876a7198b743)
	a := uint64(buf[0])*kMul1 + uint64(buf[1])*kMul2
	return uint32(a + (^a >> 47))
}

type hashAndPosition struct {
	hash uint32
	pos  int
}

func BuildAllNgrams(s []byte) []string {
	rv := make([]string, 0)

	st := make([]hashAndPosition, 0)
	for i := 0; i+2 <= len(s); i++ {
		p := hashAndPosition{
			hash: HashBigram(s[i:]),
			pos:  i,
		}
		for len(st) > 0 && p.hash > st[len(st)-1].hash {
			start := st[len(st)-1].pos
			count := i + 2 - start
			rv = append(rv, string(s[start:start+count]))

			for len(st) > 1 && st[len(st)-1].hash == st[len(st)-2].hash {
				st = st[:len(st)-1]
			}
			st = st[:len(st)-1]
		}
		if len(st) != 0 {
			start := st[len(st)-1].pos
			count := i + 2 - start
			rv = append(rv, string(s[start:start+count]))
		}
		st = append(st, p)
	}
	return rv
}

func BuildCoveringNgrams(s []byte) []string {
	const maxNgramLength = 16
	rv := make([]string, 0)

	st := make([]hashAndPosition, 0)
	for i := 0; i+2 <= len(s); i++ {
		p := hashAndPosition{
			hash: HashBigram(s[i:]),
			pos:  i,
		}

		if len(st) > 1 && i-st[0].pos+3 >= maxNgramLength {
			start := st[0].pos
			count := st[1].pos + 2 - start
			rv = append(rv, string(s[start:start+count]))
			st = st[1:]
		}

		for len(st) > 0 && p.hash > st[len(st)-1].hash {
			if st[0].hash == st[len(st)-1].hash {
				start := st[len(st)-1].pos
				count := i + 2 - start
				rv = append(rv, string(s[start:start+count]))

				for len(st) > 1 {
					lastPos := st[len(st)-1].pos + 2
					st = st[:len(st)-1]

					start := st[len(st)-1].pos
					count := lastPos - start
					rv = append(rv, string(s[start:start+count]))
				}
			}
			st = st[:len(st)-1]
		}
		st = append(st, p)
	}
	for len(st) > 1 {
		lastPos := st[len(st)-1].pos + 2
		st = st[:len(st)-1]
		start := st[len(st)-1].pos
		count := lastPos - start
		rv = append(rv, string(s[start:start+count]))
	}
	return rv
}

type SparseNgramTokenizer struct {
	scanner *bufio.Scanner
	ngrams  []string
	seen    *Set
	hasher  hash.Hash32
}

func NewSparseNgramTokenizer() *SparseNgramTokenizer {
	return &SparseNgramTokenizer{
		seen:   NewSet(1<<32 - 1),
		ngrams: make([]string, 0),
		hasher: fnv.New32(),
	}
}

func (tt *SparseNgramTokenizer) Reset(r io.Reader) {
	tt.scanner = bufio.NewScanner(r)
	tt.ngrams = tt.ngrams[:0]
	tt.seen.Reset()
}

func (tt *SparseNgramTokenizer) Next() (types.Token, error) {
	for len(tt.ngrams) == 0 {
		if !tt.scanner.Scan() {
			return nil, io.EOF
		}
		if err := tt.scanner.Err(); err != nil {
			return nil, err
		}
		line := tt.scanner.Text()
		if len(line) > 0 {
			for _, ngram := range BuildAllNgrams([]byte(line)) {
				ngram = strings.ToLower(ngram)
				tt.hasher.Reset()
				tt.hasher.Write([]byte(ngram))
				ngramID := tt.hasher.Sum32()
				if alreadySeen := tt.seen.Has(ngramID); !alreadySeen {
					tt.ngrams = append(tt.ngrams, ngram)
					tt.seen.Add(ngramID)
				}
			}
		}
	}
	ngram := tt.ngrams[len(tt.ngrams)-1]
	tt.ngrams = tt.ngrams[:len(tt.ngrams)-1]
	return newByteToken(types.SparseNgramField, []byte(ngram)), nil
}
