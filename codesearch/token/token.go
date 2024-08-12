package token

import (
	"bufio"
	"hash"
	"hash/fnv"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/buildbuddy-io/buildbuddy/codesearch/sparse"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

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

	trigrams *sparse.Set
	buf      []byte

	n  uint64
	tv uint32
}

func NewTrigramTokenizer() *TrigramTokenizer {
	return &TrigramTokenizer{
		trigrams: sparse.NewSet(1 << 24),
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

func (tt *TrigramTokenizer) Type() types.FieldType {
	return types.TrigramField
}
func (tt *TrigramTokenizer) Ngram() []byte {
	return trigramToBytes(tt.tv)
}

func (tt *TrigramTokenizer) Next() error {
	for {
		b, err := tt.r.ReadByte()
		if err != nil {
			return err
		}
		c := unicode.ToLower(rune(b)) // lowercase
		tt.tv = (tt.tv << 8) & (1<<24 - 1)
		tt.tv |= uint32(c)
		if !validUTF8((tt.tv>>8)&0xFF, tt.tv&0xFF) {
			return status.FailedPreconditionError("invalid utf8")
		}
		if tt.n++; tt.n < 3 {
			continue
		}

		alreadySeen := tt.trigrams.Has(tt.tv)
		if !alreadySeen && tt.tv != 1<<24-1 {
			tt.trigrams.Add(tt.tv)
			return nil
		}
	}
}

type WhitespaceTokenizer struct {
	r io.ByteReader
	n uint64

	sb  *strings.Builder
	tok string
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
	wt.tok = ""
}

func (wt *WhitespaceTokenizer) Type() types.FieldType {
	return types.StringTokenField
}
func (wt *WhitespaceTokenizer) Ngram() []byte {
	return []byte(wt.tok)
}

func (wt *WhitespaceTokenizer) Next() error {
	currentToken := func() string {
		ngram := wt.sb.String()
		wt.sb.Reset()
		return ngram
	}

	for {
		b, err := wt.r.ReadByte()
		if err != nil {
			if wt.sb.Len() > 0 {
				wt.tok = currentToken()
				return nil
			}
			return err
		}
		c := byte(unicode.ToLower(rune(b)))
		if c != ' ' {
			wt.sb.WriteByte(c)
			wt.n++
		} else {
			if wt.sb.Len() > 0 {
				wt.tok = currentToken()
				wt.n++
				return nil
			}
			wt.n++
		}
	}
}

// The following algorithm was inspired by github's codesearch blogpost and
// the implementation here: https://github.com/danlark1/sparse_ngrams
func HashBigram(buf []rune) uint32 {
	const kMul1 = uint64(0xc6a4a7935bd1e995)
	const kMul2 = uint64(0x228876a7198b743)
	a := uint64(buf[0])*kMul1 + uint64(buf[1])*kMul2
	return uint32(a + (^a >> 47))
}

type hashAndPosition struct {
	hash uint32
	pos  int
}

type Options struct {
	// MaxNgramLength controls how long of ngrams will be emitted by
	// BuildAllNgrams and BuildCoveringNgrams. Increasing this value will
	// result in indexing more ngrams and a bigger index size, but will
	// allow for more selectivity at query time (possibly faster queries).
	MaxNgramLength int

	// If true, emitted tokens will all be lowercased.
	LowerCase bool
}

func (o Options) Mods() []Option {
	mods := make([]Option, 1)
	mods[0] = func(o2 *Options) {
		o2.MaxNgramLength = o.MaxNgramLength
		o2.LowerCase = o.LowerCase
	}
	return mods
}

func DefaultOptions() *Options {
	return &Options{
		MaxNgramLength: 3,    // default to trigrams
		LowerCase:      true, // default to lowercase
	}
}

type Option func(*Options)

func WithMaxNgramLength(n int) Option {
	return func(o *Options) {
		o.MaxNgramLength = n
	}
}

func WithLowerCase(t bool) Option {
	return func(o *Options) {
		o.LowerCase = t
	}
}

func BuildAllNgrams(in string, mods ...Option) []string {
	opts := DefaultOptions()
	for _, mod := range mods {
		mod(opts)
	}

	s := []rune(in)
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
			if count <= opts.MaxNgramLength {
				rv = append(rv, string(s[start:start+count]))
			}
			for len(st) > 1 && st[len(st)-1].hash == st[len(st)-2].hash {
				st = st[:len(st)-1]
			}
			st = st[:len(st)-1]
		}
		if len(st) != 0 {
			start := st[len(st)-1].pos
			count := i + 2 - start
			if count <= opts.MaxNgramLength {
				rv = append(rv, string(s[start:start+count]))
			}
		}
		st = append(st, p)
	}
	return rv
}

func BuildCoveringNgrams(in string, mods ...Option) []string {
	opts := DefaultOptions()
	for _, mod := range mods {
		mod(opts)
	}

	s := []rune(in)
	rv := make([]string, 0)

	st := make([]hashAndPosition, 0)
	for i := 0; i+2 <= len(s); i++ {
		p := hashAndPosition{
			hash: HashBigram(s[i:]),
			pos:  i,
		}

		if len(st) > 1 && i-st[0].pos+3 >= opts.MaxNgramLength {
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
	opts        *Options
	scanner     *bufio.Scanner
	hasher      hash.Hash32
	alreadySeen *sparse.Set
	ngrams      []string
	s           []rune
	st          []hashAndPosition
	stringTemp  []byte
}

func NewSparseNgramTokenizer(mods ...Option) *SparseNgramTokenizer {
	opts := DefaultOptions()
	for _, mod := range mods {
		mod(opts)
	}

	return &SparseNgramTokenizer{
		opts:        opts,
		hasher:      fnv.New32(),
		alreadySeen: sparse.NewSet(1<<32 - 1),
		ngrams:      make([]string, 0),
		s:           make([]rune, bufio.MaxScanTokenSize),
		st:          make([]hashAndPosition, 0),
		stringTemp:  make([]byte, utf8.UTFMax*opts.MaxNgramLength),
	}
}

func (tt *SparseNgramTokenizer) Reset(r io.Reader) {
	tt.scanner = bufio.NewScanner(r)
	tt.alreadySeen.Reset()
	tt.ngrams = tt.ngrams[:0]
	tt.s = tt.s[:0]
	tt.st = tt.st[:0]
}

func (tt *SparseNgramTokenizer) Type() types.FieldType {
	return types.SparseNgramField
}

func (tt *SparseNgramTokenizer) Ngram() []byte {
	gram := tt.ngrams[len(tt.ngrams)-1]
	return unsafe.Slice(unsafe.StringData(gram), len(gram))
}

func (tt *SparseNgramTokenizer) refillLine() error {
	for len(tt.s) == 0 {
		if !tt.scanner.Scan() {
			return io.EOF
		}
		if err := tt.scanner.Err(); err != nil {
			return err
		}
		buf := tt.scanner.Bytes()
		lineLength := len(buf)

		if lineLength == 0 {
			continue
		}
		tt.s = tt.s[:lineLength]
		i := 0
		for ; len(buf) > 0; i++ {
			r, size := utf8.DecodeRune(buf)
			if tt.opts.LowerCase {
				tt.s[i] = unicode.ToLower(r)
			} else {
				tt.s[i] = r
			}
			buf = buf[size:]
		}
		tt.s = tt.s[:i]
	}
	return nil
}

// toBytes returns a byte slice from a slice of runes. The returned byte
// slice is interned, so it is only safe to use until the next call to toBytes.
func (tt *SparseNgramTokenizer) toBytes(r []rune) []byte {
	// Walk the array once and check if it's all ascii. Shove it into
	// stringTemp as we go. If it is all ascii, then we can return a slice
	// of stringTemp and be done. Otherwise, fall through to calling
	// utf8.EncodeRune below.
	ascii := true
	for i := 0; i < len(r); i++ {
		if r[i] > unicode.MaxASCII {
			ascii = false
			break
		}
		tt.stringTemp[i] = byte(r[i])
	}
	if ascii {
		return tt.stringTemp[:len(r)]
	}

	offset := 0
	for i := range r {
		offset += utf8.EncodeRune(tt.stringTemp[offset:], r[i])
	}
	return tt.stringTemp[:offset]
}

func (tt *SparseNgramTokenizer) hashNgram(b []byte) uint32 {
	tt.hasher.Reset()
	tt.hasher.Write(b)
	return tt.hasher.Sum32()
}

func (tt *SparseNgramTokenizer) buildAllNgrams() {
	s := tt.s
	tt.st = tt.st[:0]

	for i := 0; i+2 <= len(s); i++ {
		p := hashAndPosition{
			hash: HashBigram(s[i:]),
			pos:  i,
		}
		for len(tt.st) > 0 && p.hash > tt.st[len(tt.st)-1].hash {
			start := tt.st[len(tt.st)-1].pos
			count := i + 2 - start
			if count <= tt.opts.MaxNgramLength {
				buf := tt.toBytes(s[start : start+count])
				ngramID := tt.hashNgram(buf)
				if !tt.alreadySeen.Has(ngramID) {
					tt.ngrams = append(tt.ngrams, string(buf))
					tt.alreadySeen.Add(ngramID)
				}
			}
			for len(tt.st) > 1 && tt.st[len(tt.st)-1].hash == tt.st[len(tt.st)-2].hash {
				tt.st = tt.st[:len(tt.st)-1]
			}
			tt.st = tt.st[:len(tt.st)-1]
		}
		if len(tt.st) != 0 {
			start := tt.st[len(tt.st)-1].pos
			count := i + 2 - start
			if count <= tt.opts.MaxNgramLength {
				buf := tt.toBytes(s[start : start+count])
				ngramID := tt.hashNgram(buf)
				if !tt.alreadySeen.Has(ngramID) {
					tt.ngrams = append(tt.ngrams, string(buf))
					tt.alreadySeen.Add(ngramID)
				}
			}
		}
		tt.st = append(tt.st, p)
	}
	tt.s = tt.s[:0]
}

func (tt *SparseNgramTokenizer) Next() error {
	// Pop off the last ngram on each call to Next().
	if len(tt.ngrams) > 0 {
		tt.ngrams = tt.ngrams[:len(tt.ngrams)-1]
	}

	// Refill tt.ngrams if it's empty.
	for len(tt.ngrams) == 0 {
		// Read another line from the input reader, or bail out
		// if it's exhausted.
		if err := tt.refillLine(); err != nil {
			return err
		}
		tt.buildAllNgrams()
	}
	return nil
}
