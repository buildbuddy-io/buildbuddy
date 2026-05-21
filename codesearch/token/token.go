package token

import (
	"bufio"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"strings"
	"unicode"
	"unicode/utf8"
	"unsafe"

	"github.com/bits-and-blooms/bloom/v3"
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
	freqs    []uint32
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
	tt.freqs = tt.freqs[:0]
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
func (tt *TrigramTokenizer) NgramString() string {
	return string([]byte{
		byte((tt.tv >> 16) & 255),
		byte((tt.tv >> 8) & 255),
		byte(tt.tv & 255),
	})
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

		if tt.tv == 1<<24-1 {
			continue
		}
		if idx, seen := tt.trigrams.Index(tt.tv); seen {
			tt.freqs[idx]++
			continue
		}
		tt.trigrams.Add(tt.tv)
		tt.freqs = append(tt.freqs, 1)
		return nil
	}
}

func (tt *TrigramTokenizer) ForEachTermFrequency(fn func(ngram string, frequency uint32)) {
	for i, trigram := range tt.trigrams.Dense() {
		fn(string(trigramToBytes(trigram)), tt.freqs[i])
	}
}

func (tt *TrigramTokenizer) TermFrequencyStats() types.TermFrequencyStats {
	return termFrequencyStatsFromFrequencies(tt.freqs)
}

type WhitespaceTokenizer struct {
	r io.ByteReader
	n uint64

	sb    *strings.Builder
	tok   string
	freqs map[string]uint32
}

func NewWhitespaceTokenizer() *WhitespaceTokenizer {
	return &WhitespaceTokenizer{
		sb:    &strings.Builder{},
		freqs: make(map[string]uint32),
	}
}

func (wt *WhitespaceTokenizer) Reset(r io.Reader) {
	if br, ok := r.(io.ByteReader); ok {
		wt.r = br
	} else {
		wt.r = bufio.NewReader(r)
	}
	wt.sb.Reset()
	clear(wt.freqs)
	wt.n = 0
	wt.tok = ""
}

func (wt *WhitespaceTokenizer) Type() types.FieldType {
	return types.KeywordField
}
func (wt *WhitespaceTokenizer) Ngram() []byte {
	return []byte(wt.tok)
}
func (wt *WhitespaceTokenizer) NgramString() string {
	return wt.tok
}

func (wt *WhitespaceTokenizer) setToken(tok string) {
	wt.tok = tok
	wt.freqs[tok]++
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
				wt.setToken(currentToken())
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
				wt.setToken(currentToken())
				wt.n++
				return nil
			}
			wt.n++
		}
	}
}

func (wt *WhitespaceTokenizer) ForEachTermFrequency(fn func(ngram string, frequency uint32)) {
	for ngram, freq := range wt.freqs {
		fn(ngram, freq)
	}
}

func (wt *WhitespaceTokenizer) TermFrequencyStats() types.TermFrequencyStats {
	freqs := make([]uint32, 0, len(wt.freqs))
	for _, freq := range wt.freqs {
		freqs = append(freqs, freq)
	}
	return termFrequencyStatsFromFrequencies(freqs)
}

// The following algorithm was inspired by github's codesearch blogpost and
// the implementation here: https://github.com/danlark1/sparse_ngrams
func HashBigram(buf []rune) uint32 {
	const kMul1 = uint64(0xc6a4a7935bd1e995)
	const kMul2 = uint64(0x228876a7198b743)
	a := uint64(buf[0])*kMul1 + uint64(buf[1])*kMul2
	return uint32(a + (^a >> 47))
}

func hashByteBigram(buf []byte) uint32 {
	const kMul1 = uint64(0xc6a4a7935bd1e995)
	const kMul2 = uint64(0x228876a7198b743)
	a := uint64(buf[0])*kMul1 + uint64(buf[1])*kMul2
	return uint32(a + (^a >> 47))
}

type hashAndPosition struct {
	hash uint32
	pos  int
}

// maxCompactASCIINgramLength is the most ASCII bytes that fit in the compact
// uint64 key: one byte stores the length, and the remaining seven store data.
const maxCompactASCIINgramLength = 7

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
	opts         *Options
	scanner      *bufio.Scanner
	hasher       hash.Hash32
	trigramsSeen *sparse.Set
	longramsSeen *bloom.BloomFilter
	asciiSeen    map[uint64]int
	ngrams       []string
	allNgrams    []string
	ngramIndexes []int
	ngramFreqs   []uint32
	s            []rune
	b            []byte
	st           []hashAndPosition
	stringTemp   []byte

	trigramNgramIndexes []int
	stringNgramIndexes  map[string]int
	pendingNgramKind    ngramKind
	pendingNgramIndex   int
	pendingASCIIKey     uint64
}

type ngramKind int

const (
	ngramKindNone ngramKind = iota
	ngramKindTrigram
	ngramKindASCII
	ngramKindString
)

func NewSparseNgramTokenizer(mods ...Option) *SparseNgramTokenizer {
	opts := DefaultOptions()
	for _, mod := range mods {
		mod(opts)
	}

	return &SparseNgramTokenizer{
		opts:         opts,
		hasher:       fnv.New32(),
		trigramsSeen: sparse.NewSet(1 << 24),
		longramsSeen: bloom.NewWithEstimates(100000, 0.0001),
		asciiSeen:    make(map[uint64]int),
		ngrams:       make([]string, 0),
		allNgrams:    make([]string, 0),
		ngramIndexes: make([]int, 0),
		ngramFreqs:   make([]uint32, 0),
		s:            make([]rune, bufio.MaxScanTokenSize),
		b:            make([]byte, bufio.MaxScanTokenSize),
		st:           make([]hashAndPosition, 0),
		stringTemp:   make([]byte, utf8.UTFMax*opts.MaxNgramLength),
	}
}

func (tt *SparseNgramTokenizer) Reset(r io.Reader) {
	tt.scanner = bufio.NewScanner(r)
	if tt.stringNgramIndexes != nil {
		clear(tt.stringNgramIndexes)
	}
	tt.trigramsSeen.Reset()
	tt.longramsSeen.ClearAll()
	clear(tt.asciiSeen)
	tt.ngrams = tt.ngrams[:0]
	tt.allNgrams = tt.allNgrams[:0]
	tt.ngramIndexes = tt.ngramIndexes[:0]
	tt.ngramFreqs = tt.ngramFreqs[:0]
	tt.trigramNgramIndexes = tt.trigramNgramIndexes[:0]
	tt.s = tt.s[:0]
	tt.b = tt.b[:0]
	tt.st = tt.st[:0]
	tt.pendingNgramKind = ngramKindNone
	tt.pendingNgramIndex = 0
	tt.pendingASCIIKey = 0
}

func (tt *SparseNgramTokenizer) Type() types.FieldType {
	return types.SparseNgramField
}

func (tt *SparseNgramTokenizer) Ngram() []byte {
	gram := tt.ngrams[len(tt.ngrams)-1]
	return unsafe.Slice(unsafe.StringData(gram), len(gram))
}
func (tt *SparseNgramTokenizer) NgramString() string {
	return tt.ngrams[len(tt.ngrams)-1]
}

func (tt *SparseNgramTokenizer) NgramFrequency() uint32 {
	if len(tt.ngramIndexes) == 0 {
		return 1
	}
	idx := tt.ngramIndexes[len(tt.ngramIndexes)-1]
	if idx < 0 || idx >= len(tt.ngramFreqs) {
		return 1
	}
	return tt.ngramFreqs[idx]
}

func (tt *SparseNgramTokenizer) ForEachTermFrequency(fn func(ngram string, frequency uint32)) {
	for i, ngram := range tt.allNgrams {
		fn(ngram, tt.ngramFreqs[i])
	}
}

func (tt *SparseNgramTokenizer) TermFrequencyStats() types.TermFrequencyStats {
	return termFrequencyStatsFromFrequencies(tt.ngramFreqs)
}

func termFrequencyStatsFromFrequencies(freqs []uint32) types.TermFrequencyStats {
	stats := types.TermFrequencyStats{}
	for _, freq := range freqs {
		tf := uint64(freq)
		stats.Occurrences += int64(tf)
		stats.UniquePostings++
		stats.CountBytesEstimate += int64(uvarintLen64(tf))
		addTermFrequencyBucket(&stats, tf)
		if freq > 1 {
			duplicateCount := tf - 1
			stats.DuplicatePostings++
			stats.DuplicateOccurrences += int64(duplicateCount)
			stats.ExceptionBytesEstimate += 1 + int64(uvarintLen64(duplicateCount))
		}
	}
	return stats
}

func addTermFrequencyBucket(stats *types.TermFrequencyStats, tf uint64) {
	switch {
	case tf == 1:
		stats.Count1++
	case tf == 2:
		stats.Count2++
	case tf <= 4:
		stats.Count3To4++
	case tf <= 8:
		stats.Count5To8++
	case tf <= 16:
		stats.Count9To16++
	case tf <= 32:
		stats.Count17To32++
	case tf <= 64:
		stats.Count33To64++
	case tf <= 128:
		stats.Count65To128++
	default:
		stats.Count129Plus++
	}
}

func uvarintLen64(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
}

func (tt *SparseNgramTokenizer) incrementNgramFrequency(idx int) {
	if idx < 0 || idx >= len(tt.ngramFreqs) {
		return
	}
	tt.ngramFreqs[idx]++
}

func (tt *SparseNgramTokenizer) appendNgram(buf []byte) {
	ngram := string(buf)
	idx := len(tt.allNgrams)
	tt.allNgrams = append(tt.allNgrams, ngram)
	tt.ngramFreqs = append(tt.ngramFreqs, 1)
	tt.ngrams = append(tt.ngrams, ngram)
	tt.ngramIndexes = append(tt.ngramIndexes, idx)

	switch tt.pendingNgramKind {
	case ngramKindNone:
	case ngramKindTrigram:
		if tt.pendingNgramIndex < len(tt.trigramNgramIndexes) {
			tt.trigramNgramIndexes[tt.pendingNgramIndex] = idx
		}
	case ngramKindASCII:
		tt.asciiSeen[tt.pendingASCIIKey] = idx
	case ngramKindString:
		if tt.stringNgramIndexes == nil {
			tt.stringNgramIndexes = make(map[string]int)
		}
		tt.stringNgramIndexes[ngram] = idx
	}
	tt.pendingNgramKind = ngramKindNone
	tt.pendingNgramIndex = 0
	tt.pendingASCIIKey = 0
}

func (tt *SparseNgramTokenizer) TestOrAdd(buf []byte) bool {
	tt.pendingNgramKind = ngramKindNone
	tt.pendingNgramIndex = 0
	tt.pendingASCIIKey = 0
	if len(buf) < 3 {
		panic("too short of a trigram")
	} else if len(buf) == 3 {
		return tt.testOrAddTrigram(buf)
	} else {
		if key, ok := compactASCIINgramKey(buf); ok {
			return tt.testOrAddASCIIKey(key)
		}
		return tt.testOrAddString(buf)
	}
}

func (tt *SparseNgramTokenizer) TestOrAddASCII(buf []byte) bool {
	tt.pendingNgramKind = ngramKindNone
	tt.pendingNgramIndex = 0
	tt.pendingASCIIKey = 0
	if len(buf) < 3 {
		panic("too short of a trigram")
	} else if len(buf) == 3 {
		return tt.testOrAddTrigram(buf)
	} else if len(buf) <= maxCompactASCIINgramLength {
		return tt.testOrAddASCIIKey(mustCompactASCIIKey(buf))
	}
	return tt.testOrAddString(buf)
}

func (tt *SparseNgramTokenizer) testOrAddTrigram(buf []byte) bool {
	tri := uint32(buf[0])<<16 | uint32(buf[1])<<8 | uint32(buf[2])
	if idx, seen := tt.trigramsSeen.Index(tri); seen {
		if idx < len(tt.trigramNgramIndexes) {
			tt.incrementNgramFrequency(tt.trigramNgramIndexes[idx])
		}
		return true
	}
	tt.trigramsSeen.Add(tri)
	tt.trigramNgramIndexes = append(tt.trigramNgramIndexes, -1)
	tt.pendingNgramKind = ngramKindTrigram
	tt.pendingNgramIndex = len(tt.trigramNgramIndexes) - 1
	return false
}

func (tt *SparseNgramTokenizer) testOrAddASCIIKey(key uint64) bool {
	if idx, seen := tt.asciiSeen[key]; seen {
		tt.incrementNgramFrequency(idx)
		return true
	}
	tt.asciiSeen[key] = -1
	tt.pendingNgramKind = ngramKindASCII
	tt.pendingASCIIKey = key
	return false
}

func (tt *SparseNgramTokenizer) testOrAddString(buf []byte) bool {
	seen := tt.longramsSeen.TestOrAdd(buf)
	if seen {
		if len(buf) > 0 && tt.stringNgramIndexes != nil {
			key := unsafe.String(&buf[0], len(buf))
			if idx, ok := tt.stringNgramIndexes[key]; ok {
				tt.incrementNgramFrequency(idx)
			}
		}
		return true
	}
	tt.pendingNgramKind = ngramKindString
	return false
}

func compactASCIINgramKey(buf []byte) (uint64, bool) {
	if len(buf) == 0 || len(buf) > maxCompactASCIINgramLength {
		return 0, false
	}
	for _, c := range buf {
		if c >= utf8.RuneSelf {
			return 0, false
		}
	}
	key, err := compactASCIIKeyAssumingASCII(buf)
	if err != nil {
		return 0, false
	}
	return key, true
}

func mustCompactASCIIKey(buf []byte) uint64 {
	key, err := compactASCIIKeyAssumingASCII(buf)
	if err != nil {
		panic(err)
	}
	return key
}

func compactASCIIKey(buf []byte) (uint64, error) {
	for i, c := range buf {
		if c >= utf8.RuneSelf {
			return 0, fmt.Errorf("cannot compact non-ASCII byte 0x%x at offset %d", c, i)
		}
	}
	return compactASCIIKeyAssumingASCII(buf)
}

func compactASCIIKeyAssumingASCII(buf []byte) (uint64, error) {
	if len(buf) == 0 {
		return 0, fmt.Errorf("cannot compact empty ASCII ngram")
	}
	if len(buf) > maxCompactASCIINgramLength {
		return 0, fmt.Errorf("cannot compact ASCII ngram of length %d; max compact length is %d", len(buf), maxCompactASCIINgramLength)
	}
	key := uint64(len(buf))
	for i, c := range buf {
		key |= uint64(c) << (8 * (i + 1))
	}
	return key, nil
}

func (tt *SparseNgramTokenizer) refillLine() error {
	for len(tt.s) == 0 && len(tt.b) == 0 {
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

		tt.b = tt.b[:lineLength]
		allASCII := true
		for i, c := range buf {
			if c >= utf8.RuneSelf {
				allASCII = false
				break
			}
			if tt.opts.LowerCase && 'A' <= c && c <= 'Z' {
				c += 'a' - 'A'
			}
			tt.b[i] = c
		}
		if allASCII {
			return nil
		}
		tt.b = tt.b[:0]

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

func (tt *SparseNgramTokenizer) buildAllByteNgrams() {
	s := tt.b
	tt.st = tt.st[:0]

	for i := 0; i+2 <= len(s); i++ {
		p := hashAndPosition{
			hash: hashByteBigram(s[i:]),
			pos:  i,
		}
		for len(tt.st) > 0 && p.hash > tt.st[len(tt.st)-1].hash {
			start := tt.st[len(tt.st)-1].pos
			tt.addByteNgram(s, start, i+2)
			for len(tt.st) > 1 && tt.st[len(tt.st)-1].hash == tt.st[len(tt.st)-2].hash {
				tt.st = tt.st[:len(tt.st)-1]
			}
			tt.st = tt.st[:len(tt.st)-1]
		}
		if len(tt.st) != 0 {
			start := tt.st[len(tt.st)-1].pos
			tt.addByteNgram(s, start, i+2)
		}
		tt.st = append(tt.st, p)
	}
	tt.b = tt.b[:0]
}

func (tt *SparseNgramTokenizer) addByteNgram(s []byte, start, end int) {
	if end-start > tt.opts.MaxNgramLength {
		return
	}
	buf := s[start:end]
	if !tt.TestOrAddASCII(buf) {
		tt.appendNgram(buf)
	}
}

func (tt *SparseNgramTokenizer) buildAllNgrams() {
	if len(tt.b) > 0 {
		tt.buildAllByteNgrams()
		return
	}

	s := tt.s
	tt.st = tt.st[:0]

	for i := 0; i+2 <= len(s); i++ {
		p := hashAndPosition{
			hash: HashBigram(s[i:]),
			pos:  i,
		}
		for len(tt.st) > 0 && p.hash > tt.st[len(tt.st)-1].hash {
			start := tt.st[len(tt.st)-1].pos
			tt.addRuneNgram(s, start, i+2)
			for len(tt.st) > 1 && tt.st[len(tt.st)-1].hash == tt.st[len(tt.st)-2].hash {
				tt.st = tt.st[:len(tt.st)-1]
			}
			tt.st = tt.st[:len(tt.st)-1]
		}
		if len(tt.st) != 0 {
			start := tt.st[len(tt.st)-1].pos
			tt.addRuneNgram(s, start, i+2)
		}
		tt.st = append(tt.st, p)
	}
	tt.s = tt.s[:0]
}

func (tt *SparseNgramTokenizer) addRuneNgram(s []rune, start, end int) {
	if end-start > tt.opts.MaxNgramLength {
		return
	}
	buf := tt.toBytes(s[start:end])
	if !tt.TestOrAdd(buf) {
		tt.appendNgram(buf)
	}
}

func (tt *SparseNgramTokenizer) Next() error {
	// Pop off the last ngram on each call to Next().
	if len(tt.ngrams) > 0 {
		tt.ngrams = tt.ngrams[:len(tt.ngrams)-1]
		tt.ngramIndexes = tt.ngramIndexes[:len(tt.ngramIndexes)-1]
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
