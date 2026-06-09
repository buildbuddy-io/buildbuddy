package posting

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"

	"github.com/RoaringBitmap/roaring/roaring64"
)

// countedListMagic is the prefix that identifies a posting list whose
// serialized bytes carry per-doc term frequencies in addition to the doc IDs.
// "CSTF" stands for CodeSearch Term Frequency; the trailing byte is the format
// version. A roaring64 payload begins with an 8-byte little-endian count of the
// distinct high-32-bit key groups it holds. Our doc IDs are
// (generation<<32 | docIndex), so a posting list spans only a handful of
// generations and that leading count is a small integer — its low byte is tiny
// and the upper bytes are zero, so the first 5 bytes can never spell the ASCII
// sequence "CSTF" (0x43 0x53 0x54 0x46). This magic therefore cannot be
// mistaken for a plain roaring payload.
//
// Layout (uvarint is encoding/binary.Uvarint):
//
//	magic      [5]byte             // {'C','S','T','F', version}
//	roaringLen uvarint             // length of the roaring payload that follows
//	roaring    [roaringLen]byte    // roaring64 serialization of the doc-ID set
//	tail                           // RLE-encoded per-doc frequencies
//
// Frequency tail is run-length-encoded as (runlen, value) uvarint pairs in
// the order the roaring iterator emits doc IDs. Decode loop sums runs until
// the bitmap's cardinality is consumed. Chosen because real ngram TFs are
// dominated by long runs of TF=1 punctuated by sparse outliers, which this
// encoding compresses to a handful of bytes per list.
//
// RLE is purely a wire codec: in memory frequencies are always a plain parallel
// column (see Freqs). marshalCounted / unmarshalCountedReadOnly are the only
// code that knows this layout.
var countedListMagic = []byte{'C', 'S', 'T', 'F', 4}

// A ReadOnlyList is a read-only interface for a posting list. This interface exists to support
// low/no-allocation reads of posting lists that are read from a pebble DB - a roaring.Bitmap
// can be created by sending a buffer owned by pebble directly to roaring.FromUnsafeBytes, which
// does not copy or take ownership of the buffer. However, when this is done, the posting list
// is not allowed to be modified, and this interface is used in those cases to ensure no accidental
// modifications are made.
type ReadOnlyList interface {
	GetCardinality() uint64
	ToArray() []uint64
	Iterator() roaring64.IntPeekable64
	Frequency(uint64) uint32
	GetSerializedSizeInBytes() uint64
	MarshalInto(buf []byte) error
	Marshal() ([]byte, error)
}

// List is a full mutable interface for a posting list. It should be used when a posting list is
// created in a way that is safe for modification
type List interface {
	ReadOnlyList
	Or(ReadOnlyList)
	And(ReadOnlyList)
	AndNot(ReadOnlyList)
	Add(uint64)
	Remove(uint64)
	Clear()
}

type roaringWrapper struct {
	*roaring64.Bitmap
}

func (w *roaringWrapper) Or(l ReadOnlyList) {
	w.Bitmap.Or(readOnlyListToRoaring(l))
}
func (w *roaringWrapper) And(l ReadOnlyList) {
	w.Bitmap.And(readOnlyListToRoaring(l))
}

// AndNot is the same as set difference, equivalent to w - l
func (w *roaringWrapper) AndNot(l ReadOnlyList) {
	w.Bitmap.AndNot(readOnlyListToRoaring(l))
}

func (w *roaringWrapper) Frequency(id uint64) uint32 {
	if w.Bitmap.Contains(id) {
		return 1
	}
	return 0
}

func (w *roaringWrapper) MarshalInto(buf []byte) error {
	stream := bytes.NewBuffer(buf)
	_, err := w.Bitmap.WriteTo(stream)
	return err
}

func (w *roaringWrapper) Marshal() ([]byte, error) {
	stream := bytes.NewBuffer(make([]byte, 0, int(w.GetSerializedSizeInBytes())))
	_, err := w.Bitmap.WriteTo(stream)
	return stream.Bytes(), err
}

// copyInto copies src into buf, failing if buf lacks the capacity.
func copyInto(buf, src []byte) error {
	if cap(buf) < len(src) {
		return fmt.Errorf("buffer too small: got capacity %d, need %d", cap(buf), len(src))
	}
	copy(buf[:len(src)], src)
	return nil
}

// docIDSet is the doc-ID half of a posting list: a sorted set that serializes to
// a roaring bitmap. It stores a single element inline in `first` (avoiding an
// `ids` allocation for the common rare-ngram case) and switches to the `ids`
// slice at two or more elements. `ids` is populated only when len >= 2.
//
// It is deliberately frequency-agnostic. MergeList keeps a parallel Freqs column
// in sync with it using the positions implied by appendSorted / removeAt and
// reported by indexOf. Callers mutate the set only through its methods rather
// than its fields.
type docIDSet struct {
	first uint64
	ids   []uint64
	last  uint64
	count int
}

func (s *docIDSet) len() int {
	return s.count
}

func (s *docIDSet) at(i int) uint64 {
	if s.count == 1 {
		return s.first
	}
	return s.ids[i]
}

// indexOf returns the position of id in the set and whether it is present.
func (s *docIDSet) indexOf(id uint64) (int, bool) {
	switch s.count {
	case 0:
		return 0, false
	case 1:
		if s.first == id {
			return 0, true
		}
		return 0, false
	default:
		i := sort.Search(len(s.ids), func(i int) bool {
			return s.ids[i] >= id
		})
		if i < len(s.ids) && s.ids[i] == id {
			return i, true
		}
		return 0, false
	}
}

func (s *docIDSet) array() []uint64 {
	switch s.count {
	case 0:
		return []uint64{}
	case 1:
		return []uint64{s.first}
	default:
		out := make([]uint64, len(s.ids))
		copy(out, s.ids)
		return out
	}
}

func (s *docIDSet) iterator() roaring64.IntPeekable64 {
	return s.roaring().Iterator()
}

func (s *docIDSet) roaring() *roaring64.Bitmap {
	bm := roaring64.New()
	switch s.count {
	case 0:
	case 1:
		bm.Add(s.first)
	default:
		bm.AddMany(s.ids)
	}
	return bm
}

// roaringBytes serializes the set to its roaring representation.
func (s *docIDSet) roaringBytes() ([]byte, error) {
	bm := s.roaring()
	stream := bytes.NewBuffer(make([]byte, 0, int(bm.GetSerializedSizeInBytes())))
	if _, err := bm.WriteTo(stream); err != nil {
		return nil, err
	}
	return stream.Bytes(), nil
}

// appendSorted adds id as the new greatest element. id must be strictly greater
// than the current maximum, matching the indexing path's contract.
func (s *docIDSet) appendSorted(id uint64) {
	if s.count > 0 && id <= s.last {
		panicNotIncreasing(id, s.last)
	}
	switch s.count {
	case 0:
		s.first = id
	case 1:
		s.ids = append(s.ids, s.first, id)
	default:
		s.ids = append(s.ids, id)
	}
	s.last = id
	s.count++
}

// setSorted replaces the contents with the given ascending ids (copied; not
// retained).
func (s *docIDSet) setSorted(ids []uint64) {
	s.reset()
	if len(ids) == 0 {
		return
	}
	s.first = ids[0]
	s.last = ids[len(ids)-1]
	s.count = len(ids)
	if len(ids) > 1 {
		s.ids = append(s.ids, ids...)
	}
}

// removeAt deletes the element at position i, collapsing back to the inline
// single-element form when one element remains.
func (s *docIDSet) removeAt(i int) {
	if s.count <= 1 {
		s.reset()
		return
	}
	s.ids = append(s.ids[:i], s.ids[i+1:]...)
	s.count = len(s.ids)
	if s.count == 1 {
		s.first = s.ids[0]
		s.last = s.first
		s.ids = s.ids[:0]
		return
	}
	s.first = s.ids[0]
	s.last = s.ids[len(s.ids)-1]
}

func (s *docIDSet) reset() {
	s.first = 0
	s.ids = s.ids[:0]
	s.last = 0
	s.count = 0
}

// panicNotIncreasing is kept out of line (and not inlined) so the fmt.Sprintf
// cost stays off appendSorted's hot path.
//
//go:noinline
func panicNotIncreasing(id, last uint64) {
	panic(fmt.Sprintf("docIDSet appends must be strictly increasing: got %d after %d", id, last))
}

// Freqs is the per-document term-frequency column that rides alongside a posting
// list's doc IDs, indexed by position (parallel to the doc-ID order). It is the
// single in-memory representation of frequencies in this package — the doc-ID
// structure is never burdened with them, matching how Lucene and Tantivy keep
// freqs in a parallel stream.
//
// The overwhelmingly common case — every term frequency is 1 — is stored for
// free: `dense` stays nil and `materialized` false, so an all-ones list costs
// nothing beyond its doc IDs and serializes as a plain roaring bitmap. The
// column materializes into a dense []uint32 only once some frequency differs
// from 1, and from then on `dense` stays the same length as the doc-ID set.
type Freqs struct {
	dense        []uint32
	materialized bool
}

func (f *Freqs) at(i int) uint32 {
	if !f.materialized {
		return 1
	}
	return f.dense[i]
}

// appendFreq records freq for the next (highest) position. n is the number of
// positions already present. The column stays unmaterialized while every
// frequency is 1, backfilling ones for the existing positions the first time a
// non-unit frequency arrives.
func (f *Freqs) appendFreq(n int, freq uint32) {
	if !f.materialized {
		if freq == 1 {
			return
		}
		f.dense = f.dense[:0]
		for i := 0; i < n; i++ {
			f.dense = append(f.dense, 1)
		}
		f.materialized = true
	}
	f.dense = append(f.dense, freq)
}

// setAll replaces the contents with the given per-position frequencies, staying
// unmaterialized (all-ones) when every frequency is 1.
func (f *Freqs) setAll(freqs []uint32) {
	for _, v := range freqs {
		if v != 1 {
			f.dense = append(f.dense[:0], freqs...)
			f.materialized = true
			return
		}
	}
	f.clear()
}

// removeAt deletes the entry at position i, shifting later entries left.
func (f *Freqs) removeAt(i int) {
	if !f.materialized {
		return
	}
	f.dense = append(f.dense[:i], f.dense[i+1:]...)
}

func (f *Freqs) clear() {
	f.dense = f.dense[:0]
	f.materialized = false
}

// any reports whether any position has a frequency other than 1.
func (f *Freqs) any() bool {
	for _, v := range f.dense {
		if v != 1 {
			return true
		}
	}
	return false
}

// MergeList is the package's unified mutable posting list. It backs every path
// that mutates a list:
//
//   - indexing/build: AddWithFrequency appends doc IDs (in increasing order, the
//     indexer's normal case) through an allocation-light append fast path;
//   - the pebble value merger and delete compaction: Or / RemoveAll combine and
//     prune lists, summing and preserving frequencies;
//   - query result combination: And / AndNot / Or set algebra.
//
// It is a structure-of-arrays: a docIDSet (a roaring bitmap with an inline
// single-element fast path for rare ngrams) plus a parallel Freqs column.
// Frequencies are run-length-encoded only at serialization time (marshalCounted);
// in memory they stay a plain column.
type MergeList struct {
	docs  docIDSet
	freqs Freqs

	serialized []byte
}

func (l *MergeList) GetCardinality() uint64            { return uint64(l.docs.len()) }
func (l *MergeList) ToArray() []uint64                 { return l.docs.array() }
func (l *MergeList) Iterator() roaring64.IntPeekable64 { return l.docs.iterator() }

func (l *MergeList) frequencyAt(i int) uint32 {
	return l.freqs.at(i)
}

func (l *MergeList) Frequency(id uint64) uint32 {
	i, ok := l.docs.indexOf(id)
	if !ok {
		return 0
	}
	return l.freqs.at(i)
}

func (l *MergeList) Clear() {
	l.docs.reset()
	l.freqs.clear()
	l.serialized = nil
}

func (l *MergeList) Add(id uint64) {
	l.AddWithFrequency(id, 1)
}

// AddWithFrequency inserts id with the given term frequency (a zero frequency is
// treated as 1). Appending an id greater than the current maximum — the indexing
// path's normal case — is an allocation-light append; inserting into the middle
// or front rebuilds the parallel arrays. Re-adding an id already present leaves
// the list unchanged.
func (l *MergeList) AddWithFrequency(id uint64, freq uint32) {
	if freq == 0 {
		freq = 1
	}
	n := l.docs.len()
	if n == 0 || id > l.docs.last {
		// This branch condition is exactly appendSorted's precondition (empty, or
		// strictly greater than the current max), so appendSorted cannot panic
		// here. That guarantee is also what lets us grow freqs first: the two
		// parallel columns can never be left out of step.
		l.freqs.appendFreq(n, freq)
		l.docs.appendSorted(id)
		l.serialized = nil
		return
	}
	if _, ok := l.docs.indexOf(id); ok {
		return
	}

	oldIDs := l.docs.array()
	ids := make([]uint64, 0, len(oldIDs)+1)
	freqs := make([]uint32, 0, len(oldIDs)+1)
	inserted := false
	for i, oldID := range oldIDs {
		if !inserted && id < oldID {
			ids = append(ids, id)
			freqs = append(freqs, freq)
			inserted = true
		}
		ids = append(ids, oldID)
		freqs = append(freqs, l.frequencyAt(i))
	}
	if !inserted {
		ids = append(ids, id)
		freqs = append(freqs, freq)
	}
	l.setFromSorted(ids, freqs)
}

// setFromSorted resets l to the given ascending doc IDs and parallel
// frequencies. freqs may be nil, in which case every frequency defaults to 1.
// The input slices are copied; l does not retain them.
func (l *MergeList) setFromSorted(ids []uint64, freqs []uint32) {
	l.docs.setSorted(ids)
	if freqs != nil {
		l.freqs.setAll(freqs)
	} else {
		l.freqs.clear()
	}
	l.serialized = nil
}

// Or merges other into l, summing frequencies for shared doc IDs.
func (l *MergeList) Or(other ReadOnlyList) {
	rIDs := other.ToArray()
	if len(rIDs) == 0 {
		return
	}
	rFreqs := frequenciesInOrder(other, rIDs)
	n := l.docs.len()
	if n == 0 {
		l.setFromSorted(rIDs, rFreqs)
		return
	}

	ids := make([]uint64, 0, n+len(rIDs))
	freqs := make([]uint32, 0, n+len(rIDs))
	i, j := 0, 0
	for i < n && j < len(rIDs) {
		leftID := l.docs.at(i)
		rightID := rIDs[j]
		switch {
		case leftID < rightID:
			ids = append(ids, leftID)
			freqs = append(freqs, l.frequencyAt(i))
			i++
		case rightID < leftID:
			ids = append(ids, rightID)
			freqs = append(freqs, rFreqs[j])
			j++
		default:
			ids = append(ids, leftID)
			freqs = append(freqs, l.frequencyAt(i)+rFreqs[j])
			i++
			j++
		}
	}
	for ; i < n; i++ {
		ids = append(ids, l.docs.at(i))
		freqs = append(freqs, l.frequencyAt(i))
	}
	for ; j < len(rIDs); j++ {
		ids = append(ids, rIDs[j])
		freqs = append(freqs, rFreqs[j])
	}
	l.setFromSorted(ids, freqs)
}

func (l *MergeList) And(other ReadOnlyList) {
	if l.docs.len() == 0 {
		return
	}
	if other.GetCardinality() == 0 {
		l.Clear()
		return
	}
	otherBM := readOnlyListToRoaring(other)

	if !l.freqs.materialized {
		bm := l.docs.roaring()
		bm.And(otherBM)
		l.docs.setSorted(bm.ToArray())
		l.serialized = nil
		return
	}

	ids := l.docs.array()
	keepIDs := make([]uint64, 0, len(ids))
	keepFreqs := make([]uint32, 0, len(ids))
	for i, id := range ids {
		if otherBM.Contains(id) {
			keepIDs = append(keepIDs, id)
			keepFreqs = append(keepFreqs, l.freqs.at(i))
		}
	}
	l.setFromSorted(keepIDs, keepFreqs)
}

func (l *MergeList) AndNot(other ReadOnlyList) {
	l.RemoveAll(other)
}

// Remove deletes id from the list, preserving the frequencies of surviving docs.
func (l *MergeList) Remove(id uint64) {
	i, ok := l.docs.indexOf(id)
	if !ok {
		return
	}
	l.freqs.removeAt(i)
	l.docs.removeAt(i)
	l.serialized = nil
}

// RemoveAll deletes every doc ID in dels from the list in one pass, preserving
// the frequencies of survivors. This is the bulk path used by delete
// compaction: it costs O(cardinality) per call regardless of how many IDs dels
// holds, rather than a per-ID search-and-shift. An all-ones list (no stored
// frequencies) reduces to a single roaring AndNot.
func (l *MergeList) RemoveAll(dels ReadOnlyList) {
	if l.docs.len() == 0 || dels.GetCardinality() == 0 {
		return
	}
	delBM := readOnlyListToRoaring(dels)

	if !l.freqs.materialized {
		// No per-doc frequencies to preserve: a plain set difference suffices.
		bm := l.docs.roaring()
		bm.AndNot(delBM)
		l.docs.setSorted(bm.ToArray())
		l.serialized = nil
		return
	}

	// Counted list: walk the postings once, keeping survivors and their
	// frequencies. delBM.Contains is ~O(1), so cost is independent of |dels|.
	ids := l.docs.array()
	keepIDs := make([]uint64, 0, len(ids))
	keepFreqs := make([]uint32, 0, len(ids))
	for i, id := range ids {
		if !delBM.Contains(id) {
			keepIDs = append(keepIDs, id)
			keepFreqs = append(keepFreqs, l.freqs.at(i))
		}
	}
	l.setFromSorted(keepIDs, keepFreqs)
}

func (l *MergeList) ensureSerialized() error {
	if l.serialized != nil {
		return nil
	}
	roaringBuf, err := l.docs.roaringBytes()
	if err != nil {
		return err
	}
	if !l.freqs.any() {
		l.serialized = roaringBuf
		return nil
	}
	l.serialized = marshalCounted(roaringBuf, l.freqs.dense)
	return nil
}

func (l *MergeList) GetSerializedSizeInBytes() uint64 {
	if err := l.ensureSerialized(); err != nil {
		return 0
	}
	return uint64(len(l.serialized))
}

func (l *MergeList) MarshalInto(buf []byte) error {
	if err := l.ensureSerialized(); err != nil {
		return err
	}
	return copyInto(buf, l.serialized)
}

func (l *MergeList) Marshal() ([]byte, error) {
	if err := l.ensureSerialized(); err != nil {
		return nil, err
	}
	return bytes.Clone(l.serialized), nil
}

// marshalCounted serializes a counted posting list: the magic prefix, the
// roaring doc-ID payload, then the run-length-encoded frequency tail. freqs must
// be the full per-position frequency slice (length == cardinality); it is the
// caller's job to fall back to a plain roaring payload when every frequency is 1.
func marshalCounted(roaringBuf []byte, freqs []uint32) []byte {
	buf := make([]byte, 0, len(countedListMagic)+binary.MaxVarintLen64+len(roaringBuf)+len(freqs))
	buf = append(buf, countedListMagic...)
	buf = binary.AppendUvarint(buf, uint64(len(roaringBuf)))
	buf = append(buf, roaringBuf...)
	for i := 0; i < len(freqs); {
		v := freqs[i]
		j := i + 1
		for j < len(freqs) && freqs[j] == v {
			j++
		}
		buf = binary.AppendUvarint(buf, uint64(j-i))
		buf = binary.AppendUvarint(buf, uint64(v))
		i = j
	}
	return buf
}

// frequenciesInOrder returns the per-doc frequencies of l in the same order as
// ids (which must be l.ToArray()). Lists that already hold a parallel frequency
// column return it directly; otherwise every present doc has frequency 1.
func frequenciesInOrder(l ReadOnlyList, ids []uint64) []uint32 {
	switch t := l.(type) {
	case *countedReadOnlyList:
		return t.freqs
	case *MergeList:
		freqs := make([]uint32, len(ids))
		for i := range ids {
			freqs[i] = t.freqs.at(i)
		}
		return freqs
	}
	freqs := make([]uint32, len(ids))
	for i, id := range ids {
		f := l.Frequency(id)
		if f == 0 {
			f = 1
		}
		freqs[i] = f
	}
	return freqs
}

// countedReadOnlyList wraps a roaring bitmap and a parallel slice of per-doc
// frequencies. It does NOT embed *roaringWrapper because the underlying bitmap
// may be backed by pebble-owned memory (via FromUnsafeBytes), so mutating
// methods like Or/And/AndNot would corrupt that memory. Only read-only methods
// are exposed.
type countedReadOnlyList struct {
	bm         *roaring64.Bitmap
	freqs      []uint32
	serialized []byte
}

func (w *countedReadOnlyList) GetCardinality() uint64 {
	return w.bm.GetCardinality()
}

func (w *countedReadOnlyList) ToArray() []uint64 {
	return w.bm.ToArray()
}

func (w *countedReadOnlyList) Iterator() roaring64.IntPeekable64 {
	return w.bm.Iterator()
}

func (w *countedReadOnlyList) Frequency(id uint64) uint32 {
	if !w.bm.Contains(id) {
		return 0
	}
	idx := int(w.bm.Rank(id)) - 1
	if idx >= 0 && idx < len(w.freqs) {
		return w.freqs[idx]
	}
	return 1
}

func (w *countedReadOnlyList) GetSerializedSizeInBytes() uint64 {
	return uint64(len(w.serialized))
}

func (w *countedReadOnlyList) MarshalInto(buf []byte) error {
	return copyInto(buf, w.serialized)
}

func (w *countedReadOnlyList) Marshal() ([]byte, error) {
	return bytes.Clone(w.serialized), nil
}

func NewReadOnlyList(ids ...uint64) ReadOnlyList {
	return NewList(ids...)
}

func NewList(ids ...uint64) List {
	bm := roaring64.New()
	if len(ids) > 0 {
		bm.AddMany(ids)
	}
	return &roaringWrapper{bm}
}

// NewMergeList returns an empty unified mutable posting list. See MergeList.
func NewMergeList() *MergeList {
	return &MergeList{}
}

// NewBuilderList returns a MergeList seeded with the given ids, a convenience for
// the indexing path (which then appends term frequencies via AddWithFrequency)
// and for tests. Ids are added via Add, so they need not be sorted.
func NewBuilderList(ids ...uint64) *MergeList {
	pl := &MergeList{}
	for _, id := range ids {
		pl.Add(id)
	}
	return pl
}

func readOnlyListToRoaring(l ReadOnlyList) *roaring64.Bitmap {
	if bm, ok := l.(*roaringWrapper); ok {
		return bm.Bitmap
	}
	if bm, ok := l.(*countedReadOnlyList); ok {
		return bm.bm
	}
	bm := roaring64.New()
	bm.AddMany(l.ToArray())
	return bm
}

func Unmarshal(buf []byte) (*MergeList, error) {
	bm, freqs, err := decodePostings(buf)
	if err != nil {
		return nil, err
	}
	ml := NewMergeList()
	ml.setFromSorted(bm.ToArray(), freqs)
	return ml, nil
}

// UnmarshalReadOnly unmarshals a posting list from a byte slice without copying
// the underlying data. Important: buf must remain valid for the lifetime of the returned
// ReadOnlyList.
func UnmarshalReadOnly(buf []byte) (ReadOnlyList, error) {
	bm, freqs, err := decodePostings(buf)
	if err != nil {
		return nil, err
	}
	if freqs == nil {
		return &roaringWrapper{bm}, nil
	}
	return &countedReadOnlyList{bm: bm, freqs: freqs, serialized: buf}, nil
}

// decodePostings is the shared decode path for both Unmarshal and
// UnmarshalReadOnly. It splits buf into the roaring doc-ID payload and (for
// counted lists) the RLE frequency tail, then decodes each: a plain list is just
// the roaring payload, while a counted list carries the magic prefix, an
// explicit roaring length, the roaring payload, then the tail. freqs is nil for
// a plain (all-ones) list. The returned bitmap aliases buf via FromUnsafeBytes,
// so buf must outlive any read-only list built from it.
func decodePostings(buf []byte) (*roaring64.Bitmap, []uint32, error) {
	counted := bytes.HasPrefix(buf, countedListMagic)

	// For a plain list the whole buffer is the roaring payload; for a counted
	// list the length prefix bounds it and the remainder is the frequency tail.
	roaringBuf := buf
	var tail []byte
	if counted {
		rest := buf[len(countedListMagic):]
		roaringLen, n := binary.Uvarint(rest)
		if n <= 0 {
			return nil, nil, fmt.Errorf("invalid counted posting list roaring length")
		}
		rest = rest[n:]
		if roaringLen > uint64(len(rest)) {
			return nil, nil, fmt.Errorf("counted posting list roaring length %d exceeds remaining buffer %d", roaringLen, len(rest))
		}
		roaringBuf = rest[:roaringLen:roaringLen]
		tail = rest[roaringLen:]
	}

	bm := roaring64.New()
	read, err := bm.FromUnsafeBytes(roaringBuf)
	if err != nil {
		return nil, nil, err
	}
	if read < int64(len(roaringBuf)) {
		return nil, nil, fmt.Errorf("read only %d bytes of roaring buffer with size %d", read, len(roaringBuf))
	}

	if !counted {
		return bm, nil, nil
	}
	freqs, err := decodeRLEFreqs(tail, bm.GetCardinality())
	if err != nil {
		return nil, nil, err
	}
	return bm, freqs, nil
}

// decodeRLEFreqs expands the run-length-encoded frequency tail (see
// marshalCounted) into a per-position slice of length cardinality.
func decodeRLEFreqs(tail []byte, cardinality uint64) ([]uint32, error) {
	if cardinality > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("counted posting list cardinality %d overflows int", cardinality)
	}
	freqs := make([]uint32, 0, int(cardinality))
	for len(tail) > 0 {
		runlen, n := binary.Uvarint(tail)
		if n <= 0 || runlen == 0 {
			return nil, fmt.Errorf("invalid counted posting list run length")
		}
		tail = tail[n:]
		value, n := binary.Uvarint(tail)
		if n <= 0 {
			return nil, fmt.Errorf("invalid counted posting list frequency")
		}
		if value > uint64(^uint32(0)) {
			return nil, fmt.Errorf("posting list frequency %d overflows uint32", value)
		}
		tail = tail[n:]
		if uint64(len(freqs))+runlen > cardinality {
			return nil, fmt.Errorf("counted posting list runs total %d exceed cardinality %d", uint64(len(freqs))+runlen, cardinality)
		}
		freq := uint32(value)
		for i := uint64(0); i < runlen; i++ {
			freqs = append(freqs, freq)
		}
	}
	if uint64(len(freqs)) != cardinality {
		return nil, fmt.Errorf("decoded %d frequencies for posting list with cardinality %d", len(freqs), cardinality)
	}
	return freqs, nil
}

// A FieldMap is a collection of postingLists that are keyed by the field that
// was matched.
//
// For example, if a document {"a": "aaa", "b": "bbb"} matches a
// query like (:eq * "bbb") or (:eq "b" "bbb"), then the fieldmap will store
// that docID in a postinglist for the field "b". It's normal for a document
// to be in multiple fields of the fieldmap at once if that document has
// multiple fields that matched the query.
type FieldMap map[string]List

func NewFieldMap() FieldMap {
	return make(map[string]List)
}

func (fm *FieldMap) OrField(fieldName string, pl2 ReadOnlyList) {
	if pl, ok := (*fm)[fieldName]; ok {
		pl.Or(pl2)
	} else {
		newPl := NewMergeList()
		newPl.Or(pl2)
		(*fm)[fieldName] = newPl
	}
}

func (fm *FieldMap) Or(fm2 FieldMap) {
	for fieldName, pl2 := range fm2 {
		fm.OrField(fieldName, pl2)
	}
}
func (fm *FieldMap) And(fm2 FieldMap) {
	mergedPL := fm.ToPosting()
	mergedPL.And(fm2.ToPosting())

	for fieldName, pl2 := range fm2 {
		fm.OrField(fieldName, pl2)
	}
	for _, pl := range *fm {
		pl.And(mergedPL)
	}
}
func (fm *FieldMap) ToPosting() List {
	pl := NewMergeList()
	for _, pl2 := range *fm {
		pl.Or(pl2)
	}
	return pl
}

func (fm *FieldMap) GetCardinality() uint64 {
	return fm.ToPosting().GetCardinality()
}

func (fm *FieldMap) Remove(r ReadOnlyList) {
	f := (*fm)
	for _, pl := range f {
		pl.AndNot(r)
	}
}
