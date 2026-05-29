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
// version. Roaring bitmaps begin with their own cookie (0x3B30 / 0x3B31), so
// this 5-byte sequence cannot be mistaken for a plain roaring payload.
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

// termFreqs stores per-position term frequencies for a posting list. It keeps
// the overwhelmingly common "every TF is 1" case allocation-free
// (the slice stays nil, `materialized` false) and otherwise stores a plain
// dense slice — never a map, so the indexing hot loop pays only a slice append
// with no hashing. The backing slice is retained across clear() so a reused
// list avoids re-allocating it.
//
// Frequencies are indexed by position (parallel to the posting list's doc-ID
// order), not by doc ID. That keeps the append path branch-light at the cost of
// shifting entries on the rare out-of-order insert / remove.
type termFreqs struct {
	dense        []uint32
	materialized bool
}

func (f *termFreqs) at(i int) uint32 {
	if !f.materialized {
		return 1
	}
	return f.dense[i]
}

// materialize allocates the dense slice and backfills `count` implicit 1s for
// the positions that were, until now, all frequency 1.
func (f *termFreqs) materialize(count int) {
	if f.materialized {
		return
	}
	f.dense = append(f.dense[:0], make([]uint32, count)...)
	for i := range f.dense {
		f.dense[i] = 1
	}
	f.materialized = true
}

// append records the frequency for the next position. count is the number of
// positions already stored (the index this entry will occupy). This is the
// indexing hot path: a frequency of 1 on an all-ones list is a no-op.
func (f *termFreqs) append(count int, freq uint32) {
	if !f.materialized {
		if freq == 1 {
			return
		}
		f.materialize(count)
	}
	f.dense = append(f.dense, freq)
}

// removeAt deletes the entry at position i, shifting later entries left.
func (f *termFreqs) removeAt(i int) {
	if !f.materialized {
		return
	}
	f.dense = append(f.dense[:i], f.dense[i+1:]...)
}

// setAll replaces the contents with the given per-position frequencies. It
// stays unmaterialized (all-ones) when every frequency is 1.
func (f *termFreqs) setAll(freqs []uint32) {
	for _, v := range freqs {
		if v != 1 {
			f.dense = append(f.dense[:0], freqs...)
			f.materialized = true
			return
		}
	}
	f.clear()
}

func (f *termFreqs) clear() {
	f.dense = f.dense[:0]
	f.materialized = false
}

// any reports whether any position has a frequency other than 1.
func (f *termFreqs) any() bool {
	for _, v := range f.dense {
		if v != 1 {
			return true
		}
	}
	return false
}

// countedList is the shared sorted (doc ID, term frequency) representation
// behind BuilderList and MergeList. It owns all read and serialization logic so
// the two mutable wrappers can't drift apart. Doc IDs are kept sorted
// ascending; a single-element list stores its only ID in `first` without
// allocating `ids`, which is the common case for rare ngrams. `ids` is
// populated only when count >= 2.
type countedList struct {
	first      uint64
	ids        []uint64
	freqs      termFreqs
	last       uint64
	count      int
	serialized []byte
}

func (l *countedList) idAt(i int) uint64 {
	if l.count == 1 {
		return l.first
	}
	return l.ids[i]
}

func (l *countedList) frequencyAt(i int) uint32 {
	return l.freqs.at(i)
}

// indexOf returns the position of id in the (sorted) list and whether it is
// present.
func (l *countedList) indexOf(id uint64) (int, bool) {
	switch l.count {
	case 0:
		return 0, false
	case 1:
		if l.first == id {
			return 0, true
		}
		return 0, false
	default:
		i := sort.Search(len(l.ids), func(i int) bool {
			return l.ids[i] >= id
		})
		if i < len(l.ids) && l.ids[i] == id {
			return i, true
		}
		return 0, false
	}
}

func (l *countedList) GetCardinality() uint64 {
	return uint64(l.count)
}

func (l *countedList) ToArray() []uint64 {
	switch l.count {
	case 0:
		return []uint64{}
	case 1:
		return []uint64{l.first}
	default:
		out := make([]uint64, len(l.ids))
		copy(out, l.ids)
		return out
	}
}

func (l *countedList) Frequency(id uint64) uint32 {
	i, ok := l.indexOf(id)
	if !ok {
		return 0
	}
	return l.freqs.at(i)
}

func (l *countedList) Iterator() roaring64.IntPeekable64 {
	return l.toRoaring().Iterator()
}

func (l *countedList) Clear() {
	l.first = 0
	l.ids = l.ids[:0]
	l.freqs.clear()
	l.last = 0
	l.count = 0
	l.serialized = nil
}

// setFromSorted resets l to the given ascending doc IDs and parallel
// frequencies. freqs may be nil, in which case every frequency defaults to 1.
// The input slices are copied; l does not retain them.
func (l *countedList) setFromSorted(ids []uint64, freqs []uint32) {
	l.Clear()
	if len(ids) == 0 {
		return
	}
	l.first = ids[0]
	l.last = ids[len(ids)-1]
	l.count = len(ids)
	if len(ids) > 1 {
		l.ids = append(l.ids, ids...)
	}
	if freqs != nil {
		l.freqs.setAll(freqs)
	}
}

// Remove deletes id from the list, preserving the frequencies of surviving
// docs. It lives on the shared core because both building (dropping a stale doc
// during a same-batch update) and merge-time delete compaction need it.
func (l *countedList) Remove(id uint64) {
	switch l.count {
	case 0:
		return
	case 1:
		if l.first == id {
			l.Clear()
		}
		return
	}
	i := sort.Search(len(l.ids), func(i int) bool {
		return l.ids[i] >= id
	})
	if i >= len(l.ids) || l.ids[i] != id {
		return
	}
	l.freqs.removeAt(i)
	l.ids = append(l.ids[:i], l.ids[i+1:]...)
	l.count = len(l.ids)
	l.serialized = nil
	if l.count == 1 {
		// Collapse back to the single-element (first-only) form.
		l.first = l.ids[0]
		l.last = l.first
		l.ids = l.ids[:0]
		return
	}
	l.first = l.ids[0]
	l.last = l.ids[len(l.ids)-1]
}

func (l *countedList) toRoaring() *roaring64.Bitmap {
	bm := roaring64.New()
	switch l.count {
	case 0:
	case 1:
		bm.Add(l.first)
	default:
		bm.AddMany(l.ids)
	}
	return bm
}

func (l *countedList) GetSerializedSizeInBytes() uint64 {
	if err := l.ensureSerialized(); err != nil {
		return 0
	}
	return uint64(len(l.serialized))
}

func (l *countedList) MarshalInto(buf []byte) error {
	if err := l.ensureSerialized(); err != nil {
		return err
	}
	if cap(buf) < len(l.serialized) {
		return fmt.Errorf("buffer too small: got capacity %d, need %d", cap(buf), len(l.serialized))
	}
	copy(buf[:len(l.serialized)], l.serialized)
	return nil
}

func (l *countedList) Marshal() ([]byte, error) {
	if err := l.ensureSerialized(); err != nil {
		return nil, err
	}
	out := make([]byte, len(l.serialized))
	copy(out, l.serialized)
	return out, nil
}

func (l *countedList) ensureSerialized() error {
	if l.serialized != nil {
		return nil
	}
	bm := l.toRoaring()
	roaringBuf := make([]byte, 0, int(bm.GetSerializedSizeInBytes()))
	stream := bytes.NewBuffer(roaringBuf)
	if _, err := bm.WriteTo(stream); err != nil {
		return err
	}
	roaringBuf = stream.Bytes()
	if !l.freqs.any() {
		l.serialized = roaringBuf
		return nil
	}
	l.serialized = l.marshalRLECounts(roaringBuf, l.rleCountSize())
	return nil
}

func (l *countedList) marshalRLECounts(roaringBuf []byte, countBytes int) []byte {
	buf := make([]byte, 0, len(countedListMagic)+binary.MaxVarintLen64+len(roaringBuf)+countBytes)
	buf = append(buf, countedListMagic...)
	buf = binary.AppendUvarint(buf, uint64(len(roaringBuf)))
	buf = append(buf, roaringBuf...)
	for i := 0; i < l.count; {
		v := l.frequencyAt(i)
		j := i + 1
		for j < l.count && l.frequencyAt(j) == v {
			j++
		}
		buf = binary.AppendUvarint(buf, uint64(j-i))
		buf = binary.AppendUvarint(buf, uint64(v))
		i = j
	}
	return buf
}

// rleCountSize returns the byte length of the RLE-encoded frequency tail that
// marshalRLECounts would produce. Used to pre-size the output buffer.
func (l *countedList) rleCountSize() int {
	n := 0
	for i := 0; i < l.count; {
		v := l.frequencyAt(i)
		j := i + 1
		for j < l.count && l.frequencyAt(j) == v {
			j++
		}
		n += uvarintLen64(uint64(j-i)) + uvarintLen64(uint64(v))
		i = j
	}
	return n
}

// BuilderList is a posting list optimized for the indexing build path. It
// assumes doc IDs are usually added in increasing order, avoiding roaring
// container maintenance in the indexing hot loop. It is build-only: it supports
// Add/AddWithFrequency but not the boolean set operations. Merging and delete
// compaction use MergeList instead.
type BuilderList struct {
	countedList
}

func (l *BuilderList) Add(id uint64) {
	l.AddWithFrequency(id, 1)
}

// AddWithFrequency appends id with the given term frequency. Doc IDs must be
// added in strictly increasing order — this is the only thing the indexing path
// does, and it lets the build stay a branch-light append. Adding an out-of-order
// or duplicate id panics, since either would silently corrupt the sorted ids.
func (l *BuilderList) AddWithFrequency(id uint64, freq uint32) {
	if freq == 0 {
		freq = 1
	}
	if l.count > 0 && id <= l.last {
		panic(fmt.Sprintf("BuilderList ids must be strictly increasing: got %d after %d", id, l.last))
	}
	l.serialized = nil
	l.freqs.append(l.count, freq)
	switch l.count {
	case 0:
		l.first = id
	case 1:
		l.ids = append(l.ids, l.first, id)
	default:
		l.ids = append(l.ids, id)
	}
	l.last = id
	l.count++
}

// MergeList is a mutable posting list used by the pebble value merger and by
// delete compaction. Unlike BuilderList it supports Or (merging another list,
// summing shared frequencies) and Remove, but not the indexing Add path.
type MergeList struct {
	countedList
}

// Or merges other into l, summing frequencies for shared doc IDs.
func (l *MergeList) Or(other ReadOnlyList) {
	rIDs := other.ToArray()
	if len(rIDs) == 0 {
		return
	}
	rFreqs := frequenciesInOrder(other, rIDs)
	if l.count == 0 {
		l.setFromSorted(rIDs, rFreqs)
		return
	}

	ids := make([]uint64, 0, l.count+len(rIDs))
	freqs := make([]uint32, 0, l.count+len(rIDs))
	i, j := 0, 0
	for i < l.count && j < len(rIDs) {
		leftID := l.idAt(i)
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
	for ; i < l.count; i++ {
		ids = append(ids, l.idAt(i))
		freqs = append(freqs, l.frequencyAt(i))
	}
	for ; j < len(rIDs); j++ {
		ids = append(ids, rIDs[j])
		freqs = append(freqs, rFreqs[j])
	}
	l.setFromSorted(ids, freqs)
}

// frequenciesInOrder returns the per-doc frequencies of l in the same order as
// ids (which must be l.ToArray()). For a counted read-only list it returns the
// decoded frequency slice directly; otherwise every present doc has frequency 1.
func frequenciesInOrder(l ReadOnlyList, ids []uint64) []uint32 {
	if c, ok := l.(*countedReadOnlyList); ok {
		return c.freqs
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
	if cap(buf) < len(w.serialized) {
		return fmt.Errorf("buffer too small: got capacity %d, need %d", cap(buf), len(w.serialized))
	}
	copy(buf[:len(w.serialized)], w.serialized)
	return nil
}

func (w *countedReadOnlyList) Marshal() ([]byte, error) {
	out := make([]byte, len(w.serialized))
	copy(out, w.serialized)
	return out, nil
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

// NewBuilderList returns a build-only posting list optimized for the indexing
// path. See BuilderList.
func NewBuilderList(ids ...uint64) *BuilderList {
	pl := &BuilderList{}
	for _, id := range ids {
		pl.Add(id)
	}
	return pl
}

// NewMergeList returns an empty posting list for the merge / compaction path.
// See MergeList.
func NewMergeList() *MergeList {
	return &MergeList{}
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

// uvarintLen64 returns the number of bytes that binary.PutUvarint would write
// for v, without actually allocating a buffer. Used to size the frequency
// payload before allocating, and to estimate on-disk cost for profiling.
func uvarintLen64(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n++
	}
	return n
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
	ml := NewMergeList()
	if isCountedList(buf) {
		roList, err := unmarshalCountedReadOnly(buf)
		if err != nil {
			return nil, err
		}
		ml.setFromSorted(roList.ToArray(), roList.freqs)
		return ml, nil
	}

	roList, err := UnmarshalReadOnly(buf)
	if err != nil {
		return nil, err
	}
	ml.setFromSorted(roList.ToArray(), nil)
	return ml, nil
}

// UnmarshalReadOnly unmarshals a posting list from a byte slice without copying
// the underlying data. Important: buf must remain valid for the lifetime of the returned
// ReadOnlyList.
func UnmarshalReadOnly(buf []byte) (ReadOnlyList, error) {
	if isCountedList(buf) {
		return unmarshalCountedReadOnly(buf)
	}
	pl := roaring64.New()
	n, err := pl.FromUnsafeBytes(buf)
	if err != nil {
		return nil, err
	}
	if n < int64(len(buf)) {
		return nil, fmt.Errorf("read only %d bytes of buffer with size %d", n, len(buf))
	}
	return &roaringWrapper{pl}, nil
}

func isCountedList(buf []byte) bool {
	return bytes.HasPrefix(buf, countedListMagic)
}

func unmarshalCountedReadOnly(buf []byte) (*countedReadOnlyList, error) {
	if !bytes.HasPrefix(buf, countedListMagic) {
		return nil, fmt.Errorf("unknown counted posting list format")
	}
	pl, countBuf, err := unmarshalCountedRoaring(buf[len(countedListMagic):])
	if err != nil {
		return nil, err
	}
	cardinality := pl.GetCardinality()
	if cardinality > uint64(int(^uint(0)>>1)) {
		return nil, fmt.Errorf("counted posting list cardinality %d overflows int", cardinality)
	}
	freqs := make([]uint32, 0, int(cardinality))
	for len(countBuf) > 0 {
		runlen, n := binary.Uvarint(countBuf)
		if n <= 0 || runlen == 0 {
			return nil, fmt.Errorf("invalid counted posting list run length")
		}
		countBuf = countBuf[n:]
		value, n := binary.Uvarint(countBuf)
		if n <= 0 {
			return nil, fmt.Errorf("invalid counted posting list frequency")
		}
		if value > uint64(^uint32(0)) {
			return nil, fmt.Errorf("posting list frequency %d overflows uint32", value)
		}
		countBuf = countBuf[n:]
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
	return &countedReadOnlyList{
		bm:         pl,
		freqs:      freqs,
		serialized: buf,
	}, nil
}

func unmarshalCountedRoaring(rest []byte) (*roaring64.Bitmap, []byte, error) {
	roaringLen, n := binary.Uvarint(rest)
	if n <= 0 {
		return nil, nil, fmt.Errorf("invalid counted posting list roaring length")
	}
	rest = rest[n:]
	if roaringLen > uint64(len(rest)) {
		return nil, nil, fmt.Errorf("counted posting list roaring length %d exceeds remaining buffer %d", roaringLen, len(rest))
	}
	roaringBuf := rest[:roaringLen:roaringLen]
	rest = rest[roaringLen:]

	pl := roaring64.New()
	read, err := pl.FromUnsafeBytes(roaringBuf)
	if err != nil {
		return nil, nil, err
	}
	if read < int64(len(roaringBuf)) {
		return nil, nil, fmt.Errorf("read only %d bytes of roaring buffer with size %d", read, len(roaringBuf))
	}
	return pl, rest, nil
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
		newPl := NewList()
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
	pl := NewList()
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
