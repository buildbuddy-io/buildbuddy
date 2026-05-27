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

type BuilderList struct {
	first      uint64
	firstFreq  uint32
	ids        []uint64
	freqs      []uint32
	last       uint64
	count      int
	serialized []byte
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

// NewBuilderList returns a posting list optimized for the indexing path.
// It assumes doc IDs are usually added in increasing order, avoiding roaring
// container maintenance in the indexing hot loop. Boolean operations preserve
// frequencies, but appending sorted doc IDs is the intended fast path.
func NewBuilderList(ids ...uint64) *BuilderList {
	pl := &BuilderList{}
	for _, id := range ids {
		pl.Add(id)
	}
	return pl
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

func (l *BuilderList) Add(id uint64) {
	l.AddWithFrequency(id, 1)
}

func (l *BuilderList) AddWithFrequency(id uint64, freq uint32) {
	if freq == 0 {
		freq = 1
	}
	if l.count > 0 && id == l.last {
		l.addFrequencyAt(l.count-1, freq)
		return
	}
	if l.count > 0 && id < l.last {
		freqs := l.toFrequencyMap()
		freqs[id] += freq
		l.setFromFrequencyMap(freqs)
		return
	}
	l.serialized = nil
	switch l.count {
	case 0:
		l.first = id
		l.firstFreq = freq
	case 1:
		l.ids = append(l.ids, l.first, id)
		l.freqs = append(l.freqs, l.firstFreq, freq)
	default:
		l.ids = append(l.ids, id)
		l.freqs = append(l.freqs, freq)
	}
	l.last = id
	l.count++
}

func (l *BuilderList) addFrequencyAt(idx int, freq uint32) {
	if freq == 0 {
		return
	}
	l.serialized = nil
	if l.count == 1 {
		l.firstFreq += freq
		return
	}
	l.freqs[idx] += freq
}

func (l *BuilderList) idAt(i int) uint64 {
	if l.count == 1 {
		return l.first
	}
	return l.ids[i]
}

func (l *BuilderList) frequencyAt(i int) uint32 {
	if l.count == 1 {
		return l.firstFreq
	}
	return l.freqs[i]
}

func (l *BuilderList) setFromSortedSlices(ids []uint64, freqs []uint32) {
	l.Clear()
	if len(ids) == 0 {
		return
	}
	l.first = ids[0]
	l.firstFreq = freqs[0]
	l.last = ids[len(ids)-1]
	l.count = len(ids)
	if len(ids) > 1 {
		l.ids = ids
		l.freqs = freqs
	}
}

func (l *BuilderList) copyFrom(other *BuilderList) {
	l.first = other.first
	l.firstFreq = other.firstFreq
	l.last = other.last
	l.count = other.count
	l.serialized = nil
	l.ids = l.ids[:0]
	l.freqs = l.freqs[:0]
	if other.count > 1 {
		l.ids = append(l.ids, other.ids...)
		l.freqs = append(l.freqs, other.freqs...)
	}
}

func (l *BuilderList) orBuilder(other *BuilderList) {
	if other.count == 0 {
		return
	}
	if l.count == 0 {
		l.copyFrom(other)
		return
	}

	ids := make([]uint64, 0, l.count+other.count)
	freqs := make([]uint32, 0, l.count+other.count)
	i, j := 0, 0
	for i < l.count && j < other.count {
		leftID := l.idAt(i)
		rightID := other.idAt(j)
		switch {
		case leftID < rightID:
			ids = append(ids, leftID)
			freqs = append(freqs, l.frequencyAt(i))
			i++
		case rightID < leftID:
			ids = append(ids, rightID)
			freqs = append(freqs, other.frequencyAt(j))
			j++
		default:
			ids = append(ids, leftID)
			freqs = append(freqs, l.frequencyAt(i)+other.frequencyAt(j))
			i++
			j++
		}
	}
	for ; i < l.count; i++ {
		ids = append(ids, l.idAt(i))
		freqs = append(freqs, l.frequencyAt(i))
	}
	for ; j < other.count; j++ {
		ids = append(ids, other.idAt(j))
		freqs = append(freqs, other.frequencyAt(j))
	}
	l.setFromSortedSlices(ids, freqs)
}

func (l *BuilderList) Remove(id uint64) {
	freqs := l.toFrequencyMap()
	delete(freqs, id)
	l.setFromFrequencyMap(freqs)
}

func (l *BuilderList) Clear() {
	l.first = 0
	l.firstFreq = 0
	l.ids = l.ids[:0]
	l.freqs = l.freqs[:0]
	l.last = 0
	l.count = 0
	l.serialized = nil
}

// Or merges other into l, summing frequencies for shared doc IDs. The hot
// path — the pebble value merger — always passes another *BuilderList here,
// so that case is open-coded. Other input types fall through to a generic
// (slower) path that iterates other via the ReadOnlyList interface.
func (l *BuilderList) Or(other ReadOnlyList) {
	if other, ok := other.(*BuilderList); ok {
		l.orBuilder(other)
		return
	}
	freqs := l.toFrequencyMap()
	it := other.Iterator()
	for it.HasNext() {
		id := it.Next()
		freqs[id] += other.Frequency(id)
	}
	l.setFromFrequencyMap(freqs)
}

// And intersects l with other, keeping l's frequencies for surviving doc IDs.
// Not exercised on any production hot path; correctness over speed.
func (l *BuilderList) And(other ReadOnlyList) {
	freqs := l.toFrequencyMap()
	for id := range freqs {
		if other.Frequency(id) == 0 {
			delete(freqs, id)
		}
	}
	l.setFromFrequencyMap(freqs)
}

// AndNot removes any doc IDs in other from l, keeping l's frequencies for
// surviving doc IDs. Not exercised on any production hot path; CompactDeletes
// uses a Remove loop instead.
func (l *BuilderList) AndNot(other ReadOnlyList) {
	freqs := l.toFrequencyMap()
	for id := range freqs {
		if other.Frequency(id) != 0 {
			delete(freqs, id)
		}
	}
	l.setFromFrequencyMap(freqs)
}

func (l *BuilderList) GetCardinality() uint64 {
	return uint64(l.count)
}

func (l *BuilderList) ToArray() []uint64 {
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

func (l *BuilderList) Frequency(id uint64) uint32 {
	switch l.count {
	case 0:
		return 0
	case 1:
		if l.first == id {
			return l.firstFreq
		}
		return 0
	default:
		i := sort.Search(len(l.ids), func(i int) bool {
			return l.ids[i] >= id
		})
		if i < len(l.ids) && l.ids[i] == id {
			return l.freqs[i]
		}
		return 0
	}
}

func (l *BuilderList) Iterator() roaring64.IntPeekable64 {
	return l.toRoaring().Iterator()
}

func (l *BuilderList) GetSerializedSizeInBytes() uint64 {
	if err := l.ensureSerialized(); err != nil {
		return 0
	}
	return uint64(len(l.serialized))
}

func (l *BuilderList) MarshalInto(buf []byte) error {
	if err := l.ensureSerialized(); err != nil {
		return err
	}
	if cap(buf) < len(l.serialized) {
		return fmt.Errorf("buffer too small: got capacity %d, need %d", cap(buf), len(l.serialized))
	}
	copy(buf[:len(l.serialized)], l.serialized)
	return nil
}

func (l *BuilderList) Marshal() ([]byte, error) {
	if err := l.ensureSerialized(); err != nil {
		return nil, err
	}
	out := make([]byte, len(l.serialized))
	copy(out, l.serialized)
	return out, nil
}

func (l *BuilderList) ensureSerialized() error {
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
	if !l.hasNonOneFrequency() {
		l.serialized = roaringBuf
		return nil
	}
	l.serialized = l.marshalRLECounts(roaringBuf, l.rleCountSize())
	return nil
}

func (l *BuilderList) marshalRLECounts(roaringBuf []byte, countBytes int) []byte {
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

func (l *BuilderList) hasNonOneFrequency() bool {
	switch l.count {
	case 0:
		return false
	case 1:
		return l.firstFreq != 1
	default:
		for _, freq := range l.freqs {
			if freq != 1 {
				return true
			}
		}
		return false
	}
}

// rleCountSize returns the byte length of the RLE-encoded frequency tail that
// marshalRLECounts would produce. Used to pre-size the output buffer.
func (l *BuilderList) rleCountSize() int {
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

func (l *BuilderList) toRoaring() *roaring64.Bitmap {
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

func (l *BuilderList) setFromRoaring(bm *roaring64.Bitmap) {
	l.Clear()
	ids := bm.ToArray()
	if len(ids) == 0 {
		return
	}
	l.first = ids[0]
	l.firstFreq = 1
	l.last = ids[len(ids)-1]
	l.count = len(ids)
	if len(ids) > 1 {
		l.ids = append(l.ids, ids...)
		for range ids {
			l.freqs = append(l.freqs, 1)
		}
	}
}

func (l *BuilderList) toFrequencyMap() map[uint64]uint32 {
	freqs := make(map[uint64]uint32, l.count)
	switch l.count {
	case 0:
	case 1:
		freqs[l.first] = l.firstFreq
	default:
		for i, id := range l.ids {
			freqs[id] = l.freqs[i]
		}
	}
	return freqs
}

func (l *BuilderList) setFromFrequencyMap(freqs map[uint64]uint32) {
	l.Clear()
	if len(freqs) == 0 {
		return
	}
	ids := make([]uint64, 0, len(freqs))
	for id := range freqs {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})
	l.first = ids[0]
	l.firstFreq = freqs[ids[0]]
	l.last = ids[len(ids)-1]
	l.count = len(ids)
	if len(ids) > 1 {
		l.ids = append(l.ids, ids...)
		for _, id := range ids {
			l.freqs = append(l.freqs, freqs[id])
		}
	}
}

func readOnlyListToRoaring(l ReadOnlyList) *roaring64.Bitmap {
	if bm, ok := l.(*roaringWrapper); ok {
		return bm.Bitmap
	}
	if bm, ok := l.(*countedReadOnlyList); ok {
		return bm.bm
	}
	if bm, ok := l.(*BuilderList); ok {
		return bm.toRoaring()
	}
	bm := roaring64.New()
	bm.AddMany(l.ToArray())
	return bm
}

func Unmarshal(buf []byte) (List, error) {
	pl := &BuilderList{}
	if isCountedList(buf) {
		roList, err := unmarshalCountedReadOnly(buf)
		if err != nil {
			return nil, err
		}
		ids := roList.ToArray()
		if len(ids) == 0 {
			return pl, nil
		}
		pl.first = ids[0]
		pl.firstFreq = roList.freqs[0]
		pl.last = ids[len(ids)-1]
		pl.count = len(ids)
		if len(ids) > 1 {
			pl.ids = append(pl.ids, ids...)
			pl.freqs = append(pl.freqs, roList.freqs...)
		}
		return pl, nil
	}

	roList, err := UnmarshalReadOnly(buf)
	if err != nil {
		return nil, err
	}
	pl.setFromRoaring(readOnlyListToRoaring(roList))
	return pl, nil
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
