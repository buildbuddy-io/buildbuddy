package posting

import (
	"bytes"
	"fmt"

	"github.com/RoaringBitmap/roaring/roaring64"
)

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
	ids        []uint64
	last       uint64
	count      int
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
// container maintenance in the indexing hot loop. Boolean operations convert
// through roaring and are not the intended fast path.
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
	if l.count > 0 && id == l.last {
		return
	}
	if l.count > 0 && id < l.last {
		bm := l.toRoaring()
		bm.Add(id)
		l.setFromRoaring(bm)
		return
	}
	l.serialized = nil
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

func (l *BuilderList) Remove(id uint64) {
	bm := l.toRoaring()
	bm.Remove(id)
	l.setFromRoaring(bm)
}

func (l *BuilderList) Clear() {
	l.first = 0
	l.ids = l.ids[:0]
	l.last = 0
	l.count = 0
	l.serialized = nil
}

func (l *BuilderList) Or(other ReadOnlyList) {
	bm := l.toRoaring()
	bm.Or(readOnlyListToRoaring(other))
	l.setFromRoaring(bm)
}

func (l *BuilderList) And(other ReadOnlyList) {
	bm := l.toRoaring()
	bm.And(readOnlyListToRoaring(other))
	l.setFromRoaring(bm)
}

func (l *BuilderList) AndNot(other ReadOnlyList) {
	bm := l.toRoaring()
	bm.AndNot(readOnlyListToRoaring(other))
	l.setFromRoaring(bm)
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
	stream := bytes.NewBuffer(make([]byte, 0, int(bm.GetSerializedSizeInBytes())))
	if _, err := bm.WriteTo(stream); err != nil {
		return err
	}
	l.serialized = stream.Bytes()
	return nil
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
	l.last = ids[len(ids)-1]
	l.count = len(ids)
	if len(ids) > 1 {
		l.ids = append(l.ids, ids...)
	}
}

func readOnlyListToRoaring(l ReadOnlyList) *roaring64.Bitmap {
	if bm, ok := l.(*roaringWrapper); ok {
		return bm.Bitmap
	}
	if bm, ok := l.(*BuilderList); ok {
		return bm.toRoaring()
	}
	bm := roaring64.New()
	bm.AddMany(l.ToArray())
	return bm
}

func Unmarshal(buf []byte) (List, error) {
	readStream := bytes.NewReader(buf)
	pl := roaring64.New()
	n, err := pl.ReadFrom(readStream)
	if err != nil {
		return nil, err
	}
	if n != int64(len(buf)) {
		return nil, fmt.Errorf("read only %d bytes of buffer with size %d", n, len(buf))
	}
	return &roaringWrapper{pl}, nil
}

// UnmarshalReadOnly unmarshals a posting list from a byte slice without copying
// the underlying data. Important: buf must remain valid for the lifetime of the returned
// ReadOnlyList.
func UnmarshalReadOnly(buf []byte) (ReadOnlyList, error) {
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
