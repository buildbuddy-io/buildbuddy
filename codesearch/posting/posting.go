package posting

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/cockroachdb/pebble"
)

type List interface {
	Or(List)
	And(List)
	Add(uint64)
	Remove(uint64)
	GetCardinality() uint64
	ToArray() []uint64
	Clear()
}

type roaringWrapper struct {
	*roaring64.Bitmap
}

func (w *roaringWrapper) Or(l List) {
	bm, ok := l.(*roaringWrapper)
	if !ok {
		panic("not roaringWrapper")
	}
	w.Bitmap.Or(bm.Bitmap)
}
func (w *roaringWrapper) And(l List) {
	bm, ok := l.(*roaringWrapper)
	if !ok {
		panic("not roaringWrapper")
	}
	w.Bitmap.And(bm.Bitmap)
}

func NewList(ids ...uint64) List {
	bm := roaring64.New()
	if len(ids) > 0 {
		bm.AddMany(ids)
	}
	return &roaringWrapper{bm}
}

func GetSerializedSizeInBytes(pl List) int {
	bm, ok := pl.(*roaringWrapper)
	if !ok {
		panic("not roaringWrapper")
	}
	return int(bm.GetSerializedSizeInBytes())
}

func MarshalInto(pl List, buf []byte) error {
	bm, ok := pl.(*roaringWrapper)
	if !ok {
		panic("not roaringWrapper")
	}
	stream := bytes.NewBuffer(buf)
	_, err := bm.Bitmap.WriteTo(stream)
	return err
}

func Marshal(pl List) ([]byte, error) {
	bm, ok := pl.(*roaringWrapper)
	if !ok {
		panic("not roaringWrapper")
	}
	stream := bytes.NewBuffer(make([]byte, 0, int(bm.GetSerializedSizeInBytes())))
	_, err := bm.Bitmap.WriteTo(stream)
	return stream.Bytes(), err
}

func Unmarshal(buf []byte) (List, error) {
	readStream := bytes.NewReader(buf)
	pl := roaring64.New()
	for len(buf) > 0 {
		plTemp := roaring64.New()
		n, err := plTemp.ReadFrom(readStream)
		if err != nil {
			return nil, err
		}
		buf = buf[n:]
		pl.Or(plTemp)
	}
	return &roaringWrapper{pl}, nil
}

// A fieldMap is a collection of postingLists that are keyed by the field that
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

func (fm *FieldMap) OrField(fieldName string, pl2 List) {
	if pl, ok := (*fm)[fieldName]; ok {
		pl.Or(pl2)
	} else {
		(*fm)[fieldName] = pl2
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
func (fm *FieldMap) Remove(docid uint64) {
	f := (*fm)
	for _, pl := range f {
		pl.Remove(docid)
	}
}

// TODO(jdelfino): probably move to their own file

func unmarshalSingle(buf []byte) (List, uint64, error) {
	if len(buf) == 8 {
		// bare uint64, not a posting list
		return nil, binary.LittleEndian.Uint64(buf), nil
	}

	readStream := bytes.NewReader(buf)
	pl := roaring64.New()
	n, err := pl.ReadFrom(readStream)
	if err != nil {
		return nil, 0, err
	}
	if n != int64(len(buf)) {
		return nil, 0, fmt.Errorf("expected to read %d bytes, but read %d", len(buf), n)
	}
	return &roaringWrapper{pl}, 0, nil
}

func InitRoaringMerger(key, value []byte) (pebble.ValueMerger, error) {
	// TODO(jdelfino): Sanity check key to make sure it is a posting list key?
	pl, docId, err := unmarshalSingle(value)
	if err != nil {
		return nil, err
	}

	if pl == nil {
		pl = NewList(docId)
	}

	return &RoaringValueMerger{
		pl: pl,
	}, nil
}

type RoaringValueMerger struct {
	pl List
}

func mergeInto(pl List, docIdBytes []byte) error {
	newPl, docId, err := unmarshalSingle(docIdBytes)
	if err != nil {
		return err
	}
	if newPl == nil {
		pl.Add(docId)
	} else {
		pl.Or(newPl)
	}
	return nil
}

func (rm *RoaringValueMerger) MergeNewer(value []byte) error {
	return mergeInto(rm.pl, value)
}

func (rm *RoaringValueMerger) MergeOlder(value []byte) error {
	return mergeInto(rm.pl, value)
}

func (rm *RoaringValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	r, err := Marshal(rm.pl)
	return r, nil, err
}
