package posting

import (
	"bytes"

	"github.com/RoaringBitmap/roaring/roaring64"
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
	bm.AddMany(ids)
	return &roaringWrapper{bm}
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
