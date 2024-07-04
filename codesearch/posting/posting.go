package posting

import (
	"encoding/binary"
	"slices"

	"golang.org/x/exp/maps"
)

type List interface {
	Or(List)
	And(List)
	Add(uint64)
	Remove(uint64)
	Marshal() ([]byte, error)
	Unmarshal([]byte) (List, error)
	GetCardinality() uint64
	ToArray() []uint64
	Clear()
}

func NewList(ids ...uint64) List {
	if len(ids) > 0 {
		slices.Sort(ids)
		ids = slices.Compact(ids)
		pl := uint64PostingList(ids)
		return &pl
	}
	n := uint64PostingList(make([]uint64, 0))
	return &n
}

type uint64PostingList []uint64

func (pl *uint64PostingList) Or(pl2 List) {
	var l []uint64
	l1 := *pl
	l2 := pl2.ToArray()
	i := 0
	j := 0
	for i < len(l1) || j < len(l2) {
		switch {
		case j == len(l2) || (i < len(l1) && l1[i] < l2[j]):
			l = append(l, l1[i])
			i++
		case i == len(l1) || (j < len(l2) && l1[i] > l2[j]):
			l = append(l, l2[j])
			j++
		case l1[i] == l2[j]:
			l = append(l, l1[i])
			i++
			j++
		}
	}
	*pl = l
}
func (pl *uint64PostingList) And(pl2 List) {
	var l []uint64
	l1 := *pl
	l2 := pl2.ToArray()
	i := 0
	j := 0
	for i < len(l1) && j < len(l2) {
		switch {
		case l1[i] < l2[j]:
			i++
		case l2[j] < l1[i]:
			j++
		case l1[i] == l2[j]:
			l = append(l, l2[j])
			i++
		}
	}
	*pl = l
}

func (pl *uint64PostingList) Add(u uint64) {
	idx, alreadyPresent := slices.BinarySearch(*pl, u)
	if !alreadyPresent {
		*pl = slices.Insert(*pl, idx, u)
	}
}

func (pl *uint64PostingList) Remove(u uint64) {
	idx, alreadyPresent := slices.BinarySearch(*pl, u)
	if alreadyPresent {
		*pl = slices.Delete(*pl, idx, idx+1)
	}
}
func (pl *uint64PostingList) Marshal() ([]byte, error) {
	buf := make([]byte, 0, len(*pl)*8)
	for i := range *pl {
		buf = binary.AppendUvarint(buf, (*pl)[i])
	}
	return buf, nil
}
func (pl *uint64PostingList) Unmarshal(buf []byte) (List, error) {
	l := make([]uint64, 0)
	for len(buf) > 0 {
		u, n := binary.Uvarint(buf)
		l = append(l, u)
		buf = buf[n:]
	}
	*pl = l
	return pl, nil
}
func (pl *uint64PostingList) GetCardinality() uint64 {
	return uint64(len(*pl))
}
func (pl *uint64PostingList) ToArray() []uint64 {
	return *pl
}
func (pl *uint64PostingList) Clear() {
	*pl = (*pl)[:0]
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
func (fm *FieldMap) Map() map[string][]uint64 {
	m := make(map[string][]uint64)
	for f, pl := range *fm {
		m[f] = pl.ToArray()
	}
	return m
}

type uint64PostingSet map[uint64]struct{}

func (ps *uint64PostingSet) Or(l2 List) {
	ps2, ok := l2.(*uint64PostingSet)
	if !ok {
		panic("Mixed posting set types")
	}
	maps.Copy(*ps, *ps2)
}

func (ps *uint64PostingSet) And(l2 List) {
	ps2, ok := l2.(*uint64PostingSet)
	if !ok {
		panic("Mixed posting set types")
	}
	for k := range *ps {
		if _, ok := (*ps2)[k]; !ok {
			delete(*ps, k)
		}
	}
}
func (ps *uint64PostingSet) Add(u uint64) {
	(*ps)[u] = struct{}{}
}
func (ps *uint64PostingSet) Remove(u uint64) {
	delete(*ps, u)
}
func (ps *uint64PostingSet) Marshal() ([]byte, error) {
	buf := make([]byte, 0, len(*ps)*8)
	for _, u := range (*ps).ToArray() {
		buf = binary.AppendUvarint(buf, u)
	}
	return buf, nil
}
func (ps *uint64PostingSet) Unmarshal(buf []byte) (List, error) {
	m := make(map[uint64]struct{}, 0)
	for len(buf) > 0 {
		u, n := binary.Uvarint(buf)
		m[u] = struct{}{}
		buf = buf[n:]
	}
	*ps = m
	return ps, nil
}
func (ps *uint64PostingSet) GetCardinality() uint64 {
	return uint64(len(*ps))
}
func (ps *uint64PostingSet) ToArray() []uint64 {
	ids := maps.Keys(*ps)
	slices.Sort(ids)
	return ids
}
func (ps *uint64PostingSet) Clear() {
	maps.Clear(*ps)
}
