package posting

import (
	"encoding/binary"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"golang.org/x/exp/maps"
)

type uint64Posting struct {
	uint64
	positions []uint64
}

func (u64p *uint64Posting) Docid() uint64 {
	return u64p.uint64
}
func (u64p *uint64Posting) Positions() []uint64 {
	return u64p.positions
}
func (u64p *uint64Posting) Merge(p types.Posting) {
	u2, ok := p.(*uint64Posting)
	if !ok {
		panic("Mixed posting set types")
	}
	if u64p.uint64 != u2.uint64 {
		panic("Mismatched docids")
	}
	u64p.positions = append(u64p.positions, u2.positions...)
}

// New returns a new Posting.
func New(docID uint64, positions ...uint64) types.Posting {
	return &uint64Posting{docID, positions}
}

type List interface {
	Or(List)
	And(List)
	Add(types.Posting)
	Remove(uint64)
	Marshal() ([]byte, error)
	Unmarshal([]byte) (List, error)
	GetCardinality() uint64
	ToArray() []types.Posting
	Clear()
}

func NewList(ps ...types.Posting) List {
	m := make(map[uint64]types.Posting, len(ps))
	for _, p := range ps {
		m[p.Docid()] = p
	}
	set := uint64PostingSet(m)
	return &set
}

type uint64PostingSet map[uint64]types.Posting

func (ps *uint64PostingSet) Or(l2 List) {
	ps2, ok := l2.(*uint64PostingSet)
	if !ok {
		panic("Mixed posting set types")
	}
	for k := range *ps2 {
		if v1, ok := (*ps)[k]; ok {
			v1.Merge((*ps2)[k])
		} else {
			(*ps)[k] = (*ps2)[k]
		}
	}
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
func (ps *uint64PostingSet) Add(p types.Posting) {
	(*ps)[p.Docid()] = p
}
func (ps *uint64PostingSet) Remove(u uint64) {
	delete(*ps, u)
}
func (ps *uint64PostingSet) Marshal() ([]byte, error) {
	buf := make([]byte, 0, len(*ps)*8)
	for docid, posting := range *ps {
		buf = binary.AppendUvarint(buf, docid)
		buf = binary.AppendUvarint(buf, uint64(len(posting.Positions())))
	}
	return buf, nil
}
func (ps *uint64PostingSet) Unmarshal(buf []byte) (List, error) {
	m := make(map[uint64]types.Posting, 0)
	for len(buf) > 0 {
		docid, n := binary.Uvarint(buf)
		buf = buf[n:]
		numPositions, n := binary.Uvarint(buf)
		buf = buf[n:]

		positions := make([]uint64, 0, int(numPositions))
		m[docid] = &uint64Posting{uint64: docid, positions: positions}
	}
	*ps = m
	return ps, nil
}

func (ps *uint64PostingSet) ToArray() []types.Posting {
	docids := maps.Keys(*ps)
	slices.Sort(docids)

	pls := make([]types.Posting, len(docids))
	for i, docid := range docids {
		pls[i] = New(docid)
	}
	return pls
}

func (ps *uint64PostingSet) GetCardinality() uint64 {
	return uint64(len(*ps))
}
func (ps *uint64PostingSet) Clear() {
	maps.Clear(*ps)
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
