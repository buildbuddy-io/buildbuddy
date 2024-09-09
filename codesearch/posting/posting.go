package posting

import (
	"encoding/binary"
	"github.com/bmkessler/streamvbyte"
	"slices"
	"sort"
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
func (pl *uint64PostingList) GetCardinality() uint64 {
	return uint64(len(*pl))
}
func (pl *uint64PostingList) ToArray() []uint64 {
	return *pl
}
func (pl *uint64PostingList) Clear() {
	*pl = (*pl)[:0]
}

func Marshal(pl List) ([]byte, error) {
	upl, ok := pl.(*uint64PostingList)
	if !ok {
		panic("not uint64PostingList")
	}

	var encoded []byte
	remainingIDs := *upl
	for len(remainingIDs) > 0 {
		generation := uint32(remainingIDs[0] >> 32)

		// walk forward until we encounter an ID with a different top 32
		// bits.
		end := 0
		for end = 0; end < len(remainingIDs); end++ {
			if uint32(remainingIDs[end]>>32) != generation {
				break
			}
		}

		// chop off the top 32 bits of all the IDs and encode that as
		// the "generation".
		ids := make([]uint32, end)
		for i, d := range remainingIDs[:end] {
			ids[i] = uint32(d)
		}
		remainingIDs = remainingIDs[end:]

		// prepare a buffer big enough to hold our IDs.
		buf := make([]byte, streamvbyte.MaxSize32(len(ids))+4+4+4)

		// Encode the length of the list.
		listLength := uint32(len(ids))
		if _, err := binary.Encode(buf[4:8], binary.LittleEndian, listLength); err != nil {
			return nil, err
		}

		// Encode the generation.
		if _, err := binary.Encode(buf[8:12], binary.LittleEndian, generation); err != nil {
			return nil, err
		}

		// Encode the rest of the data.
		d := streamvbyte.EncodeDeltaUint32(buf[12:], ids, 0)

		// Encode the length of the actual buf buffer into the first
		// 4 bytes.
		if _, err := binary.Encode(buf[0:4], binary.LittleEndian, uint32(d)); err != nil {
			return nil, err
		}
		encoded = append(encoded, buf[:d+12]...)
	}
	return encoded, nil
}

func Unmarshal(buf []byte) (List, error) {
	var sections [][]uint64
	for len(buf) > 0 {
		bufLength := uint32(0)
		if _, err := binary.Decode(buf[0:4], binary.LittleEndian, &bufLength); err != nil {
			return nil, err
		}

		listLength := uint32(0)
		if _, err := binary.Decode(buf[4:8], binary.LittleEndian, &listLength); err != nil {
			return nil, err
		}

		generation := uint32(0)
		if _, err := binary.Decode(buf[8:12], binary.LittleEndian, &generation); err != nil {
			return nil, err
		}

		lowerIDs := make([]uint32, listLength)
		streamvbyte.DecodeDeltaUint32(lowerIDs, buf[12:12+bufLength], 0)

		l := make([]uint64, listLength)
		for i, lower := range lowerIDs {
			l[i] = uint64(generation)<<32 | uint64(lower)
		}

		sections = append(sections, l)
		buf = buf[bufLength+12:]
	}
	sort.Slice(sections, func(i, j int) bool {
		return sections[i][0] < sections[j][0]
	})

	l := sections[0]
	for _, s := range sections[1:] {
		l = append(l, s...)
	}
	pl := uint64PostingList(l)
	return &pl, nil
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
