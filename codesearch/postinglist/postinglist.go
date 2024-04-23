package postinglist

import (
	"encoding/binary"
	"slices"
)

type PostingList interface {
	Or(PostingList)
	And(PostingList)
	Add(uint64)
	Remove(uint64)
	Marshal() ([]byte, error)
	Unmarshal([]byte) (PostingList, error)
	GetCardinality() uint64
	ToArray() []uint64
	Clear()
}

func New(ids ...uint64) PostingList {
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

func (pl *uint64PostingList) Or(pl2 PostingList) {
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
func (pl *uint64PostingList) And(pl2 PostingList) {
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
func (pl *uint64PostingList) Unmarshal(buf []byte) (PostingList, error) {
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
