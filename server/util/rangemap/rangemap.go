package rangemap

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

var (
	RangeOverlapError      = errors.New("Range overlap")
	RangeDoesNotExistError = errors.New("Range does not exist")
)

// Ranges are [inclusive,exclusive)
type Range struct {
	Start []byte
	End   []byte

	Val interface{}
}

func (r *Range) String() string {
	return fmt.Sprintf("[%s, %s)", string(r.Start), string(r.End))
}

func (r *Range) Contains(key []byte) bool {
	// bytes.Compare(a,b) does:
	//  0 if a==b, -1 if a < b, and +1 if a > b
	greaterThanOrEqualToStart := bytes.Compare(key, r.Start) > -1
	lessThanEnd := bytes.Compare(key, r.End) == -1

	contained := greaterThanOrEqualToStart && lessThanEnd
	return contained
}

type RangeMap struct {
	ranges []*Range
}

func New() *RangeMap {
	return &RangeMap{
		ranges: make([]*Range, 0),
	}
}

func (rm *RangeMap) Add(start, end []byte, value interface{}) (*Range, error) {
	insertIndex := sort.Search(len(rm.ranges), func(i int) bool {
		//  0 if a==b, -1 if a < b, and +1 if a > b
		c := bytes.Compare(rm.ranges[i].Start, end)
		b := c >= 0
		return b
	})

	// if we're inserting anywhere but the very beginning, ensure that
	// we don't overlap with the range before us.
	prevRangeIndex := insertIndex - 1
	if len(rm.ranges) > 0 && prevRangeIndex >= 0 {
		if bytes.Compare(rm.ranges[prevRangeIndex].End, start) > 0 {
			return nil, RangeOverlapError
		}
	}

	newRange := &Range{
		Start: start,
		End:   end,
		Val:   value,
	}

	if insertIndex >= len(rm.ranges) {
		rm.ranges = append(rm.ranges, newRange)
	} else {
		rm.ranges = append(rm.ranges[:insertIndex+1], rm.ranges[insertIndex:]...)
		rm.ranges[insertIndex] = newRange
	}
	log.Debugf("Rangemap added new range: %s", newRange)
	return newRange, nil
}

func (rm *RangeMap) Remove(start, end []byte) error {
	deleteIndex := -1
	for i, r := range rm.ranges {
		if bytes.Equal(start, r.Start) && bytes.Equal(end, r.End) {
			deleteIndex = i
			break
		}
	}
	if deleteIndex == -1 {
		return RangeDoesNotExistError
	}
	rm.ranges = append(rm.ranges[:deleteIndex], rm.ranges[deleteIndex+1:]...)
	return nil
}

func (rm *RangeMap) Get(start, end []byte) *Range {
	if len(rm.ranges) == 0 {
		return nil
	}

	// Search returns the smallest i for which func returns true.
	// We want the smallest range that is bigger than this key
	// aka, starts AFTER this key, and then we'll go one start of it
	i := sort.Search(len(rm.ranges), func(i int) bool {
		//  0 if a==b, -1 if a < b, and +1 if a > b
		return bytes.Compare(rm.ranges[i].Start, start) > 0
	})

	// This is safe anyway because of how sort.Search works, but
	// be clear so readers see this won't hit an out of range panic.
	if i > 0 {
		i -= 1
	}

	r := rm.ranges[i]
	startEq := bytes.Equal(r.Start, start)
	endEq := bytes.Equal(r.End, end)
	if startEq && endEq {
		return r
	}
	return nil
}

func (rm *RangeMap) GetOverlapping(start, end []byte) []*Range {
	if len(rm.ranges) == 0 {
		return nil
	}
	// Search returns the smallest i for which func returns true.
	// We want the smallest range that is bigger than this key
	// aka, starts AFTER this key, and then we'll go one start of it
	startIndex := sort.Search(len(rm.ranges), func(i int) bool {
		//  0 if a==b, -1 if a < b, and +1 if a > b
		return bytes.Compare(rm.ranges[i].Start, start) > 0
	})

	if startIndex > 0 && rm.ranges[startIndex-1].Contains(start) {
		startIndex -= 1
	}

	// Search returns the smallest i for which func returns true.
	// We want the smallest range that is bigger than this key
	// aka, starts AFTER this key, and then we'll go one start of it
	endIndex := sort.Search(len(rm.ranges), func(i int) bool {
		//  0 if a==b, -1 if a < b, and +1 if a > b
		return bytes.Compare(rm.ranges[i].Start, end) >= 0
	})

	if endIndex == 0 {
		return nil
	}
	return rm.ranges[startIndex:endIndex]
}

func (rm *RangeMap) Lookup(key []byte) interface{} {
	if len(rm.ranges) == 0 {
		return nil
	}

	// Search returns the smallest i for which func returns true.
	// We want the smallest range that is bigger than this key
	// aka, starts AFTER this key, and then we'll go one start of it
	i := sort.Search(len(rm.ranges), func(i int) bool {
		//  0 if a==b, -1 if a < b, and +1 if a > b
		return bytes.Compare(rm.ranges[i].Start, key) > 0
	})

	// This is safe anyway because of how sort.Search works, but
	// be clear so readers see this won't hit an out of range panic.
	if i > 0 {
		i -= 1
	}
	if rm.ranges[i].Contains(key) {
		return rm.ranges[i].Val
	}

	return nil

}

func (rm *RangeMap) String() string {
	buf := "RangeMap:\n"
	for i, r := range rm.ranges {
		buf += r.String()
		if i != len(rm.ranges)-1 {
			buf += "\n"
		}
	}
	return buf
}

func (rm *RangeMap) Ranges() []*Range {
	return rm.ranges
}

func (rm *RangeMap) Clear() {
	rm.ranges = make([]*Range, 0)
}
