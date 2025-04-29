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

type Range struct {
	Start []byte
	End   []byte
}

// RangeWithVal[T] is a data structure that holds [start, end) (i.e. inclusive start and
// exclusive end) and a value.
type RangeWithVal[T any] struct {
	Range

	Val T
}

func (r *Range) String() string {
	return fmt.Sprintf("[%q, %q)", string(r.Start), string(r.End))
}

func (r *Range) Contains(key []byte) bool {
	// bytes.Compare(a,b) does:
	//  0 if a==b, -1 if a < b, and +1 if a > b
	greaterThanOrEqualToStart := bytes.Compare(key, r.Start) > -1
	lessThanEnd := bytes.Compare(key, r.End) == -1

	contained := greaterThanOrEqualToStart && lessThanEnd
	return contained
}

// RangeMap is an ordered map of ranges that supports add, remove, and look up
// Ranges. The ranges in RangeMap are non-overlapping.
type RangeMap[T any] struct {
	ranges []*RangeWithVal[T]
}

func New[T any]() *RangeMap[T] {
	return &RangeMap[T]{
		ranges: make([]*RangeWithVal[T], 0),
	}
}

// Add adds a range to RangeMap. Returns an error if the new range overlap with
// existing ranges.
func (rm *RangeMap[T]) Add(start, end []byte, value T) (*RangeWithVal[T], error) {
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

	newRange := &RangeWithVal[T]{
		Range: Range{
			Start: start,
			End:   end,
		},
		Val: value,
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

// Remove removes a range with the specific start and end.
// Returns RangeDoesNotExistError if the range is not found.
func (rm *RangeMap[T]) Remove(start, end []byte) error {
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

// Get returns the range with the specific start and end. Returns nil if the
// range is not found.
func (rm *RangeMap[T]) Get(start, end []byte) *RangeWithVal[T] {
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

// GetOverlapping returns a list of ranges overlapped with the specific start
// and end.
func (rm *RangeMap[T]) GetOverlapping(start, end []byte) []*RangeWithVal[T] {
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

// Lookup looks up the range containing the key and returns the value of the
// range.
func (rm *RangeMap[T]) Lookup(key []byte) (T, bool) {
	var zero T
	if len(rm.ranges) == 0 {
		return zero, false
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
		return rm.ranges[i].Val, true
	}

	return zero, false

}

func (rm *RangeMap[T]) String() string {
	buf := "RangeMap:\n"
	for i, r := range rm.ranges {
		buf += r.String()
		if i != len(rm.ranges)-1 {
			buf += "\n"
		}
	}
	return buf
}

// Ranges returns a list of ordered ranges that the RangeMap contains.
func (rm *RangeMap[T]) Ranges() []*RangeWithVal[T] {
	return rm.ranges
}

func (rm *RangeMap[T]) Clear() {
	rm.ranges = make([]*RangeWithVal[T], 0)
}
