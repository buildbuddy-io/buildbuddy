// Package bitset provides a fixed-length bitset backed by a packed byte slice.
package bitset

import "github.com/buildbuddy-io/buildbuddy/server/util/status"

const (
	bitsPerByte = 8
)

// Set stores a fixed-length sequence of bits in packed form.
//
// The zero value is an empty set with length 0.
type Set struct {
	// n is the number of addressable bits in the set.
	n int
	// data stores the bits packed little-endian within each byte: bit i lives in
	// data[i/8] at bit position i%8.
	data []byte
}

// New returns a Set with the given number of bits.
//
// New returns an error if n is negative.
func New(n int) (*Set, error) {
	if n < 0 {
		return nil, status.InvalidArgumentError("bitset length must be >= 0")
	}
	return &Set{
		n:    n,
		data: make([]byte, bytesForBits(n)),
	}, nil
}

// Set sets the bit at index i to v.
//
// Set returns an error if i is out of range.
func (s *Set) Set(i int, v bool) error {
	byteIndex, mask, err := s.bit(i)
	if err != nil {
		return err
	}
	if v {
		s.data[byteIndex] |= mask
		return nil
	}
	s.data[byteIndex] &^= mask
	return nil
}

// Get reports whether the bit at index i is set.
//
// Get returns an error if i is out of range.
func (s *Set) Get(i int) (bool, error) {
	byteIndex, mask, err := s.bit(i)
	if err != nil {
		return false, err
	}
	return s.data[byteIndex]&mask != 0, nil
}

// Len returns the number of bits in the set.
func (s *Set) Len() int {
	return s.n
}

// Bytes returns the packed byte representation backing the set.
//
// The returned slice aliases the set's storage. Modifying it updates the set
// without allocating.
func (s *Set) Bytes() []byte {
	return s.data
}

func (s *Set) bit(i int) (int, byte, error) {
	if i < 0 || i >= s.n {
		return 0, 0, status.OutOfRangeErrorf("bit index %d out of range [0, %d)", i, s.n)
	}
	return int(i / bitsPerByte), byte(1 << uint(i%bitsPerByte)), nil
}

func bytesForBits(n int) int {
	byteCount := n / bitsPerByte
	if n%bitsPerByte != 0 {
		byteCount++
	}
	return byteCount
}
