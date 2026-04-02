package bitset

import (
	"strconv"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
)

var testLengths = []int{0, 1, 7, 8, 9, 16, 17}

func TestNew(t *testing.T) {
	for _, length := range testLengths {
		t.Run("length_"+strconv.Itoa(length), func(t *testing.T) {
			s, err := New(length)
			require.NoError(t, err)
			require.NotNil(t, s)
			require.Equal(t, length, s.Len())
		})
	}

	t.Run("negative length", func(t *testing.T) {
		s, err := New(-1)
		require.Error(t, err)
		require.True(t, status.IsInvalidArgumentError(err))
		require.Nil(t, s)
	})
}

func TestSet(t *testing.T) {
	for _, length := range testLengths {
		t.Run("length_"+strconv.Itoa(length), func(t *testing.T) {
			for i := range length {
				s, err := New(length)
				require.NoError(t, err)

				err = s.Set(i, true)
				require.NoError(t, err)
				requireBitPattern(t, s, length, func(j int) bool {
					return j == i
				})

				require.NoError(t, s.Set(i, false))
				requireBitPattern(t, s, length, func(int) bool {
					return false
				})
			}
		})
	}
}

func TestSet_NegativeIndex(t *testing.T) {
	for _, length := range testLengths {
		t.Run("length_"+strconv.Itoa(length), func(t *testing.T) {
			s, err := New(length)
			require.NoError(t, err)

			err = s.Set(-1, true)
			require.Error(t, err)
			require.True(t, status.IsOutOfRangeError(err))
		})
	}
}

func TestSet_IndexAtLen(t *testing.T) {
	for _, length := range testLengths {
		t.Run("length_"+strconv.Itoa(length), func(t *testing.T) {
			s, err := New(length)
			require.NoError(t, err)

			err = s.Set(length, true)
			require.Error(t, err)
			require.True(t, status.IsOutOfRangeError(err))
		})
	}
}

func TestGet(t *testing.T) {
	for _, length := range testLengths {
		t.Run("length_"+strconv.Itoa(length), func(t *testing.T) {
			s, err := New(length)
			require.NoError(t, err)

			for i := range length {
				require.NoError(t, s.Set(i, i%2 == 1))
			}

			for i := range length {
				got, err := s.Get(i)
				require.NoError(t, err)
				require.Equal(t, i%2 == 1, got, "bit %d", i)
			}
		})
	}
}

func TestGet_NegativeIndex(t *testing.T) {
	for _, length := range testLengths {
		t.Run("length_"+strconv.Itoa(length), func(t *testing.T) {
			s, err := New(length)
			require.NoError(t, err)

			_, err = s.Get(-1)
			require.Error(t, err)
			require.True(t, status.IsOutOfRangeError(err))
		})
	}
}

func TestGet_IndexAtLen(t *testing.T) {
	for _, length := range testLengths {
		t.Run("length_"+strconv.Itoa(length), func(t *testing.T) {
			s, err := New(length)
			require.NoError(t, err)

			_, err = s.Get(length)
			require.Error(t, err)
			require.True(t, status.IsOutOfRangeError(err))
		})
	}
}

func TestBytes(t *testing.T) {
	for _, testCase := range []struct {
		name        string
		length      int
		initialBits map[int]bool
		mutate      func([]byte)
		wantBytes   []byte
		wantBits    map[int]bool
	}{
		{
			name:      "length 0",
			length:    0,
			wantBytes: []byte{},
			wantBits:  map[int]bool{},
		},
		{
			name:   "single bit in first byte",
			length: 7,
			initialBits: map[int]bool{
				0: true,
			},
			wantBytes: []byte{0b00000001},
			wantBits: map[int]bool{
				0: true,
				1: false,
				2: false,
				3: false,
				4: false,
				5: false,
				6: false,
			},
		},
		{
			name:   "first and last bit in one byte",
			length: 8,
			initialBits: map[int]bool{
				0: true,
				7: true,
			},
			wantBytes: []byte{0b10000001},
			wantBits: map[int]bool{
				0: true,
				1: false,
				2: false,
				3: false,
				4: false,
				5: false,
				6: false,
				7: true,
			},
		},
		{
			name:   "bit in second byte",
			length: 9,
			initialBits: map[int]bool{
				8: true,
			},
			wantBytes: []byte{0b00000000, 0b00000001},
			wantBits: map[int]bool{
				0: false,
				1: false,
				2: false,
				3: false,
				4: false,
				5: false,
				6: false,
				7: false,
				8: true,
			},
		},
		{
			name:   "returned slice aliases storage",
			length: 9,
			mutate: func(bytes []byte) {
				copy(bytes, []byte{0b10000001, 0b00000001})
			},
			wantBytes: []byte{0b10000001, 0b00000001},
			wantBits: map[int]bool{
				0: true,
				1: false,
				2: false,
				3: false,
				4: false,
				5: false,
				6: false,
				7: true,
				8: true,
			},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			s, err := New(testCase.length)
			require.NoError(t, err)

			for index, value := range testCase.initialBits {
				require.NoError(t, s.Set(index, value))
			}

			bytes := s.Bytes()
			if testCase.mutate != nil {
				testCase.mutate(bytes)
			}

			require.Equal(t, testCase.wantBytes, bytes)
			require.Equal(t, testCase.wantBytes, s.Bytes())
			for i := range testCase.length {
				got, err := s.Get(i)
				require.NoError(t, err)
				require.Equal(t, testCase.wantBits[i], got, "bit %d", i)
			}
		})
	}
}

func requireBitPattern(t *testing.T, s *Set, length int, want func(int) bool) {
	t.Helper()

	for i := range length {
		got, err := s.Get(i)
		require.NoError(t, err)
		require.Equal(t, want(i), got, "bit %d", i)
	}
}
