package keys_test

import (
	"bytes"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAtimeIndexKey(t *testing.T) {
	fileKey := []byte("PTfoo/GR00000000000000000123/abcd/1/cas/v7")

	// Round trip.
	k := keys.AtimeIndexKey("foo", 12345678, fileKey)
	part, atime, fk, err := keys.ParseAtimeIndexKey(k)
	require.NoError(t, err)
	assert.Equal(t, "foo", part)
	assert.Equal(t, int64(12345678), atime)
	assert.Equal(t, fileKey, fk)

	// Byte order equals atime order (including atimes whose big-endian bytes
	// contain '/', 0x2f).
	older := keys.AtimeIndexKey("foo", 0x2f2f2f, fileKey)
	newer := keys.AtimeIndexKey("foo", 0x2f2f2f+1, fileKey)
	assert.Negative(t, bytes.Compare(older, newer))
	part, atime, fk, err = keys.ParseAtimeIndexKey(older)
	require.NoError(t, err)
	assert.Equal(t, "foo", part)
	assert.Equal(t, int64(0x2f2f2f), atime)
	assert.Equal(t, fileKey, fk)

	// Partition bounds contain the partition's entries and exclude others.
	start, end := keys.AtimeIndexPartitionRange("foo")
	assert.True(t, bytes.Compare(start, k) <= 0 && bytes.Compare(k, end) < 0)
	other := keys.AtimeIndexKey("fop", 12345678, fileKey)
	assert.False(t, bytes.Compare(start, other) <= 0 && bytes.Compare(other, end) < 0)
	// The case a separator-delimited range must actually guard against is a
	// partition ID that extends another as a prefix: "foobar" stays outside
	// "foo"'s bounds only because the trailing '/' (0x2f) sorts below the
	// extension's next byte.
	ext := keys.AtimeIndexKey("foobar", 12345678, fileKey)
	assert.False(t, bytes.Compare(start, ext) <= 0 && bytes.Compare(ext, end) < 0)

	// A negative atime is clamped to zero: encoded as uint64 it would sort
	// after every valid entry (breaking "byte order equals time order"),
	// making its record unreachable by the age-bounded eviction sweep.
	neg := keys.AtimeIndexKey("foo", -5, fileKey)
	part, atime, fk, err = keys.ParseAtimeIndexKey(neg)
	require.NoError(t, err)
	assert.Equal(t, "foo", part)
	assert.Equal(t, int64(0), atime)
	assert.Equal(t, fileKey, fk)
	assert.Negative(t, bytes.Compare(neg, older), "clamped entry must sort first")
	assert.True(t, bytes.Compare(start, neg) <= 0 && bytes.Compare(neg, end) < 0)

	// Non-index keys don't parse.
	_, _, _, err = keys.ParseAtimeIndexKey([]byte("PTfoo/abcd"))
	assert.Error(t, err)

	// The backfill marker sorts outside the partition's index range.
	marker := keys.AtimeIndexBackfillMarkerKey("foo")
	assert.False(t, bytes.Compare(start, marker) <= 0 && bytes.Compare(marker, end) < 0)
}

func TestPartitionIDFromRangeStart(t *testing.T) {
	cases := []struct {
		name string
		key  string
		want string
	}{
		{"with_partition", "PTfoo/abc123", "foo"},
		{"empty_partition_segment", "PT/abc", ""},
		{"no_slash_after_prefix", "PTfoo", ""},
		{"missing_prefix", "abc/def", ""},
		{"meta_range_prefix", "\x02somekey", ""},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := keys.PartitionIDFromRangeStart([]byte(tc.key))
			assert.Equal(t, tc.want, got)
		})
	}
}
