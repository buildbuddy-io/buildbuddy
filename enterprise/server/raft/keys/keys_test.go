package keys_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/stretchr/testify/assert"

	rfpb "github.com/buildbuddy-io/buildbuddy/proto/raft"
)

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

func TestEnsurePartitionID(t *testing.T) {
	t.Run("backfills_empty", func(t *testing.T) {
		rd := &rfpb.RangeDescriptor{Start: []byte("PTfoo/abc")}
		mutated := keys.EnsurePartitionID(rd)
		assert.True(t, mutated)
		assert.Equal(t, "foo", rd.GetPartitionId())
	})
	t.Run("leaves_set_value_alone", func(t *testing.T) {
		rd := &rfpb.RangeDescriptor{Start: []byte("PTfoo/abc"), PartitionId: "preset"}
		mutated := keys.EnsurePartitionID(rd)
		assert.False(t, mutated)
		assert.Equal(t, "preset", rd.GetPartitionId())
	})
	t.Run("meta_range_unchanged", func(t *testing.T) {
		rd := &rfpb.RangeDescriptor{Start: []byte("\x02meta")}
		mutated := keys.EnsurePartitionID(rd)
		assert.False(t, mutated)
		assert.Equal(t, "", rd.GetPartitionId())
	})
}
