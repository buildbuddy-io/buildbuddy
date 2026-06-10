package keys_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/raft/keys"
	"github.com/stretchr/testify/assert"
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
