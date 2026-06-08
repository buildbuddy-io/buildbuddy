package bringup

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvenlyDividePartitionIntoRanges_SetsPartitionID(t *testing.T) {
	ranges, err := evenlyDividePartitionIntoRanges(disk.Partition{
		ID:        "PART1",
		NumRanges: 4,
	})
	require.NoError(t, err)
	require.Len(t, ranges, 4)
	for i, r := range ranges {
		assert.Equal(t, "PART1", r.GetPartitionId(), "range %d", i)
	}
}
