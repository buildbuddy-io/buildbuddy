package memory_metrics_collector

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	// noExpiration is a special expiration value indicating that no expiration
	// should be set.
	noExpiration time.Duration = 0
)

func TestIncrementCounts(t *testing.T) {
	m, err := NewMemoryMetricsCollector()
	require.NoError(t, err)
	counts1 := make(map[string]int64)
	counts1["field1"] = 4
	counts1["field2"] = 50_000
	err = m.IncrementCounts(context.Background(), "key", counts1)
	require.NoError(t, err)
	read, err := m.ReadCounts(context.Background(), "key")
	require.NoError(t, err)
	require.Equal(t, counts1, read)
	err = m.IncrementCounts(context.Background(), "key2", counts1)
	require.NoError(t, err)
	read, err = m.ReadCounts(context.Background(), "key")
	require.NoError(t, err)
	require.Equal(t, counts1, read)
	read, err = m.ReadCounts(context.Background(), "key2")
	require.NoError(t, err)
	require.Equal(t, counts1, read)
	counts2 := make(map[string]int64)
	counts2["field2"] = 50_000
	counts2["field3"] = 7
	err = m.IncrementCounts(context.Background(), "key", counts2)
	require.NoError(t, err)
	read, err = m.ReadCounts(context.Background(), "key")
	require.NoError(t, err)
	expected := make(map[string]int64)
	expected["field1"] = 4
	expected["field2"] = 100_000
	expected["field3"] = 7
	require.Equal(t, expected, read)
	read, err = m.ReadCounts(context.Background(), "key2")
	require.NoError(t, err)
	require.Equal(t, counts1, read)
}

func TestSetAndGetAll(t *testing.T) {
	mc, err := NewMemoryMetricsCollector()
	require.NoError(t, err)
	ctx := context.Background()
	const nKeys = 50_000

	// Do a bunch of Set() operations
	keys := make([]string, 0, nKeys)
	expectedValues := make([]string, 0, nKeys)
	for i := 0; i < nKeys; i++ {
		key := fmt.Sprintf("key_%d", i)
		keys = append(keys, key)
		val := fmt.Sprintf("val_%d", i)
		expectedValues = append(expectedValues, val)
		err := mc.Set(ctx, key, val, noExpiration)
		require.NoError(t, err)
	}

	// Read back the values written via Set()
	values, err := mc.GetAll(ctx, keys...)
	require.NoError(t, err)
	require.Equal(t, expectedValues, values)
}

func TestListAppendAndRange(t *testing.T) {
	mc, err := NewMemoryMetricsCollector()
	require.NoError(t, err)
	ctx := context.Background()

	err = mc.ListAppend(ctx, "key", "1", "2")
	require.NoError(t, err)
	err = mc.ListAppend(ctx, "key", "3")
	require.NoError(t, err)

	for _, testCase := range []struct {
		start, stop int64
		expected    []string
		msg         string
	}{
		{0, 2, []string{"1", "2", "3"}, "getting the complete list should work"},
		{0, 1, []string{"1", "2"}, "getting a partial range should work"},
		{0, 0, []string{"1"}, "getting a range of size 1 should work"},
		{-3, 2, []string{"1", "2", "3"}, "negative start indexes should work"},
		{0, -1, []string{"1", "2", "3"}, "negative end indexes should work"},
		{-100, 2, []string{"1", "2", "3"}, "out-of-range start index should be treated like first element index"},
		{0, 100, []string{"1", "2", "3"}, "out-of-range end index should be treated like last element index"},
		{2, 1, []string{}, "start > end should return empty list"},
	} {
		list, err := mc.ListRange(ctx, "key", testCase.start, testCase.stop)
		require.NoError(t, err)
		require.Equal(t, testCase.expected, list, testCase.msg)
	}
}
