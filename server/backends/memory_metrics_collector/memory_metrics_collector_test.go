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
