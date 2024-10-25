package timeseries_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/timeseries"
	"github.com/stretchr/testify/require"
)

func TestCompressAndDecompress(t *testing.T) {
	for _, n := range []int{0, 1, 100, 8192, 8193, 15000, 25000} {
		t.Run(fmt.Sprintf("%d samples", n), func(t *testing.T) {
			tsb := timeseries.Buffer{}
			expectedValues := []int32{}
			for i := 0; i < n; i++ {
				val := int32(i / 2)
				tsb.Append(val)
				expectedValues = append(expectedValues, val)
			}
			tsb.Pack()
			values, err := timeseries.Decompress(tsb.Chunks()...)
			require.NoError(t, err)
			require.Equal(t, expectedValues, values)
		})
	}
}

func TestDecompressMalformedData(t *testing.T) {
	_, err := timeseries.Decompress(timeseries.Chunk{
		Length: 100,
		Data:   []byte{0, 1, 2, 3, 4, 5, 6},
	})
	require.Error(t, err)
}
