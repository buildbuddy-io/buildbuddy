package timeseries_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/timeseries"
	"github.com/stretchr/testify/require"

	timeseriespb "github.com/buildbuddy-io/buildbuddy/proto/timeseries"
)

func TestEncodeAndDecode(t *testing.T) {
	for _, n := range []int{0, 1, 10, 100, 1000, 10_000} {
		t.Run(fmt.Sprintf("%d samples", n), func(t *testing.T) {
			expectedValues := []int64{}
			// Construct a sequence with some positive values, some negative
			// values, some repeats
			for i := 0; i < n; i++ {
				val := int64(i/2 - n/4)
				expectedValues = append(expectedValues, val)
			}
			pb := timeseries.Encode(expectedValues)

			encodedSize := proto.Size(pb)
			decodedSize := len(expectedValues) * 8
			t.Logf("n=%d, compression_ratio=%.3f", n, float64(encodedSize)/float64(decodedSize))

			values, err := timeseries.Decode(pb)
			require.NoError(t, err)
			require.Equal(t, expectedValues, values)
		})
	}
}

func TestDecompressMalformedData(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *timeseriespb.Timeseries
	}{
		{
			name: "InvalidData",
			value: &timeseriespb.Timeseries{
				Length:   100,
				DataHigh: nil,
				DataLow:  nil,
			},
		},
		{
			name: "InvalidDataWithLengthMismatch",
			value: &timeseriespb.Timeseries{
				Length:   100,
				DataHigh: []byte{0, 1, 2, 3},
				DataLow:  []byte{0, 1},
			},
		},
	} {
		_, err := timeseries.Decode(test.value)
		require.Error(t, err)
	}
}
