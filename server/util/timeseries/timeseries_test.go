package timeseries_test

import (
	"fmt"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/timeseries"
	"github.com/stretchr/testify/require"
)

func TestDeltaEncodeAndDecode(t *testing.T) {
	for _, n := range []int{0, 1, 10} {
		t.Run(fmt.Sprintf("%d samples", n), func(t *testing.T) {
			expectedValues := []int64{}
			// Construct a sequence with some positive values, some negative
			// values, some repeats
			for i := 0; i < n; i++ {
				val := int64(i/2 - n/4)
				expectedValues = append(expectedValues, val)
			}
			enc := timeseries.DeltaEncode(expectedValues)
			dec := timeseries.DeltaDecode(enc)

			require.Equal(t, expectedValues, dec)
		})
	}
}
