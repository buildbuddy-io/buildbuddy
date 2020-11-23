package buckets

var (
	// Buckets suitable for values measured in microsecond durations and whose values
	// are not constrained to any particular range.
	// TODO(bduffany): Measure how expensive this is.
	HighVariabilityMicrosecondBuckets = []float64{
		// sub-millisecond buckets
		1, 2.5, 5,
		10, 25, 50,
		100, 250, 500,
		// sub-second buckets
		1_000, 2_500, 5_000,
		10_000, 25_000, 50_000,
		100_000, 250_000, 500_000,
		// one or more seconds
		1_000_000, 2_500_000, 5_000_000,
		10_000_000, 25_000_000, 50_000_000,
	}
)
