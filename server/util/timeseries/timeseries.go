package timeseries

// DeltaEncode encodes the given timeseries data as differences relative to the
// previous data point, starting from 0. Assuming the data mostly changes by
// small increments over time, this should result in better compression when
// encoded as repeated int64 in protobuf wire format.
func DeltaEncode(data []int64) []int64 {
	last := int64(0)
	out := make([]int64, 0, len(data))
	for _, x := range data {
		out = append(out, x-last)
		last = x
	}
	return out
}

// Decode returns the original []int64 data from a delta encoding.
func DeltaDecode(deltas []int64) []int64 {
	cur := int64(0)
	out := make([]int64, 0, len(deltas))
	for _, d := range deltas {
		cur += d
		out = append(out, cur)
	}
	return out
}
