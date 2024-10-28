package timeseries

import (
	"fmt"

	"github.com/bmkessler/streamvbyte"

	timeseriespb "github.com/buildbuddy-io/buildbuddy/proto/timeseries"
)

// Encode packs the given data into a new Timeseries proto.
func Encode(data []int64) *timeseriespb.Timeseries {
	hi := make([]uint32, len(data))
	lo := make([]uint32, len(data))
	for i, v := range data {
		hi[i] = uint32((uint64(v) & 0xFFFFFFFF00000000) >> 32)
		lo[i] = uint32((uint64(v) & 0x00000000FFFFFFFF))
	}

	hiEnc := make([]byte, streamvbyte.MaxSize32(len(hi)))
	loEnc := make([]byte, streamvbyte.MaxSize32(len(lo)))
	n := streamvbyte.EncodeDeltaUint32(hiEnc, hi, 0)
	hiEnc = hiEnc[:n]
	n = streamvbyte.EncodeDeltaUint32(loEnc, lo, 0)
	loEnc = loEnc[:n]

	return &timeseriespb.Timeseries{
		DataHigh: hiEnc,
		DataLow:  loEnc,
		Length:   int64(len(data)),
	}
}

// Decode returns the original []int64 data from a Timeseries proto.
func Decode(ts *timeseriespb.Timeseries) (_ []int64, err error) {
	// streamvbyte lib panics if data is malformed, so we recover() here and
	// return an error in this case.
	defer func() {
		if panicValue := recover(); panicValue != nil {
			err = fmt.Errorf("malformed data")
		}
	}()

	hi := make([]uint32, ts.Length)
	lo := make([]uint32, ts.Length)
	streamvbyte.DecodeDeltaUint32(hi, ts.DataHigh, 0)
	streamvbyte.DecodeDeltaUint32(lo, ts.DataLow, 0)

	out := make([]int64, ts.Length)
	for i := range out {
		value := uint64(0)
		value |= uint64(hi[i]) << 32
		value |= uint64(lo[i])
		out[i] = int64(value)
	}

	return out, nil
}
