package timeseries

import (
	"fmt"

	"github.com/bmkessler/streamvbyte"
)

const (
	chunkSize = 8 * 1024
)

// Buffer compactly buffers timeseries data using streamvbyte. It currently
// only supports 32-bit ints.
//
// If this is used to represent millisecond-precision unix timestamps, the max
// representable timestamp occurs in the year 2038. Consider representing these
// timestamps as durations relative to a starting timestamp.
type Buffer struct {
	// Current chunk that has yet to be compressed.
	buf []int32
	// Compressed chunks.
	chunks []Chunk
	// Intermediate compression buffer.
	chunk []byte
}

func (d *Buffer) Append(value int32) {
	d.buf = append(d.buf, value)
	if len(d.buf) >= chunkSize {
		d.Pack()
	}
}

// Pack compresses any buffered data, ensuring that the values will be
// returned by Chunks().
func (d *Buffer) Pack() {
	maxSize := streamvbyte.MaxSize32(len(d.buf))
	if len(d.chunk) < maxSize {
		d.chunk = make([]byte, maxSize)
	}
	n := streamvbyte.EncodeDeltaInt32(d.chunk, d.buf, 0)
	// Make a new slice so that the backing array doesn't have excess capacity,
	// which will take up memory.
	trimmedChunk := append(make([]byte, 0, n), d.chunk[:n]...)
	d.chunks = append(d.chunks, Chunk{
		Length: len(d.buf),
		Data:   trimmedChunk,
	})
	// Reuse the current buffer, setting its length to 0.
	d.buf = d.buf[:0]
}

// Chunks returns all compressed chunks.
// It does not return chunks that have not yet been packed.
func (d *Buffer) Chunks() []Chunk {
	return d.chunks
}

type Chunk struct {
	// Length is the number of compressed int32 values stored in Data.
	Length int
	// Data is the compressed data.
	Data []byte
}

// Decompress returns the original timeseries data given compressed chunks.
func Decompress(chunks ...Chunk) (_ []int32, err error) {
	// streamvbyte lib panics if data is malformed, so we recover() here and
	// return an error in this case.
	defer func() {
		if panicValue := recover(); panicValue != nil {
			err = fmt.Errorf("malformed data")
		}
	}()

	var length int
	for _, c := range chunks {
		length += c.Length
	}
	out := make([]int32, length)
	off := 0
	for _, c := range chunks {
		streamvbyte.DecodeDeltaInt32(out[off:off+c.Length], c.Data, 0)
		off += c.Length
	}
	return out, nil
}
