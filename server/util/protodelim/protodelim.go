// package protodelim contains utilities for working with length-delimited
// proto streams.
package protodelim

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"google.golang.org/protobuf/proto"
)

var (
	// ErrNegativeLength is returned when a negative length is read from a
	// varint-delimited stream.
	ErrNegativeLength = errors.New("unexpected negative length")
)

// delimiterFunc is a function that reads the length delimiter from a stream.
type delimiterFunc func(io.ByteReader) (uint64, error)

// ReadUvarint is a length delimiter for uvarint-encoded lengths.
func ReadUvarint(r io.ByteReader) (uint64, error) {
	return binary.ReadUvarint(r)
}

// ReadVarint is a length delimiter for varint-encoded lengths.
func ReadVarint(r io.ByteReader) (uint64, error) {
	n, err := binary.ReadVarint(r)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, ErrNegativeLength
	}
	return uint64(n), nil
}

// Reader is a length-delimited proto reader.
type Reader struct {
	buf       bytes.Buffer
	br        *bufio.Reader
	delimiter delimiterFunc
}

// NewReader returns a new length-delimited proto reader, where the given
// function is used to read lengths (usually either ReadUvarint or ReadVarint
// from this package).
func NewReader(r io.Reader, delimiter delimiterFunc) *Reader {
	return &Reader{
		br:        bufio.NewReader(r),
		delimiter: delimiter,
	}
}

// Read reads a proto from the stream.
func (r *Reader) Read(m proto.Message) error {
	size, err := r.delimiter(r.br)
	if err != nil {
		return err
	}
	r.buf.Reset()
	if _, err := io.CopyN(&r.buf, r.br, int64(size)); err != nil {
		return err
	}
	return proto.Unmarshal(r.buf.Bytes(), m)
}
