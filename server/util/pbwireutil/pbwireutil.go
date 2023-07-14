// pbwireutil contains utilities for working with the proto wire representation.
//
// It is especially useful in hot codepaths where it's not desired to do a full
// parse of the proto, which may cause an excessive number of allocations.
//
// See https://protobuf.dev/programming-guides/encoding/ to understand more
// about the proto message encoding. For example, make sure you understand the
// Tag-Length-Value encoding scheme, as well as the difference between things
// like "tag", "field number", and "wire type."
package pbwireutil

import "google.golang.org/protobuf/encoding/protowire"

// SeekTag returns the wire type and byte offset of the field value with the
// given field number, or a value < 0 if no such field record is found.
func SeekTag(b []byte, fieldNumber protowire.Number) (protowire.Type, int) {
	off := 0
	for off < len(b) {
		f, wireType, n := protowire.ConsumeTag(b[off:])
		if n < 0 {
			return 0, -1
		}
		off += n

		if fieldNumber == f {
			return wireType, off
		}

		n = protowire.ConsumeFieldValue(f, wireType, b[off:])
		if n < 0 {
			return 0, -1
		}
		off += n
	}

	return 0, -1
}

// ConsumeFirstString returns the first string value in the buffer with the
// given field number, and the byte offset pointing just after the end of the
// field value. It returns an offset of -1 if the string is not found.
func ConsumeFirstString(b []byte, fieldNumber protowire.Number) (value string, n int) {
	return consumeFirst(b, fieldNumber, protowire.ConsumeString)
}

// ConsumeFirstBytes returns the first bytes value in the buffer with the given
// field number, and the byte offset pointing just after the end of the field
// value. It returns an offset of -1 if the string is not found.
//
// The returned buffer can also be used to get encoded submessage field values
// as bytes, or packed repeated field buffers, which both use the same LEN wire
// type used by bytes fields. See
// https://protobuf.dev/programming-guides/encoding/#structure
func ConsumeFirstBytes(b []byte, fieldNumber protowire.Number) (value []byte, n int) {
	return consumeFirst(b, fieldNumber, protowire.ConsumeBytes)
}

// ConsumeFirstVarint returns the first varint-encoded value in the buffer with
// the given field number, and the byte offset pointing just after the end of
// the field value. It returns an offset of -1 if the string is not found.
//
// NOTE: Only use this if you are certain that the wire type of the field is
// VARINT (i.e. non-packed int32, int64, uint32, uint64, sint32, sint64, bool,
// enum). See https://protobuf.dev/programming-guides/encoding/#structure
func ConsumeFirstVarint(b []byte, fieldNumber protowire.Number) (value uint64, n int) {
	return consumeFirst(b, fieldNumber, protowire.ConsumeVarint)
}

// consumeFirst consumes the first field value with the given field number,
// using the given consumer function. It returns the byte offset pointing to the
// next record immediately following the field value, or len(b) if the last
// record was consumed, or -1 if the field is not found.
func consumeFirst[T any](b []byte, fieldNumber protowire.Number, consumer func(b []byte) (T, int)) (value T, n int) {
	var zero T

	_, off := SeekTag(b, fieldNumber)
	if off < 0 {
		return zero, -1
	}

	v, n := consumer(b[off:])
	if n < 0 {
		return zero, -1
	}
	return v, off + n
}
