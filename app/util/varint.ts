// Based on https://cs.opensource.google/go/go/+/refs/tags/go1.17.3:src/encoding/binary/varint.go

import Long from "long";

export const MAX_VARINT_LENGTH_64 = 10;

const uint64 = (low = 0, high = 0) => new Long(low, high);

/**
 * Reads an unsigned variable-length integer up to 64 bits from the given byte buffer, and
 * returns the number of bytes read, starting from the given offset.
 */
export function readUvarint(bytes: Uint8Array, offset = 0): [Long, number] {
  let x = Long.UZERO;
  let s = 0;
  for (let i = 0; i < MAX_VARINT_LENGTH_64; i++) {
    if (offset + i >= bytes.byteLength) {
      throw new Error("Out of bounds while reading Uvarint");
    }
    const b = bytes[offset + i];
    const longB = uint64(b);
    const numBytesRead = i - offset + 1;
    if (b < 0x80) {
      if (i === MAX_VARINT_LENGTH_64 - 1 && b > 1) {
        throw new Error("Overflow while reading Uvarint");
      }
      return [x.or(longB.shiftLeft(uint64(s))), numBytesRead];
    }
    x = x.or(longB.and(uint64(0x7f)).shiftLeft(uint64(s)));
    s += 7;
  }
  throw new Error("Overflow while reading Uvarint");
}

/**
 * Reads a signed variable-length integer up to 64 bits from the given byte buffer, and
 * returns the number of bytes read, starting from the given offset.
 */
export function readVarint(bytes: Uint8Array, offset = 0): [Long, number] {
  const [ux, numBytesRead] = readUvarint(bytes, offset);
  let x = ux.shiftRightUnsigned(1).toSigned();
  if (ux.and(uint64(1)).notEquals(uint64(0))) {
    x = x.not();
  }
  return [x, numBytesRead];
}
