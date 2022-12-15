// This file is primarily adapted from useful (unexported) functions from
// the golang package 'archive/zip', slightly modified to match our use
// cases.

// Copyright (c) 2009 The Go Authors. All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:

//    * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//    * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.

// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package bytestream

import (
	"encoding/binary"
	"errors"

	arpb "github.com/buildbuddy-io/buildbuddy/proto/archive"
)

const (
	fileHeaderSignature      = 0x04034b50
	directoryHeaderSignature = 0x02014b50
	directoryEndLen          = 22
	directoryHeaderLen       = 46
	fileHeaderLen            = 30
)

var (
	ErrFormat    = errors.New("zip: not a valid zip file")
	ErrAlgorithm = errors.New("zip: unsupported compression algorithm")
	ErrChecksum  = errors.New("zip: checksum error")
	ErrZip64     = errors.New("zip: zip64 not supported")
)

type directoryEnd struct {
	diskNbr            uint32 // unused
	dirDiskNbr         uint32 // unused
	dirRecordsThisDisk uint64 // unused
	directoryRecords   uint64
	directorySize      uint64
	directoryOffset    uint64 // relative to file
	commentLen         uint16
	comment            string
}

type readBuf []byte

func (b *readBuf) uint8() uint8 {
	v := (*b)[0]
	*b = (*b)[1:]
	return v
}

func (b *readBuf) uint16() uint16 {
	v := binary.LittleEndian.Uint16(*b)
	*b = (*b)[2:]
	return v
}

func (b *readBuf) uint32() uint32 {
	v := binary.LittleEndian.Uint32(*b)
	*b = (*b)[4:]
	return v
}

func (b *readBuf) uint64() uint64 {
	v := binary.LittleEndian.Uint64(*b)
	*b = (*b)[8:]
	return v
}

func (b *readBuf) sub(n int) readBuf {
	b2 := (*b)[:n]
	*b = (*b)[n:]
	return b2
}

func findSignatureInBlock(b []byte) int {
	for i := len(b) - directoryEndLen; i >= 0; i-- {
		// defined from directoryEndSignature in struct.go
		if b[i] == 'P' && b[i+1] == 'K' && b[i+2] == 0x05 && b[i+3] == 0x06 {
			// n is length of comment
			n := int(b[i+directoryEndLen-2]) | int(b[i+directoryEndLen-1])<<8
			if n+directoryEndLen+i <= len(b) {
				return i
			}
		}
	}
	return -1
}

func readDirectoryEnd(input []byte, trueSize int64) (dir *directoryEnd, baseOffset int64, err error) {

	var directoryEndOffset int64 = -1
	bLen := int64(len(input))
	if bLen > trueSize {
		// XXX: error
		bLen = trueSize
	}

	if p := findSignatureInBlock(input); p >= 0 {
		input = input[p:]
		directoryEndOffset = trueSize - bLen + int64(p)
	}
	if directoryEndOffset < 0 {
		return nil, 0, ErrFormat
	}

	// read header into struct
	b := readBuf(input[4:]) // skip signature
	d := &directoryEnd{
		diskNbr:            uint32(b.uint16()),
		dirDiskNbr:         uint32(b.uint16()),
		dirRecordsThisDisk: uint64(b.uint16()),
		directoryRecords:   uint64(b.uint16()),
		directorySize:      uint64(b.uint32()),
		directoryOffset:    uint64(b.uint32()),
		commentLen:         b.uint16(),
	}
	l := int(d.commentLen)
	if l > len(b) {
		return nil, 0, errors.New("zip: invalid comment length")
	}
	d.comment = string(b[:l])

	// These values mean that the file can be a zip64 file
	if d.directoryRecords == 0xffff || d.directorySize == 0xffff || d.directoryOffset == 0xffffffff {
		return nil, 0, ErrZip64
	}

	baseOffset = directoryEndOffset - int64(d.directorySize) - int64(d.directoryOffset)

	// Make sure directoryOffset points to somewhere in our file.
	if o := baseOffset + int64(d.directoryOffset); o < 0 || o >= trueSize {
		return nil, 0, ErrFormat
	}
	return d, baseOffset, nil
}

// readDirectoryHeader attempts to read a directory header from r.
// It returns io.ErrUnexpectedEOF if it cannot read a complete header,
// and ErrFormat if it doesn't find a valid header signature.
func readDirectoryHeader(buf []byte, d *directoryEnd) ([]*arpb.ManifestEntry, error) {
	var headers []*arpb.ManifestEntry

	b := readBuf(buf[:])
	if len(b) < int(d.directorySize) {
		return nil, ErrFormat
	}

	for i := 0; i < int(d.directoryRecords); i++ {
		var h = &arpb.ManifestEntry{}
		headers = append(headers, h)
		if len(b) < directoryHeaderLen {
			return nil, ErrFormat
		}
		if sig := b.uint32(); sig != directoryHeaderSignature {
			return nil, ErrFormat
		}
		b = b[6:] // Skip CreatorVersion, ReaderVersion, Flags
		var compressionType = b.uint16()
		switch compressionType {
		case 0:
			h.Compression = arpb.ManifestEntry_COMPRESSION_TYPE_NONE
		case 8:
			h.Compression = arpb.ManifestEntry_COMPRESSION_TYPE_FLATE
		default:
			return nil, ErrAlgorithm
		}
		b = b[4:] // Skip ModifiedTime, ModifiedDate
		h.Crc32 = b.uint32()
		h.CompressedSize = int64(b.uint32())
		h.UncompressedSize = int64(b.uint32())
		filenameLen := int(b.uint16())
		extraLen := int(b.uint16())
		commentLen := int(b.uint16())
		b = b[8:] // Skip StartingDiskNumber, InternalAttrs, ExternalAttrs
		h.HeaderOffset = int64(b.uint32())
		if len(b) < filenameLen+extraLen+commentLen {
			return nil, ErrFormat
		}
		h.Name = string(b[:filenameLen])
		b = b[(filenameLen + extraLen + commentLen):]
	}

	// XXX: fail on zip64.
	// XXX: more validation..
	return headers, nil
}
