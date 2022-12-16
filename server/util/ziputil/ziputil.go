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

package ziputil

import (
	"compress/flate"
	"encoding/binary"
	"errors"
	"io"

	arpb "github.com/buildbuddy-io/buildbuddy/proto/archive"
)

const (
	FileHeaderSignature      = 0x04034b50
	DirectoryHeaderSignature = 0x02014b50
	DirectoryEndLen          = 22
	DirectoryHeaderLen       = 46
	FileHeaderLen            = 30
)

var (
	ErrFormat    = errors.New("zip: not a valid zip file")
	ErrAlgorithm = errors.New("zip: unsupported compression algorithm")
	ErrChecksum  = errors.New("zip: checksum error")
	ErrZip64     = errors.New("zip: zip64 not supported")
)

type DirectoryEnd struct {
	DirectoryRecords int64
	DirectorySize    int64
	DirectoryOffset  int64
	commentLen       uint16
	comment          string
}

type readBuf []byte

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

func findSignatureInBlock(b []byte) int {
	if len(b) < DirectoryEndLen {
		return -1
	}
	for i := len(b) - DirectoryEndLen; i >= 0; i-- {
		// defined from directoryEndSignature in struct.go
		if b[i] == 'P' && b[i+1] == 'K' && b[i+2] == 0x05 && b[i+3] == 0x06 {
			// n is length of comment
			n := int(b[i+DirectoryEndLen-2]) | int(b[i+DirectoryEndLen-1])<<8
			if n+DirectoryEndLen+i <= len(b) {
				return i
			}
		}
	}
	return -1
}

func compressionTypeToEnum(compression uint16) arpb.ManifestEntry_CompressionType {
	switch compression {
	case 0:
		return arpb.ManifestEntry_COMPRESSION_TYPE_NONE
	case 8:
		return arpb.ManifestEntry_COMPRESSION_TYPE_FLATE
	default:
		return arpb.ManifestEntry_COMPRESSION_TYPE_UNKNOWN
	}
}

// The returned value is equal to the number of bytes that are expected in the
// remaining (dynamically sized) header fields, or -1 if the header didn't validate.
func ValidateLocalFileHeader(header []byte, entry *arpb.ManifestEntry) (int, error) {
	buf := readBuf(header[:])
	if sig := buf.uint32(); sig != FileHeaderSignature {
		return 1, ErrFormat
	}

	buf = buf[4:] // Skip version, bitmap
	compressionType := compressionTypeToEnum(buf.uint16())
	if compressionType == arpb.ManifestEntry_COMPRESSION_TYPE_UNKNOWN {
		return -1, ErrAlgorithm
	}
	buf = buf[4:] // Skip modification time, modification date.

	crc32 := buf.uint32()
	compsize := int64(buf.uint32())
	uncompsize := int64(buf.uint32())
	if entry.GetCompressedSize() == 0xffffffff || entry.GetUncompressedSize() == 0xffffffff {
		// These values indicate zip64 format.
		return -1, ErrZip64
	}
	if entry.GetCrc32() != crc32 || entry.GetCompressedSize() != compsize || entry.GetUncompressedSize() != uncompsize {
		return -1, ErrFormat
	}

	filenameLen := int(buf.uint16())
	extraLen := int(buf.uint16())

	return filenameLen + extraLen, nil
}

func ValidateLocalFileNameAndExtras(input []byte, entry *arpb.ManifestEntry) error {
	if string(input[:len(entry.GetName())]) != entry.GetName() {
		return ErrFormat
	}
	return nil
}

func ReadDirectoryEnd(input []byte, trueSize int64) (dir *DirectoryEnd, err error) {
	if int64(len(input)) > trueSize {
		return nil, ErrFormat
	}
	if p := findSignatureInBlock(input); p >= 0 {
		input = input[p:]
	} else {
		return nil, ErrFormat
	}

	b := readBuf(input[10:]) // skip signature, disk fields
	d := &DirectoryEnd{
		DirectoryRecords: int64(b.uint16()),
		DirectorySize:    int64(b.uint32()),
		DirectoryOffset:  int64(b.uint32()),
		commentLen:       b.uint16(),
	}
	l := int(d.commentLen)
	if l > len(b) {
		return nil, errors.New("zip: invalid comment length")
	}
	d.comment = string(b[:l])

	// These values mean that the file can be a zip64 file
	if d.DirectoryRecords == 0xffff || d.DirectorySize == 0xffff || d.DirectoryOffset == 0xffffffff {
		return nil, ErrZip64
	}

	// Make sure directoryOffset points to somewhere in our file.
	if d.DirectoryOffset < 0 || d.DirectoryOffset+d.DirectorySize > trueSize {
		return nil, ErrFormat
	}
	return d, nil
}

// readDirectoryHeader attempts to read a directory header from r.
// It returns io.ErrUnexpectedEOF if it cannot read a complete header,
// and ErrFormat if it doesn't find a valid header signature.
func ReadDirectoryHeader(buf []byte, d *DirectoryEnd) ([]*arpb.ManifestEntry, error) {
	var headers []*arpb.ManifestEntry

	b := readBuf(buf[:])
	if len(b) < int(d.DirectorySize) {
		return nil, ErrFormat
	}

	for i := 0; i < int(d.DirectoryRecords); i++ {
		var h = &arpb.ManifestEntry{}
		headers = append(headers, h)
		if len(b) < DirectoryHeaderLen {
			return nil, ErrFormat
		}
		if sig := b.uint32(); sig != DirectoryHeaderSignature {
			return nil, ErrFormat
		}
		b = b[6:] // Skip CreatorVersion, ReaderVersion, Flags
		var compressionType = compressionTypeToEnum(b.uint16())
		if compressionType == arpb.ManifestEntry_COMPRESSION_TYPE_UNKNOWN {
			return nil, ErrAlgorithm
		}
		h.Compression = compressionType
		b = b[4:] // Skip ModifiedTime, ModifiedDate
		h.Crc32 = b.uint32()
		h.CompressedSize = int64(b.uint32())
		h.UncompressedSize = int64(b.uint32())
		if h.GetCompressedSize() == 0xffffffff || h.GetUncompressedSize() == 0xffffffff {
			// These values indicate zip64 format.
			return nil, ErrZip64
		}
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

	return headers, nil
}

func DecompressAndStream(writer io.Writer, reader io.Reader, entry *arpb.ManifestEntry) error {
	var outReader io.Reader
	if entry.GetCompression() == arpb.ManifestEntry_COMPRESSION_TYPE_FLATE {
		// TODO(jdhollen): maybe validate crc32?
		outReader = flate.NewReader(io.LimitReader(reader, int64(entry.GetCompressedSize())))
	} else if entry.GetCompression() == arpb.ManifestEntry_COMPRESSION_TYPE_NONE {
		outReader = io.LimitReader(reader, int64(entry.GetCompressedSize()))
	} else {
		return ErrAlgorithm
	}

	if _, err := io.Copy(writer, outReader); err != nil {
		return err
	}
	return nil
}
