package index

import (
	"crypto/sha256"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"strconv"

	"github.com/cockroachdb/pebble"
)

const (
	dataPrefix     = "dat:"
	filenamePrefix = "fil:"
	trigramPrefix  = "tri:"
	ngramPrefix    = "gra:"
	namehashPrefix = "nam:"
)

var (
	// TODO(tylerw): set via API instead.
	repository = flag.String("repo", "", "A repository to index to / filter to")
)

func trigramToBytes(tv uint32) []byte {
	l := byte((tv >> 16) & 255)
	m := byte((tv >> 8) & 255)
	r := byte(tv & 255)
	return []byte{l, m, r}
}

func trigramToString(tv uint32) string {
	return string(trigramToBytes(tv))
}

func uint32ToBytes(i uint32) []byte {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf, i)
	return buf
}

func bytesToUint32(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}

func printDB(db *pebble.DB) {
	iter := db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0},
		UpperBound: []byte{math.MaxUint8},
	})
	defer iter.Close()
	log.Printf("<BEGIN DB>")
	for iter.First(); iter.Valid(); iter.Next() {
		log.Printf("\tkey: %q len(val): %d\n", string(iter.Key()), len(iter.Value()))
	}
	log.Printf("<END DB>")
}

// validUTF8 reports whether the byte pair can appear in a
// valid sequence of UTF-8-encoded code points.
func validUTF8(c1, c2 uint32) bool {
	switch {
	case c1 < 0x80:
		// 1-byte, must be followed by 1-byte or first of multi-byte
		return c2 < 0x80 || 0xc0 <= c2 && c2 < 0xf8
	case c1 < 0xc0:
		// continuation byte, can be followed by nearly anything
		return c2 < 0xf8
	case c1 < 0xf8:
		// first of multi-byte, must be followed by continuation byte
		return 0x80 <= c2 && c2 < 0xc0
	}
	return false
}

func hashString(s string) string {
	// Compute the SHA256 hash of the file.
	h := sha256.New()
	if _, err := h.Write([]byte(s)); err != nil {
		log.Fatal(err)
	}
	return fmt.Sprintf("%x", h.Sum(nil))
}

func hashFile(f io.ReadSeeker) ([]byte, error) {
	// Compute the SHA256 hash of the file.
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return nil, err
	}
	return h.Sum(nil), nil
}

func makeKey(prefix, key string) []byte {
	return []byte(*repository + prefix + key)
}

func dataKey(key string) []byte {
	return makeKey(dataPrefix, key)
}

func filenameKey(key string) []byte {
	return makeKey(filenamePrefix, key)
}

func trigramKey(key string) []byte {
	return makeKey(trigramPrefix, key)
}

func namehashKey(key string) []byte {
	return makeKey(namehashPrefix, key)
}

func ngramKey(key string, fieldNumber int) []byte {
	// TODO(tylerw): move field to after key?
	// all ngrams for a field should be grouped together so
	// gra:abc:1:12345
	// gra:abc:1:45678
	// gra:abc:2:12344
	// gra:abc:2:45678
	// gra:def:1:12344
	// gra:def:1:45678
	// ... etc ...
	return makeKey(ngramPrefix, strconv.Itoa(fieldNumber)+":"+key)
}
