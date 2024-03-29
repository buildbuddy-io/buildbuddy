package index

import (
	"bytes"
	"io"
	"sort"
	"testing"

	"github.com/RoaringBitmap/roaring"
	"github.com/stretchr/testify/assert"
)

var sampleBuf = `
package main

import (
  "fmt"
)

func main() {
  fmt.Println("Hello World")
}

`

func oldTokenize(f io.Reader) []string {
	trigram := roaring.New()
	trigram.Clear()
	var (
		c   = byte(0)
		i   = 0
		buf = make([]byte, 0, 16384)
		tv  = uint32(0)
		n   = int64(0)
	)
	for {
		tv = (tv << 8) & (1<<24 - 1)
		if i >= len(buf) {
			n, err := f.Read(buf[:cap(buf)])
			if n == 0 {
				if err != nil {
					if err == io.EOF {
						break
					}
					return nil
				}
				return nil
			}
			buf = buf[:n]
			i = 0
		}
		c = buf[i]
		i++
		tv |= uint32(c)
		if n++; n >= 3 {
			trigram.Add(tv)
		}
		if !validUTF8((tv>>8)&0xFF, tv&0xFF) {
			return nil
		}
	}

	r := make([]string, int(trigram.GetCardinality()))
	for i, tt := range trigram.ToArray() {
		r[i] = string(trigramToBytes(tt))
	}
	return r
}

// Test generated tokens match old code search
func TestTrigramTokenizer(t *testing.T) {
	tt := NewTrigramTokenizer()
	tt.Reset(bytes.NewReader([]byte(sampleBuf)))
	newTokens := make([]string, 0)
	for {
		tok, err := tt.Next()
		if err != nil {
			break
		}
		newTokens = append(newTokens, string(tok.Ngram()))
	}
	oldTokens := oldTokenize(bytes.NewReader([]byte(sampleBuf)))

	sort.Strings(oldTokens)
	sort.Strings(newTokens)
	assert.Equal(t, oldTokens, newTokens)
}
