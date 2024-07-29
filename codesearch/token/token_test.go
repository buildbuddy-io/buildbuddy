package token

import (
	"bytes"
	"io"
	"sort"
	"strings"
	"testing"
	"unicode"

	"github.com/RoaringBitmap/roaring"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
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
		d   = rune(0)
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
		d = unicode.ToLower(rune(c))
		i++
		tv |= uint32(d)
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

// tokenizeBuf applies a tokenizer to a string and returns a sorted slice of
// tokens.
func tokenizeBuf(buf string, tt types.Tokenizer) []string {
	tt.Reset(strings.NewReader(buf))
	tokens := make([]string, 0)
	for tt.Next() == nil {
		tokens = append(tokens, string(tt.Ngram()))
	}
	return tokens
}

// Test generated tokens match old code search
func TestTrigramTokenizer(t *testing.T) {
	newTokens := tokenizeBuf(sampleBuf, NewTrigramTokenizer())
	oldTokens := oldTokenize(bytes.NewReader([]byte(sampleBuf)))
	sort.Strings(newTokens)
	sort.Strings(oldTokens)
	assert.Equal(t, oldTokens, newTokens)
}

func TestWhitespaceTokenizer(t *testing.T) {
	tokens := tokenizeBuf("this is a string", NewWhitespaceTokenizer())
	assert.Equal(t, []string{"this", "is", "a", "string"}, tokens)
}

// func TestBuildAllNgramsMatchesTrigrams(t *testing.T) {
// 	allTokens := tokenizeBuf(sampleBuf, NewSparseNgramTokenizer())
// 	triTokens := tokenizeBuf(sampleBuf, NewTrigramTokenizer())
// 	for _, tri := range triTokens {
// 		assert.Contains(t, allTokens, tri)
// 	}
// }

func TestHashBigram(t *testing.T) {
	assert.Equal(t, uint32(512235571), HashBigram([]byte("he")))
}

func TestBuildAllNgrams(t *testing.T) {
	assert.Equal(t, []string{}, BuildAllNgrams([]byte("he")))
	assert.Equal(t, []string{"hel"}, BuildAllNgrams([]byte("hel")))
	assert.Equal(t, []string{"hel", "ell"}, BuildAllNgrams([]byte("hell")))
	assert.Equal(t, []string{"hel", "ell", "llo", "lo ", "o w", "lo w", " wo",
		"lo wo", "wor", "orl", "worl", "rld"}, BuildAllNgrams([]byte("hello world")))
}

func TestBuildCoveringNgrams(t *testing.T) {
	assert.Equal(t, []string{}, BuildCoveringNgrams([]byte("he")))
	assert.Equal(t, []string{"hel"}, BuildCoveringNgrams([]byte("hel")))
	assert.Equal(t, []string{"hel", "ell"}, BuildCoveringNgrams([]byte("hell")))
	assert.Equal(t, []string{"hel", "ell", "llo", "rld", "worl", "lo wo"}, BuildCoveringNgrams([]byte("hello world")))
}

func TestSplitGithubCodesearch(t *testing.T) {
	assert.Equal(t, []string{"che", "hes", "ches", "est", "chest", "ste",
		"ter", "ster", "er "}, BuildAllNgrams([]byte("chester ")))
	assert.Equal(t, []string{"chest", "ster", "er "}, BuildCoveringNgrams([]byte("chester ")))
	assert.Equal(t, []string{"chest", "ster"}, BuildCoveringNgrams([]byte("chester")))
}

func TestSplitForLoop(t *testing.T) {
	assert.Equal(t, []string{"for", "or(", "for(", "r(i", "for(i", "(in", "int",
		"(int", "nt ", "t i", " i=", "t i=", "i=4", "t i=4",
		"nt i=4", "(int i=4", "=42"}, BuildAllNgrams([]byte("for(int i=42")))
	assert.Equal(t, []string{"for(i", "(int i=4", "=42"}, BuildCoveringNgrams([]byte("for(int i=42")))
}
