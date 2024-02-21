package index

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"regexp"
	"runtime"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/cockroachdb/pebble"
	"github.com/gabriel-vasile/mimetype"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Tuning constants for detecting text files.
// A file is assumed not to be text files (and thus not indexed)
// if it contains an invalid UTF-8 sequences, if it is longer than maxFileLength
// bytes, if it contains a line longer than maxLineLen bytes,
// or if it contains more than maxTextTrigrams distinct trigrams.
const (
	maxFileLen      = 1 << 30
	maxLineLen      = 2000
	maxTextTrigrams = 20000

	npost = 64 << 20 / 8 // 64 MB worth of post entries
)

var skipMime = regexp.MustCompile(`^audio/.*|video/.*|image/.*$`)

type fieldType int32
const (
	textField fieldType = iota
)

type SimpleIndexWriter struct {
	db *pebble.DB

	id           uuid.UUID
	tokenizer    *TrigramTokenizer
	postingLists map[string][]uint32
}

func CreateSimple(db *pebble.DB) (*SimpleIndexWriter, error) {
	id, err := uuid.NewV7()
        if err != nil {
                return nil, err
        }
	
	return &SimpleIndexWriter{
		db:           db,
		id:           id,
		tokenizer:    NewTrigramTokenizer(textField),
		postingLists: make(map[string][]uint32, npost),
	}, nil
}

func (s *SimpleIndexWriter) AddFile(name string) error {
	// Open the file and read all contents to memory.
	f, err := os.Open(name)
	if err != nil {
		return err
	}
	defer f.Close()
	buf, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	// Compute the digest of the file.
	h := sha256.New()
	n, err := h.Write(buf)
	if err != nil {
		return err
	}
	d := &repb.Digest{
		Hash:      fmt.Sprintf("%x", h.Sum(nil)),
		SizeBytes: int64(n),
	}
	return s.AddFileByDigest(name, d, buf)
}

type Field interface {
	Type() fieldType
	Name() string
	Contents() []byte
}

type Document interface {
	ID() uint32
	Fields() []int
	Field(int) Field	
}

func (s *SimpleIndexWriter) AddDocument(doc Document) error {
	addToken := func(tok Token) {
		ngram := string(tok.Ngram())
		s.postingLists[ngram] = append(s.postingLists[ngram], docid)
	}

	for _, fid := range doc.Fields() {
		field := doc.Field(fid)

		// Lookup the tokenizer to use for this field.
		s.tokenizer.Reset(bufio.NewReader(bytes.NewReader(field.Contents())))
		for {
			tok, err := s.tokenizer.Next()
			if err != nil {
				break
			}
			addToken(tok)
		}
	}
}

func (s *SimpleIndexWriter) AddFileByDigest(name string, digest *repb.Digest, contents []byte) error {
	if len(contents) > maxFileLen {
		log.Printf("%s: too long, ignoring\n", name)
		return nil
	}
	r := bytes.NewReader(contents)
	mtype, err := mimetype.DetectReader(r)
	if err == nil && skipMime.MatchString(mtype.String()) {
		log.Printf("%q: skipping (invalid mime type: %q)", name, mtype.String())
		return nil
	}
	r.Seek(0, 0)

	// Compute docid (first bytes from digest)
	hexBytes, err := hex.DecodeString(digest.GetHash())
	if err != nil {
		return err
	}
	docid := bytesToUint32(hexBytes[:4])

	// Write a pointer from digest -> filename
	if err := s.db.Set(filenameKey(digest.GetHash()), []byte(name), pebble.NoSync); err != nil {
		return err
	}
	// Write a pointer from digest -> file contents
	if err := s.db.Set(dataKey(digest.GetHash()), contents, pebble.NoSync); err != nil {
		return err
	}
	// Write a pointer from hash(name) => digest
	if err := s.db.Set(namehashKey(hashString(name)), []byte(digest.GetHash()), pebble.NoSync); err != nil {
		return err
	}

	addToken := func(tok Token) {
		ngram := string(tok.Ngram())
		s.postingLists[ngram] = append(s.postingLists[ngram], docid)
	}

	// Tokenize the file contents
	s.tokenizer.Reset(bufio.NewReader(r))
	for {
		tok, err := s.tokenizer.Next()
		if err != nil {
			break
		}
		addToken(tok)
	}

	// // Tokenize the file name as well.
	// s.tokenizer.Reset(bufio.NewReader(bytes.NewReader([]byte(name))))
	// for {
	// 	tok, err := s.tokenizer.Next()
	// 	if err != nil {
	// 		break
	// 	}
	//      addToken(tok)
	// }
	return nil
}

func (s *SimpleIndexWriter) Flush() error {
	mu := sync.Mutex{}
	batch := s.db.NewBatch()
	flushBatch := func() error {
		if batch.Empty() {
			return nil
		}
		if err := batch.Commit(pebble.Sync); err != nil {
			return err
		}
		log.Printf("flushed batch")
		batch = s.db.NewBatch()
		return nil
	}

	eg := new(errgroup.Group)
	eg.SetLimit(runtime.GOMAXPROCS(0))
	writeDocIDs := func(key []byte, ids []uint32) error {
		buf := new(bytes.Buffer)
		pl := roaring.BitmapOf(ids...)
		if _, err := pl.WriteTo(buf); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		if err := batch.Set(key, buf.Bytes(), nil); err != nil {
			return err
		}
		if batch.Len() >= npost {
			if err := flushBatch(); err != nil {
				return err
			}
		}
		return nil
	}
	for ngram, docIDs := range s.postingLists {
		writeDocIDs(ngramKey(ngram), docIDs)
	}
	return flushBatch()
}
