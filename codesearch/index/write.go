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
// A file is assumed not to be a text file (and thus is not indexed)
// if it contains an invalid UTF-8 sequences, if it is longer than maxFileLength
// bytes, if it contains a line longer than maxLineLen bytes, or if it contains
// more than maxTextTrigrams distinct trigrams.
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

type postingLists map[string][]uint32

type SimpleIndexWriter struct {
	db *pebble.DB

	id           uuid.UUID
	tokenizer    *TrigramTokenizer
	postingLists map[string][]uint32

	fieldPostingLists map[int]postingLists
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

		fieldPostingLists: make(map[int]postingLists),
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
	// TODO(tylerw): add Stored() bool
}

type Document interface {
	ID() uint32
	Fields() []int
	Field(int) Field
	// TODO(tylerw): add Boost() float64
}

type namedField struct {
	ftype fieldType
	name  string
	buf   []byte
}

func (f namedField) Type() fieldType  { return f.ftype }
func (f namedField) Name() string     { return f.name }
func (f namedField) Contents() []byte { return f.buf }
func (f namedField) String() string {
	var snippet string
	if len(f.buf) < 10 {
		snippet = string(f.buf)
	} else {
		snippet = string(f.buf[:10])
	}
	return fmt.Sprintf("field<type: %v, name: %q, buf: %q>", f.ftype, f.name, snippet)
}

type mapDocument struct {
	id       uint32
	fieldMap map[int]namedField
}

func (d mapDocument) ID() uint32        { return d.id }
func (d mapDocument) Field(k int) Field { return d.fieldMap[k] }
func (d mapDocument) Fields() []int {
	keys := make([]int, len(d.fieldMap))
	i := 0
	for k := range d.fieldMap {
		keys[i] = k
		i++
	}
	return keys
}

func (s *SimpleIndexWriter) AddDocument(doc Document) error {
	for _, fid := range doc.Fields() {
		field := doc.Field(fid)
		if _, ok := s.fieldPostingLists[fid]; !ok {
			s.fieldPostingLists[fid] = make(postingLists, 0)
		}
		postingLists := s.fieldPostingLists[fid]

		// Lookup the tokenizer to use for this field.
		s.tokenizer.Reset(bufio.NewReader(bytes.NewReader(field.Contents())))
		for {
			tok, err := s.tokenizer.Next()
			if err != nil {
				break
			}
			ngram := string(tok.Ngram())
			postingLists[ngram] = append(postingLists[ngram], doc.ID())
		}
	}
	return nil
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

	doc := mapDocument{
		id: docid,
		fieldMap: map[int]namedField{
			1: namedField{textField, "filename", []byte(name)},
			2: namedField{textField, "body", contents},
		},
	}

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

	return s.AddDocument(doc)
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
	for fieldNumber, postingLists := range s.fieldPostingLists {
		for ngram, docIDs := range postingLists {
			writeDocIDs(ngramKey(ngram, fieldNumber), docIDs)
		}
	}
	return flushBatch()
}
