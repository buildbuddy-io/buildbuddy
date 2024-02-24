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
// bytes, or if it contains more than maxTextTrigrams distinct trigrams.
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

type Writer struct {
	db  *pebble.DB
	log log.Logger

	repo              string
	segmentID         uuid.UUID
	tokenizer         *TrigramTokenizer
	fieldPostingLists map[int]postingLists
	batch             *pebble.Batch
}

func NewWriter(db *pebble.DB, repo string) (*Writer, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	subLog := log.NamedSubLogger(fmt.Sprintf("%s - %s", repo, id.String()))
	return &Writer{
		db:                db,
		log:               subLog,
		repo:              repo,
		segmentID:         id,
		tokenizer:         NewTrigramTokenizer(textField),
		fieldPostingLists: make(map[int]postingLists),
		batch:             db.NewBatch(),
	}, nil
}

func (s *Writer) AddFile(name string) error {
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
	Stored() bool
}

type Document interface {
	ID() uint32
	Fields() []int
	Field(int) Field
	// TODO(tylerw): add Boost() float64
}

type namedField struct {
	ftype  fieldType
	name   string
	buf    []byte
	stored bool
}

func (f namedField) Type() fieldType  { return f.ftype }
func (f namedField) Name() string     { return f.name }
func (f namedField) Contents() []byte { return f.buf }
func (f namedField) Stored() bool     { return f.stored }
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

func (w *Writer) storedFieldKey(docID uint32, fieldID int) []byte {
	return []byte(fmt.Sprintf("%s:doc:%d:%d", w.repo, docID, fieldID))
}

func (w *Writer) postingListKey(ngram string, fieldID int) []byte {
	return []byte(fmt.Sprintf("%s:gra:%s:%d", w.repo, ngram, fieldID))
}

func (w *Writer) AddDocument(doc Document) error {
	for _, fid := range doc.Fields() {
		field := doc.Field(fid)
		if _, ok := w.fieldPostingLists[fid]; !ok {
			w.fieldPostingLists[fid] = make(postingLists, 0)
		}
		postingLists := w.fieldPostingLists[fid]

		// TODO(tylerw): lookup the tokenizer to use for this field. It
		// may not always be tritokenizer.
		w.tokenizer.Reset(bufio.NewReader(bytes.NewReader(field.Contents())))
		for {
			tok, err := w.tokenizer.Next()
			if err != nil {
				break
			}
			ngram := string(tok.Ngram())
			postingLists[ngram] = append(postingLists[ngram], doc.ID())
		}

		if field.Stored() {
			storedFieldKey := w.storedFieldKey(doc.ID(), fid)
			w.batch.Set(storedFieldKey, field.Contents(), nil)
		}
	}
	return nil
}

func (w *Writer) AddFileByDigest(name string, digest *repb.Digest, contents []byte) error {
	if len(contents) > maxFileLen {
		w.log.Infof("%s: too long, ignoring\n", name)
		return nil
	}
	r := bytes.NewReader(contents)
	mtype, err := mimetype.DetectReader(r)
	if err == nil && skipMime.MatchString(mtype.String()) {
		w.log.Infof("%q: skipping (invalid mime type: %q)", name, mtype.String())
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
			1: namedField{textField, "filename", []byte(name), true /*=stored*/},
			2: namedField{textField, "body", contents, true /*=stored*/},
		},
	}

	return w.AddDocument(doc)
}

func (w *Writer) Flush() error {
	mu := sync.Mutex{}
	flushBatch := func() error {
		if w.batch.Empty() {
			return nil
		}
		if err := w.batch.Commit(pebble.Sync); err != nil {
			return err
		}
		w.log.Debugf("flushed batch")
		w.batch = w.db.NewBatch()
		return nil
	}
	eg := new(errgroup.Group)
	eg.SetLimit(runtime.GOMAXPROCS(0))
	writePLs := func(key []byte, ids []uint32) error {
		buf := new(bytes.Buffer)
		pl := roaring.BitmapOf(ids...)
		if _, err := pl.WriteTo(buf); err != nil {
			return err
		}
		mu.Lock()
		defer mu.Unlock()
		w.log.Debugf("Set %q", string(key))
		if err := w.batch.Set(key, buf.Bytes(), nil); err != nil {
			return err
		}
		if w.batch.Len() >= npost {
			if err := flushBatch(); err != nil {
				return err
			}
		}
		return nil
	}
	for fieldNumber, postingLists := range w.fieldPostingLists {
		for ngram, docIDs := range postingLists {
			writePLs(w.postingListKey(ngram, fieldNumber), docIDs)
		}
	}
	return flushBatch()
}
