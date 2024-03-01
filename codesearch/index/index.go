package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

const (
	npost = 64 << 20 / 8 // 64 MB worth of post entries
)

type FieldType int32

const (
	TextField FieldType = iota
)

type postingLists map[string][]uint64

type Writer struct {
	db  *pebble.DB
	log log.Logger

	repo              string
	segmentID         uuid.UUID
	tokenizer         *TrigramTokenizer
	fieldPostingLists map[string]postingLists
	batch             *pebble.Batch
}

func NewWriter(db *pebble.DB, repo string) (*Writer, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	subLog := log.NamedSubLogger(fmt.Sprintf("writer:%s (%s)", repo, id.String()))
	return &Writer{
		db:                db,
		log:               subLog,
		repo:              repo,
		segmentID:         id,
		tokenizer:         NewTrigramTokenizer(TextField),
		fieldPostingLists: make(map[string]postingLists),
		batch:             db.NewBatch(),
	}, nil
}

type Field interface {
	Type() FieldType
	Name() string
	Contents() []byte
	Stored() bool
}

type Document interface {
	ID() uint64
	Fields() []string
	Field(string) Field
	// TODO(tylerw): add Boost() float64
}

func BytesToUint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

func Uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	return buf
}

func docidKey(repo string, docID uint64) []byte {
	return []byte(fmt.Sprintf("%s:did:%d", repo, docID))
}

func (w *Writer) storedFieldKey(docID uint64, field string) []byte {
	return []byte(fmt.Sprintf("%s:doc:%d:%s", w.repo, docID, field))
}

func (w *Writer) postingListKey(ngram string, field string) []byte {
	return []byte(fmt.Sprintf("%s:gra:%s:%s", w.repo, ngram, field))
}

func (w *Writer) AddDocument(doc Document) error {
	for _, fieldName := range doc.Fields() {
		field := doc.Field(fieldName)
		if _, ok := w.fieldPostingLists[field.Name()]; !ok {
			w.fieldPostingLists[field.Name()] = make(postingLists, 0)
		}
		postingLists := w.fieldPostingLists[field.Name()]

		// **Always store DocID.**
		w.batch.Set(docidKey(w.repo, doc.ID()), Uint64ToBytes(doc.ID()), nil)

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
			storedFieldKey := w.storedFieldKey(doc.ID(), field.Name())
			w.batch.Set(storedFieldKey, field.Contents(), nil)
		}
	}
	return nil
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
	writePLs := func(key []byte, ids []uint64) error {
		buf := new(bytes.Buffer)
		pl := roaring64.BitmapOf(ids...)
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
	for fieldName, postingLists := range w.fieldPostingLists {
		for ngram, docIDs := range postingLists {
			writePLs(w.postingListKey(ngram, fieldName), docIDs)
		}
	}
	return flushBatch()
}

type Reader struct {
	db  pebble.Reader
	log log.Logger

	repo string
}

func NewReader(db pebble.Reader, repo string) *Reader {
	subLog := log.NamedSubLogger(fmt.Sprintf("reader-%s", repo))
	return &Reader{
		db:   db,
		log:  subLog,
		repo: repo,
	}
}

func (r *Reader) allIndexedFiles() ([]uint64, error) {
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: docidKey(r.repo, 0),
		UpperBound: docidKey(r.repo, math.MaxUint64),
	})
	defer iter.Close()
	found := make([]uint64, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		docid := BytesToUint64(iter.Value())
		found = append(found, docid)
	}
	return found, nil
}

func (r *Reader) storedFieldKey(docID uint64, field string) []byte {
	return []byte(fmt.Sprintf("%s:doc:%d:%s", r.repo, docID, field))
}

func (r *Reader) GetStoredFieldValue(docID uint64, field string) ([]byte, error) {
	buf, closer, err := r.db.Get(r.storedFieldKey(docID, field))
	if err == pebble.ErrNotFound {
		return nil, status.NotFoundErrorf("doc %d (field %q) not found", docID, field)
	}
	defer closer.Close()
	rbuf := make([]byte, len(buf))
	copy(rbuf, buf)
	return rbuf, nil
}

// postingListBM looks up the set of docIDs matching the provided ngram.
// If `field` is set to a non-empty value, matches are restricted to just the
// specified field. Otherwise, all fields are searched.
func (r *Reader) postingListBM(ngram []byte, restrict *roaring64.Bitmap, field string) (*roaring64.Bitmap, error) {
	minKey := []byte(fmt.Sprintf("%s:gra:%s", r.repo, ngram))
	if field != "" {
		minKey = []byte(fmt.Sprintf("%s:gra:%s:%s", r.repo, ngram, field))
	}
	maxKey := append(minKey, byte('\xff'))
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: minKey,
		UpperBound: maxKey,
	})
	defer iter.Close()

	resultSet := roaring64.New()
	postingList := roaring64.New()
	for iter.First(); iter.Valid(); iter.Next() {
		r.log.Infof("query %q matched key %q", ngram, iter.Key())
		if _, err := postingList.ReadFrom(bytes.NewReader(iter.Value())); err != nil {
			return nil, err
		}
		resultSet = roaring64.Or(resultSet, postingList)
		postingList.Clear()
	}
	if !restrict.IsEmpty() {
		resultSet.And(restrict)
	}
	return resultSet, nil
}

func (r *Reader) PostingList(trigram uint32) ([]uint64, error) {
	return r.postingList(trigram, nil)
}

func (r *Reader) postingList(trigram uint32, restrict []uint64) ([]uint64, error) {
	bm, err := r.postingListBM(trigramToBytes(trigram), roaring64.BitmapOf(restrict...), "")
	if err != nil {
		return nil, err
	}
	return bm.ToArray(), nil
}

func (r *Reader) PostingAnd(list []uint64, trigram uint32) ([]uint64, error) {
	return r.postingAnd(list, trigram, nil)
}

func (r *Reader) postingAnd(list []uint64, trigram uint32, restrict []uint64) ([]uint64, error) {
	bm, err := r.postingListBM(trigramToBytes(trigram), roaring64.BitmapOf(restrict...), "")
	if err != nil {
		return nil, err
	}
	bm.And(roaring64.BitmapOf(list...))
	return bm.ToArray(), nil
}

func (r *Reader) PostingOr(list []uint64, trigram uint32) ([]uint64, error) {
	return r.postingOr(list, trigram, nil)
}

func (r *Reader) postingOr(list []uint64, trigram uint32, restrict []uint64) ([]uint64, error) {
	bm, err := r.postingListBM(trigramToBytes(trigram), roaring64.BitmapOf(restrict...), "")
	if err != nil {
		return nil, err
	}
	bm.Or(roaring64.BitmapOf(list...))
	return bm.ToArray(), nil
}

func (r *Reader) PostingQuery(q *query.Query) ([]uint64, error) {
	return r.postingQuery(q, nil)
}

// TODO(tylerw): move this to query??
func (r *Reader) postingQuery(q *query.Query, restrict []uint64) (ret []uint64, err error) {
	var list []uint64
	r.log.Infof("q.Op: %+v", q.Op)
	switch q.Op {
	case query.QNone:
		// nothing
	case query.QAll:
		if restrict != nil {
			return restrict, err
		}
		list, err = r.allIndexedFiles()
	case query.QAnd:
		for _, t := range q.Trigram {
			tri := uint32(t[0])<<16 | uint32(t[1])<<8 | uint32(t[2])
			if list == nil {
				list, err = r.postingList(tri, restrict)
			} else {
				list, err = r.postingAnd(list, tri, restrict)
			}
			if len(list) == 0 {
				return nil, err
			}
		}
		for _, sub := range q.Sub {
			if list == nil {
				list = restrict
			}
			list, err = r.postingQuery(sub, list)
			if len(list) == 0 {
				return nil, err
			}
		}
	case query.QOr:
		for _, t := range q.Trigram {
			tri := uint32(t[0])<<16 | uint32(t[1])<<8 | uint32(t[2])
			if list == nil {
				list, err = r.postingList(tri, restrict)
			} else {
				list, err = r.postingOr(list, tri, restrict)
			}
		}
		for _, sub := range q.Sub {
			l, err := r.postingQuery(sub, restrict)
			if err != nil {
				return nil, err
			}
			list = r.mergeOr(list, l)
		}
	}
	return list, err
}

func (r *Reader) mergeOr(l1, l2 []uint64) []uint64 {
	l := append(l1, l2...)
	slices.Sort(l)
	return slices.Compact(l)
}
