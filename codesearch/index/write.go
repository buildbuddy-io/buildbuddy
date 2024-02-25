package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"runtime"
	"sync"

	"github.com/RoaringBitmap/roaring"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
)

const(
	npost = 64 << 20 / 8 // 64 MB worth of post entries
)

type FieldType int32

const (
	TextField FieldType = iota
)

type postingLists map[string][]uint32

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
	ID() uint32
	Fields() []string
	Field(string) Field
	// TODO(tylerw): add Boost() float64
}

func BytesToUint32(buf []byte) uint32 {
	return binary.LittleEndian.Uint32(buf)
}

func docidKey(repo string, docID uint32) []byte {
	return []byte(fmt.Sprintf("%s:did:%d", repo, docID))
}

func (w *Writer) storedFieldKey(docID uint32, field string) []byte {
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
		w.batch.Set(docidKey(w.repo, doc.ID()), uint32ToBytes(doc.ID()), nil)

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

func (r *Reader) allIndexedFiles() ([]uint32, error) {
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: docidKey(r.repo, 0),
		UpperBound: docidKey(r.repo, math.MaxUint32),
	})
	defer iter.Close()
	found := make([]uint32, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		docid := BytesToUint32(iter.Value())
		found = append(found, docid)
	}
	return found, nil
}

func (r *Reader) storedFieldKey(docID uint32, field string) []byte {
	return []byte(fmt.Sprintf("%s:doc:%d:%s", r.repo, docID, field))
}

func (r *Reader) GetStoredFieldValue(docid uint32, field string) ([]byte, error) {
	buf, closer, err := r.db.Get(r.storedFieldKey(docid, field))
	if err == pebble.ErrNotFound {
		return nil, status.NotFoundErrorf("doc %d (field %q) not found", docid, field)
	}
	defer closer.Close()
	rbuf := make([]byte, len(buf))
	copy(rbuf, buf)
	return rbuf, nil
}

func (r *Reader) postingListBM(ngram []byte, restrict *roaring.Bitmap) (*roaring.Bitmap, error) {
	minKey := []byte(fmt.Sprintf("%s:gra:%s", r.repo, ngram))
	maxKey := append(minKey, byte('\xff'))
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: minKey,
		UpperBound: maxKey,
	})
	defer iter.Close()

	resultSet := roaring.New()
	postingList := roaring.New()
	for iter.First(); iter.Valid(); iter.Next() {
		r.log.Infof("query %q matched key %q", ngram, iter.Key())
		if _, err := postingList.ReadFrom(bytes.NewReader(iter.Value())); err != nil {
			return nil, err
		}
		resultSet = roaring.Or(resultSet, postingList)
		postingList.Clear()
	}
	if !restrict.IsEmpty() {
		resultSet.And(restrict)
	}
	return resultSet, nil
}

func (r *Reader) PostingList(trigram uint32) ([]uint32, error) {
	return r.postingList(trigram, nil)
}

func (r *Reader) postingList(trigram uint32, restrict []uint32) ([]uint32, error) {
	bm, err := r.postingListBM(trigramToBytes(trigram), roaring.BitmapOf(restrict...))
	if err != nil {
		return nil, err
	}
	return bm.ToArray(), nil
}

func (r *Reader) PostingAnd(list []uint32, trigram uint32) ([]uint32, error) {
	return r.postingAnd(list, trigram, nil)
}

func (r *Reader) postingAnd(list []uint32, trigram uint32, restrict []uint32) ([]uint32, error) {
	bm, err := r.postingListBM(trigramToBytes(trigram), roaring.BitmapOf(restrict...))
	if err != nil {
		return nil, err
	}
	bm.And(roaring.BitmapOf(list...))
	return bm.ToArray(), nil
}

func (r *Reader) PostingOr(list []uint32, trigram uint32) ([]uint32, error) {
	return r.postingOr(list, trigram, nil)
}

func (r *Reader) postingOr(list []uint32, trigram uint32, restrict []uint32) ([]uint32, error) {
	bm, err := r.postingListBM(trigramToBytes(trigram), roaring.BitmapOf(restrict...))
	if err != nil {
		return nil, err
	}
	bm.Or(roaring.BitmapOf(list...))
	return bm.ToArray(), nil
}

func (r *Reader) PostingQuery(q *query.Query) ([]uint32, error) {
	return r.postingQuery(q, nil)
}

// TODO(tylerw): move this to query??
func (r *Reader) postingQuery(q *query.Query, restrict []uint32) (ret []uint32, err error) {
	var list []uint32
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

func (r *Reader) mergeOr(l1, l2 []uint32) []uint32 {
	var l []uint32
	i := 0
	j := 0
	for i < len(l1) || j < len(l2) {
		switch {
		case j == len(l2) || (i < len(l1) && l1[i] < l2[j]):
			l = append(l, l1[i])
			i++
		case i == len(l1) || (j < len(l2) && l1[i] > l2[j]):
			l = append(l, l2[j])
			j++
		case l1[i] == l2[j]:
			l = append(l, l1[i])
			i++
			j++
		}
	}
	return l
}
