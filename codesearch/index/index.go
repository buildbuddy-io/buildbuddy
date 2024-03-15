package index

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"strconv"
	"sync"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/xiam/sexpr/ast"
	"github.com/xiam/sexpr/parser"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

var fieldNameRegex = regexp.MustCompile(`^([a-zA-Z0-9_]+)$`)

const npost = 64 << 20 / 8 // 64 MB worth of post entries

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
		tokenizer:         NewTrigramTokenizer(types.TextField),
		fieldPostingLists: make(map[string]postingLists),
		batch:             db.NewBatch(),
	}, nil
}

func BytesToUint64(buf []byte) uint64 {
	return binary.LittleEndian.Uint64(buf)
}

func Uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, i)
	return buf
}

func (w *Writer) storedFieldKey(docID uint64, field string) []byte {
	return []byte(fmt.Sprintf("%s:doc:%d:%s", w.repo, docID, field))
}

func (w *Writer) postingListKey(ngram string, field string) []byte {
	return []byte(fmt.Sprintf("%s:gra:%s:%s", w.repo, ngram, field))
}

func (w *Writer) AddDocument(doc types.Document) error {
	// **Always store DocID.**
	docidKey := w.storedFieldKey(doc.ID(), types.DocIDField)
	w.batch.Set(docidKey, Uint64ToBytes(doc.ID()), nil)

	for _, fieldName := range doc.Fields() {
		if !fieldNameRegex.MatchString(fieldName) {
			return status.InvalidArgumentErrorf("Invalid field name %q", fieldName)
		}
		field := doc.Field(fieldName)
		if _, ok := w.fieldPostingLists[field.Name()]; !ok {
			w.fieldPostingLists[field.Name()] = make(postingLists, 0)
		}
		postingLists := w.fieldPostingLists[field.Name()]

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

func (r *Reader) storedFieldKey(docID uint64, field string) []byte {
	return []byte(fmt.Sprintf("%s:doc:%d:%s", r.repo, docID, field))
}

func (r *Reader) allDocIDs() ([]uint64, error) {
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: r.storedFieldKey(0, types.DocIDField),
		UpperBound: r.storedFieldKey(math.MaxUint64, types.DocIDField),
	})
	defer iter.Close()
	found := make([]uint64, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		if !bytes.HasSuffix(iter.Key(), []byte(types.DocIDField)) {
			continue
		}
		docid := BytesToUint64(iter.Value())
		found = append(found, docid)
	}
	return found, nil
}

func (r *Reader) GetStoredDocument(docID uint64) (types.Document, error) {
	docIDStart := r.storedFieldKey(docID, "")
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: docIDStart,
		UpperBound: r.storedFieldKey(docID, "\xff"),
	})
	defer iter.Close()

	fields := make(map[string]types.NamedField, 0)
	for iter.First(); iter.Valid(); iter.Next() {
		fieldName := string(bytes.TrimPrefix(iter.Key(), docIDStart))
		if fieldName == types.DocIDField {
			// Skip docID -- we already have it from args.
			continue
		}
		fieldVal := make([]byte, len(iter.Value()))
		copy(fieldVal, iter.Value())
		fields[fieldName] = types.NewNamedField(types.TextField, fieldName, fieldVal, true /*=stored*/)
	}
	doc := types.NewMapDocument(docID, fields)
	return doc, nil
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
// If `restrict` is set to a non-empty value, matches will only be returned if
// they are both found and also are present in the restrict set.
func (r *Reader) postingListBM(ngram []byte, restrict *roaring64.Bitmap, field string) (*roaring64.Bitmap, error) {
	minKey := []byte(fmt.Sprintf("%s:gra:%s", r.repo, ngram))
	if field != types.AllFields {
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
		if _, err := postingList.ReadFrom(bytes.NewReader(iter.Value())); err != nil {
			return nil, err
		}
		r.log.Infof("%q matched tok %q [%d]", ngram, iter.Key(), postingList.GetCardinality())
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

func (r *Reader) PostingListF(ngram []byte, field string, restrict []uint64) ([]uint64, error) {
	bm, err := r.postingListBM(ngram, roaring64.BitmapOf(restrict...), field)
	if err != nil {
		return nil, err
	}
	return bm.ToArray(), nil
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

const (
	QNone = ":none"
	QAll  = ":all"

	QAnd = ":and"
	QOr  = ":or"
	QEq  = ":eq"
)

var allowedAtoms = []string{QNone, QAll, QEq, QAnd, QOr}

func qOp(expr *ast.Node) (string, error) {
	if !expr.IsVector() || len(expr.List()) < 1 {
		return "", status.InvalidArgumentErrorf("%q not q-expr with op", expr)
	}
	firstNode := expr.List()[0]
	if firstNode.Type() != ast.NodeTypeAtom {
		return "", status.InvalidArgumentErrorf("%q not atom", expr)
	}
	atomString, ok := firstNode.Value().(string)
	if !ok {
		return "", status.InvalidArgumentErrorf("Query atom: %q not string", expr.Value())
	}
	if !slices.Contains(allowedAtoms, atomString) {
		return "", status.InvalidArgumentErrorf("Unknown query atom: %q", firstNode.Value())
	}
	return atomString, nil
}

func (r *Reader) RawQuery(squery []byte) ([]uint64, error) {
	root, err := parser.Parse(squery)
	if err != nil {
		return nil, err
	}
	// Unwrap the expression tree if it's wrapped in an outer list.
	if root.Type() == ast.NodeTypeList && len(root.List()) == 1 {
		root = root.List()[0]
	}
	return r.postingQuerySX(root, nil)
}

func (r *Reader) postingQuerySX(q *ast.Node, restrict []uint64) ([]uint64, error) {
	op, err := qOp(q)
	if err != nil {
		return nil, err
	}
	switch op {
	case QNone:
		return nil, nil
	case QAll:
		if restrict != nil {
			return restrict, nil
		}
		return r.allDocIDs()
	case QAnd:
		list := restrict
		for _, subQuery := range q.List()[1:] {
			list, err = r.postingQuerySX(subQuery, list)
			if err != nil {
				return nil, err
			}
		}
		return list, nil
	case QOr:
		var list []uint64
		for _, subQuery := range q.List()[1:] {
			l, err := r.postingQuerySX(subQuery, restrict)
			if err != nil {
				return nil, err
			}
			list = r.mergeOr(list, l)
		}
		return list, nil
	case QEq:
		children := q.List()
		if len(children) != 3 {
			return nil, status.InvalidArgumentErrorf("%s expression should have 3 elements: %q (has %d)", QEq, children, len(children))
		}
		field, ok := children[1].Value().(string)
		if !ok {
			return nil, status.InvalidArgumentErrorf("field name %q must be a string", children[1])
		}
		ngram, ok := children[2].Value().(string)
		if !ok {
			return nil, status.InvalidArgumentErrorf("ngram %q must be a string/bytes", children[2])
		}
		// TODO(tylerw): consider if this is the right place
		// for this to happen.
		if s, err := strconv.Unquote(ngram); err == nil {
			ngram = s
		}
		return r.PostingListF([]byte(ngram), field, restrict)
	default:
		return nil, status.FailedPreconditionErrorf("Unknown query op: %q", op)
	}

}

// TODO(tylerw): move this to query??
func (r *Reader) postingQuery(q *query.Query, restrict []uint64) (list []uint64, err error) {
	switch q.Op {
	case query.QNone:
		// nothing
	case query.QAll:
		if restrict != nil {
			return restrict, err
		}
		list, err = r.allDocIDs()
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
