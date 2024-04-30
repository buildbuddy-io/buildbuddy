package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"regexp"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/xiam/s-expr/ast"
	"github.com/xiam/s-expr/parser"
	"golang.org/x/exp/slices"
	"golang.org/x/sync/errgroup"
)

var fieldNameRegex = regexp.MustCompile(`^([a-zA-Z0-9][a-zA-Z0-9_]*)$`)

const batchFlushSizeBytes = 1_000_000_000 // flush batch every 1G

type postingLists map[string][]uint64

type Writer struct {
	db  *pebble.DB
	log log.Logger

	namespace         string
	segmentID         uuid.UUID
	tokenizers        map[types.FieldType]types.Tokenizer
	fieldPostingLists map[string]postingLists
	batch             *pebble.Batch
}

func NewWriter(db *pebble.DB, namespace string) (*Writer, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	subLog := log.NamedSubLogger(fmt.Sprintf("writer:%s (%s)", namespace, id.String()))
	return &Writer{
		db:                db,
		log:               subLog,
		namespace:         namespace,
		segmentID:         id,
		tokenizers:        make(map[types.FieldType]types.Tokenizer),
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

type indexKeyType string

const (
	// All stored doc fields are stored under the `docField` key, including
	// docid.
	docField indexKeyType = "doc"

	// Any searchable grams, of any length, are stored under `ngram` field.
	ngramField indexKeyType = "gra"

	// Deleted doc tombstones are stored under `deleteField`.
	deleteField indexKeyType = "del"

	// Keys are separated by `keySeparator`. The general key form is:
	// <namespace>:<key_type>:<contents>:<field_name>:<segment_id>
	keySeparator = ":"
)

type key struct {
	namespace string
	keyType   indexKeyType
	data      []byte
	field     string
	segmentID string
}

func (k *key) FromBytes(b []byte) error {
	segments := bytes.Split(b, []byte(keySeparator))
	if len(segments) < 5 {
		return status.InternalErrorf("invalid key %q (<5 segments)", b)
	}
	k.namespace = string(segments[0])
	k.keyType = indexKeyType(segments[1])
	k.data = segments[2]
	k.field = string(segments[3])
	k.segmentID = string(segments[4])
	return nil
}
func (k *key) DocID() uint64 {
	if k.keyType != docField && k.keyType != deleteField {
		return 0
	}
	d, err := strconv.ParseUint(string(k.data), 10, 64)
	if err == nil {
		return d
	}
	return 0
}
func (k *key) NGram() []byte {
	if k.keyType != ngramField {
		return nil
	}
	return k.data
}

func (w *Writer) storedFieldKey(docID uint64, field string) []byte {
	return []byte(fmt.Sprintf("%s:doc:%d:%s:%s", w.namespace, docID, field, w.segmentID.String()))
}

func (w *Writer) postingListKey(ngram string, field string) []byte {
	return []byte(fmt.Sprintf("%s:gra:%s:%s:%s", w.namespace, ngram, field, w.segmentID.String()))
}

func (w *Writer) deleteKey(docID uint64) []byte {
	return []byte(fmt.Sprintf("%s:del:%d:%s:%s", w.namespace, docID, "", w.segmentID.String()))
}

func (w *Writer) DeleteDocument(docID uint64) error {
	docidKey := w.deleteKey(docID)
	w.batch.Set(docidKey, Uint64ToBytes(docID), nil)
	return nil
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

		// Lookup the tokenizer to use; if one has not already been
		// created for this field type then make it.
		if _, ok := w.tokenizers[field.Type()]; !ok {
			switch field.Type() {
			case types.TrigramField:
				w.tokenizers[field.Type()] = NewTrigramTokenizer()
			case types.StringTokenField:
				w.tokenizers[field.Type()] = NewWhitespaceTokenizer()
			default:
				return status.InternalErrorf("No tokenizer known for field type: %q", field.Type())
			}
		}
		tokenizer := w.tokenizers[field.Type()]
		tokenizer.Reset(bytes.NewReader(field.Contents()))
		for {
			tok, err := tokenizer.Next()
			if err != nil {
				break
			}
			// TODO(tylerw): maybe shouldn't call this ngram.
			// What if it's a number?
			ngram := string(tok.Ngram())
			postingLists[ngram] = append(postingLists[ngram], doc.ID())
		}

		if field.Stored() {
			storedFieldKey := w.storedFieldKey(doc.ID(), field.Name())
			w.batch.Set(storedFieldKey, field.Contents(), nil)
		}
	}
	if w.batch.Len() >= batchFlushSizeBytes {
		if err := w.flushBatch(); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) flushBatch() error {
	if w.batch.Empty() {
		return nil
	}
	w.log.Infof("Batch size is %d", w.batch.Len())
	if err := w.batch.Commit(pebble.NoSync); err != nil {
		return err
	}
	w.log.Debugf("flushed batch")
	w.batch = w.db.NewBatch()
	return nil
}

func (w *Writer) Flush() error {
	mu := sync.Mutex{}
	eg := new(errgroup.Group)
	eg.SetLimit(runtime.GOMAXPROCS(0))
	writePLs := func(key []byte, ids []uint64) error {
		pl := posting.NewList(ids...)

		buf, err := pl.Marshal()
		if err != nil {
			return err
		}

		mu.Lock()
		defer mu.Unlock()
		w.log.Debugf("Set %q", string(key))
		if err := w.batch.Set(key, buf, nil); err != nil {
			return err
		}
		if w.batch.Len() >= batchFlushSizeBytes {
			if err := w.flushBatch(); err != nil {
				return err
			}
		}
		return nil
	}
	for fieldName, postingLists := range w.fieldPostingLists {
		for ngram, docIDs := range postingLists {
			ngram := ngram
			fieldName := fieldName
			docIDs := docIDs
			eg.Go(func() error {
				return writePLs(w.postingListKey(ngram, fieldName), docIDs)
			})
		}
	}
	if err := eg.Wait(); err != nil {
		return err
	}
	return w.flushBatch()
}

type Reader struct {
	db  pebble.Reader
	log log.Logger

	namespace string
}

func NewReader(db pebble.Reader, namespace string) *Reader {
	subLog := log.NamedSubLogger(fmt.Sprintf("reader-%s", namespace))
	return &Reader{
		db:        db,
		log:       subLog,
		namespace: namespace,
	}
}

func (r *Reader) DumpPosting() {
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{0},
		UpperBound: []byte{math.MaxUint8},
	})
	defer iter.Close()
	postingList := posting.NewList()
	k := key{}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := k.FromBytes(iter.Key()); err != nil {
			return
		}
		if k.keyType != ngramField {
			continue
		}
		if _, err := postingList.Unmarshal(iter.Value()); err != nil {
			return
		}
		fmt.Printf("%q: %d\n", k.NGram(), postingList.GetCardinality())
	}
}

func (r *Reader) storedFieldKey(docID uint64, field string) []byte {
	return []byte(fmt.Sprintf("%s:doc:%d:%s", r.namespace, docID, field))
}

func (r *Reader) deletedDocPrefix(docID uint64) []byte {
	return []byte(fmt.Sprintf("%s:del:%d::", r.namespace, docID))
}

func (r *Reader) allDocIDs() (posting.FieldMap, error) {
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: r.storedFieldKey(0, types.DocIDField),
		UpperBound: []byte(fmt.Sprintf("%s:doc:\xff", r.namespace)),
	})
	defer iter.Close()
	resultSet := posting.NewList()
	k := key{}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := k.FromBytes(iter.Key()); err != nil {
			return nil, err
		}
		if k.keyType == docField && k.field == types.DocIDField {
			resultSet.Add(BytesToUint64(iter.Value()))
		}
		continue
	}
	fm := posting.NewFieldMap()
	fm.OrField("", resultSet)
	return fm, nil
}

func (r *Reader) GetStoredDocument(docID uint64, fieldNames ...string) (types.Document, error) {
	docIDStart := r.storedFieldKey(docID, "")
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: docIDStart,
		UpperBound: r.storedFieldKey(docID, "\xff"),
	})
	defer iter.Close()

	shouldCopyField := func(fieldName string) bool {
		if len(fieldNames) == 0 {
			return true
		}
		for _, allowedFieldName := range fieldNames {
			if allowedFieldName == fieldName {
				return true
			}
		}
		return false
	}

	fields := make(map[string]types.NamedField, 0)
	k := key{}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := k.FromBytes(iter.Key()); err != nil {
			return nil, err
		}
		if k.keyType != docField || k.field == types.DocIDField {
			// Skip docID -- we already have it from args.
			continue
		}

		if !shouldCopyField(k.field) {
			continue
		}
		fieldVal := make([]byte, len(iter.Value()))
		copy(fieldVal, iter.Value())
		fields[k.field] = types.NewNamedField(types.TrigramField, k.field, fieldVal, true /*=stored*/)
	}
	doc := types.NewMapDocument(docID, fields)
	return doc, nil
}

// postingList looks up the set of docIDs matching the provided ngram.
// If `field` is set to a non-empty value, matches are restricted to just the
// specified field. Otherwise, all fields are searched.
// If `restrict` is set to a non-empty value, matches will only be returned if
// they are both found and also are present in the restrict set.
func (r *Reader) postingList(ngram []byte, restrict posting.FieldMap, field string) (posting.FieldMap, error) {
	minKey := []byte(fmt.Sprintf("%s:gra:%s", r.namespace, ngram))
	if field != types.AllFields {
		minKey = []byte(fmt.Sprintf("%s:gra:%s:%s", r.namespace, ngram, field))
	}
	maxKey := append(minKey, byte('\xff'))
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: minKey,
		UpperBound: maxKey,
	})
	defer iter.Close()

	resultSet := posting.NewFieldMap()

	k := key{}
	for iter.First(); iter.Valid(); iter.Next() {
		if err := k.FromBytes(iter.Key()); err != nil {
			return nil, err
		}
		if k.keyType != ngramField {
			continue
		}
		postingList := posting.NewList()
		if _, err := postingList.Unmarshal(iter.Value()); err != nil {
			return nil, err
		}
		resultSet.OrField(k.field, postingList)
	}
	if restrict.GetCardinality() > 0 {
		resultSet.And(restrict)
	}
	return resultSet, nil
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

func (r *Reader) postingQuery(q *ast.Node, restrict posting.FieldMap) (posting.FieldMap, error) {
	op, err := qOp(q)
	if err != nil {
		return nil, err
	}
	switch op {
	case QNone:
		return posting.NewFieldMap(), nil
	case QAll:
		if restrict != nil && restrict.GetCardinality() > 0 {
			return restrict, nil
		}
		return r.allDocIDs()
	case QAnd:
		list := restrict
		for _, subQuery := range q.List()[1:] {
			list, err = r.postingQuery(subQuery, list)
			if err != nil {
				return nil, err
			}
		}
		return list, nil
	case QOr:
		list := posting.NewFieldMap()
		for _, subQuery := range q.List()[1:] {
			l, err := r.postingQuery(subQuery, restrict)
			if err != nil {
				return nil, err
			}
			list.Or(l)
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
		pl, err := r.postingList([]byte(ngram), restrict, field)
		if err != nil {
			return nil, err
		}

		return pl, nil
	default:
		return nil, status.FailedPreconditionErrorf("Unknown query op: %q", op)
	}
}

func (r *Reader) removeDeletedDocIDs(results posting.FieldMap) error {
	docids := results.ToPosting()
	if docids.GetCardinality() == 0 {
		return nil
	}
	iter := r.db.NewIter(&pebble.IterOptions{
		LowerBound: r.deletedDocPrefix(0),
		UpperBound: []byte(fmt.Sprintf("%s:del:\xff", r.namespace)),
	})
	defer iter.Close()

	arr := docids.ToArray()
	lastDocKeyPrefix := r.deletedDocPrefix(arr[len(arr)-1])

	k := key{}
	for _, docID := range arr {
		deletedDocKeyPrefix := r.deletedDocPrefix(docID)
		valid := iter.SeekGE(deletedDocKeyPrefix)
		if !valid || bytes.Compare(iter.Key(), lastDocKeyPrefix) > 1 {
			return nil
		}
		if err := k.FromBytes(iter.Key()); err != nil {
			return err
		}
		if k.keyType == deleteField && k.DocID() == docID {
			results.Remove(docID)
		}
	}
	return nil
}

func (r *Reader) RawQuery(squery []byte) (map[string][]uint64, error) {
	start := time.Now()
	root, err := parser.Parse(squery)
	if err != nil {
		return nil, err
	}
	// Unwrap the expression tree if it's wrapped in an outer list.
	if root.Type() == ast.NodeTypeList && len(root.List()) == 1 {
		root = root.List()[0]
	}
	r.log.Infof("Took %s to parse squery", time.Since(start))
	start = time.Now()
	bm, err := r.postingQuery(root, posting.NewFieldMap())
	if err != nil {
		return nil, err
	}

	r.log.Infof("Took %s to query posts", time.Since(start))
	start = time.Now()
	err = r.removeDeletedDocIDs(bm)
	r.log.Infof("Took %s to remove deleted docs", time.Since(start))
	return bm.Map(), err
}
