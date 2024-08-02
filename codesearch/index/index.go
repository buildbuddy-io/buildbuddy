package index

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"regexp"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/codesearch/posting"
	"github.com/buildbuddy-io/buildbuddy/codesearch/token"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/cockroachdb/pebble"
	"github.com/google/uuid"
	"github.com/xiam/s-expr/ast"
	"github.com/xiam/s-expr/parser"
	"golang.org/x/exp/maps"
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

var sep = []byte(keySeparator)

func splitRight1(s, sep []byte) [][]byte {
	i := bytes.LastIndex(s, sep)
	if i == -1 {
		return [][]byte{s}
	}

	r := [][]byte{
		s[i+1:],
	}
	if len(s[:i]) > 0 {
		r = append(r, s[:i])
	}
	return r
}

func (k *key) FromBytes(b []byte) error {
	// Parse namespace
	chunks := bytes.SplitN(b, sep, 2)
	if len(chunks) != 2 {
		return status.InternalErrorf("error parsing namespace key: %q", b)
	}
	k.namespace, b = string(chunks[0]), chunks[1]

	// Parse key type
	chunks = bytes.SplitN(b, sep, 2)
	if len(chunks) != 2 {
		return status.InternalErrorf("error parsing key type key: %q", b)
	}
	k.keyType, b = indexKeyType(chunks[0]), chunks[1]

	// Next field is data, which may contain the separator character! So
	// we parse the two safe fields from the end now, and whatever is left
	// is data.

	// Parse segmentID.
	chunks = splitRight1(b, sep)
	if len(chunks) != 2 {
		return status.InternalErrorf("error parsing segment ID key: %q", b)
	}
	k.segmentID, b = string(chunks[0]), chunks[1]

	// Parse field, and remainder is data.
	chunks = splitRight1(b, sep)
	if len(chunks) != 2 {
		return status.InternalErrorf("error parsing field key: %q", b)
	}
	k.field, k.data = string(chunks[0]), chunks[1]
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
	// Example: gr12345:gra:foo:content:1234-asdad-123132-asdasd-123
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
			case types.SparseNgramField:
				w.tokenizers[field.Type()] = token.NewSparseNgramTokenizer(token.WithMaxNgramLength(6))
			case types.TrigramField:
				w.tokenizers[field.Type()] = token.NewTrigramTokenizer()
			case types.StringTokenField:
				w.tokenizers[field.Type()] = token.NewWhitespaceTokenizer()
			default:
				return status.InternalErrorf("No tokenizer known for field type: %q", field.Type())
			}
		}
		tokenizer := w.tokenizers[field.Type()]
		tokenizer.Reset(bytes.NewReader(field.Contents()))

		for tokenizer.Next() == nil {
			ngram := string(tokenizer.Ngram())
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
	fieldNames := maps.Keys(w.fieldPostingLists)
	sort.Strings(fieldNames)
	for _, fieldName := range fieldNames {
		postingLists := w.fieldPostingLists[fieldName]
		log.Printf("field: %q had %d ngrams", fieldName, len(postingLists))
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

func (r *Reader) storedFieldKey(docID uint64, field string) []byte {
	return []byte(fmt.Sprintf("%s:doc:%d:%s", r.namespace, docID, field))
}

func (r *Reader) deletedDocPrefix(docID uint64) []byte {
	return []byte(fmt.Sprintf("%s:del:%d::", r.namespace, docID))
}

func (r *Reader) allDocIDs() (posting.FieldMap, error) {
	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: r.storedFieldKey(0, types.DocIDField),
		UpperBound: []byte(fmt.Sprintf("%s:doc:\xff", r.namespace)),
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	resultSet := posting.NewList()
	k := key{}

	fieldSet := make(map[string]struct{})
	for iter.First(); iter.Valid(); iter.Next() {
		if err := k.FromBytes(iter.Key()); err != nil {
			return nil, err
		}
		if k.keyType == docField && k.field == types.DocIDField {
			resultSet.Add(BytesToUint64(iter.Value()))
		} else {
			fieldSet[k.field] = struct{}{}
		}
		continue
	}
	fm := posting.NewFieldMap()
	for fieldName := range fieldSet {
		fm.OrField(fieldName, resultSet)
	}
	return fm, nil
}

func (r *Reader) getStoredFields(docID uint64, fieldNames ...string) (map[string]types.NamedField, error) {
	docIDStart := r.storedFieldKey(docID, "")
	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: docIDStart,
		UpperBound: r.storedFieldKey(docID, "\xff"),
	})
	if err != nil {
		return nil, err
	}
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
	return fields, nil
}

func (r *Reader) GetStoredDocument(docID uint64) (types.Document, error) {
	return r.newLazyDoc(docID), nil
}

// postingList looks up the set of docIDs matching the provided ngram.
// If `field` is set to a non-empty value, matches are restricted to just the
// specified field. Otherwise, all fields are searched.
// If `restrict` is set to a non-empty value, matches will only be returned if
// they are both found and also are present in the restrict set.
func (r *Reader) postingList(ngram []byte, restrict posting.FieldMap, field string) (posting.FieldMap, error) {
	minKey := []byte(fmt.Sprintf("%s:gra:%s:%s", r.namespace, ngram, field))
	maxKey := append(minKey, byte('\xff'))
	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: minKey,
		UpperBound: maxKey,
	})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	k := key{}
	resultSet := posting.NewFieldMap()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := k.FromBytes(iter.Key()); err != nil {
			return nil, err
		}
		if k.keyType != ngramField {
			return nil, status.FailedPreconditionErrorf("key %q not ngram field!", iter.Key())
		}
		if field != types.AllFields && field != k.field {
			break
		}
		if !bytes.Equal(ngram, k.data) {
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
	log.Debugf("postingList(%q, ..., %q) found %d results", ngram, field, resultSet.GetCardinality())
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
		var list posting.FieldMap
		for _, subQuery := range q.List()[1:] {
			l, err := r.postingQuery(subQuery, restrict)
			if err != nil {
				return nil, err
			}
			if list == nil {
				list = l
			} else {
				list.And(l)
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
	iter, err := r.db.NewIter(&pebble.IterOptions{
		LowerBound: r.deletedDocPrefix(0),
		UpperBound: []byte(fmt.Sprintf("%s:del:\xff", r.namespace)),
	})
	if err != nil {
		return err
	}
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

type docMatch struct {
	docid           uint64
	matchedPostings map[string]types.Posting
}

func (dm *docMatch) FieldNames() []string {
	return maps.Keys(dm.matchedPostings)
}
func (dm *docMatch) Docid() uint64 {
	return dm.docid
}
func (dm *docMatch) Posting(fieldName string) types.Posting {
	return dm.matchedPostings[fieldName]
}

type lazyDoc struct {
	r *Reader

	id     uint64
	fields map[string]types.NamedField
}

func (d lazyDoc) ID() uint64 {
	return d.id
}

func (d lazyDoc) Field(name string) types.Field {
	if f, ok := d.fields[name]; ok {
		return f
	}
	fm, err := d.r.getStoredFields(d.id, name)
	if err == nil {
		d.fields[name] = fm[name]
	}
	return d.fields[name]
}

func (d lazyDoc) Fields() []string {
	return maps.Keys(d.fields)
}

func (r *Reader) newLazyDoc(docid uint64) *lazyDoc {
	return &lazyDoc{
		r:      r,
		id:     docid,
		fields: make(map[string]types.NamedField, 0),
	}
}

func (r *Reader) RawQuery(squery []byte) ([]types.DocumentMatch, error) {
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
	if err != nil {
		return nil, err
	}

	docMatches := make(map[uint64]*docMatch, 0)
	for field, pl := range bm {
		for _, docid := range pl.ToArray() {
			if _, ok := docMatches[docid]; !ok {
				docMatches[docid] = &docMatch{
					docid:           docid,
					matchedPostings: make(map[string]types.Posting),
				}
			}
			docMatch := docMatches[docid]
			docMatch.matchedPostings[field] = nil // TODO(tylerw): fill in.
		}
	}

	// Convert to interface (ugh).
	matches := make([]types.DocumentMatch, len(docMatches))
	i := 0
	for _, dm := range docMatches {
		matches[i] = types.DocumentMatch(dm)
		i++
	}
	return matches, nil
}
