package types

import (
	"io"
)

type FieldType int32

const (
	TrigramField FieldType = iota
	KeywordField
	SparseNgramField

	DocIDField   = "_id"
	DeletesField = "_del"

	// ImportsField holds the import-identity terms a document imports.
	// ImportIDField holds the document's own import-identity terms; it is
	// known to the index so its value can be carried in the per-doc stats
	// record and resolved into scoring signals at query time.
	ImportsField  = "imports"
	ImportIDField = "import_id"

	// SymbolsField holds the names the document declares (tree-sitter
	// extracted, lowercased), so symbol-definition matches can be scored
	// above usage and substring matches.
	SymbolsField = "symbols"

	// SignalImportInDegree is the number of documents whose ImportsField
	// contains one of this document's ImportIDField terms.
	SignalImportInDegree = "import_indegree"
)

func (ft FieldType) String() string {
	switch ft {
	case TrigramField:
		return "TrigramToken"
	case KeywordField:
		return "KeywordToken"
	case SparseNgramField:
		return "SparseNgramToken"
	default:
		return "UNKNOWN FIELD TYPE"
	}
}

type FieldSchema interface {
	Type() FieldType
	Name() string
	Stored() bool
	MakeTokenizer() Tokenizer
	MakeField([]byte) Field
	String() string
}

type DocumentSchema interface {
	Fields() []FieldSchema
	Field(name string) FieldSchema
	MakeDocument(map[string][]byte) (Document, error)
	MustMakeDocument(map[string][]byte) Document
}

type Field interface {
	Contents() []byte
	Type() FieldType
	Name() string
	Schema() FieldSchema
}

type Document interface {
	Fields() []string
	Field(string) Field
	// TODO(tylerw): add Boost() float64
}

type Posting interface {
	Docid() uint64
	Frequency() uint32
}

type DocumentMatch interface {
	Docid() uint64
	FieldNames() []string
	Posting(fieldName string) Posting
	FieldLength(fieldName string) uint32
	// Signal returns the named per-document scoring signal, or 0 if the
	// signal is absent or has not been resolved. Signals are attached to
	// matches by a SignalResolver before scoring.
	Signal(name string) float64
}

// SignalResolver computes per-document scoring signals for a set of matches
// and attaches them so scorers can read them via DocumentMatch.Signal.
// Resolution is requested explicitly so queries that don't score on signals
// pay nothing. Implemented by index.Reader.
type SignalResolver interface {
	ResolveSignals(matches []DocumentMatch, names ...string) error
}

type Tokenizer interface {
	Reset(io.Reader)
	Next() error

	Type() FieldType
	Ngram() []byte
	NgramString() string
	IterateTermFrequencies(func(ngram string, frequency uint32))
	TermFrequencyStats() TermFrequencyStats
}

// NumTFLog2Buckets is the size of CountsByLog2Bucket. It covers every
// possible uint32 frequency: bucket index = bits.Len(uint(tf-1)), so tf=1
// maps to 0 and tf == math.MaxUint32 maps to 32.
const NumTFLog2Buckets = 33

type TermFrequencyStats struct {
	Occurrences          int64
	UniquePostings       int64
	DuplicateOccurrences int64
	DuplicatePostings    int64
	// RLEBytesEstimate is the on-disk cost of the frequency tail when written
	// in the run-length-encoded form used by the posting list serializer
	// (sum of uvarint(runlen)+uvarint(value) per run).
	RLEBytesEstimate int64
	// CountsByLog2Bucket[i] is the number of postings whose term frequency
	// falls in (2^(i-1), 2^i], with bucket 0 reserved for tf == 1. Compute
	// the index with bits.Len(uint(tf - 1)).
	CountsByLog2Bucket [NumTFLog2Buckets]int64
}

type IndexWriter interface {
	AddDocument(doc Document) error
	UpdateDocument(matchField Field, newDoc Document) error
	DeleteDocument(docID uint64) error
	DeleteDocumentByMatchField(matchField Field) error
	Flush() error
}

type IndexReader interface {
	GetStoredDocument(docID uint64) Document
	RawQuery(squery string) ([]DocumentMatch, error)
}

type Scorer interface {
	Skip() bool
	// Score computes a relevance score for a matched document using only
	// index-side data (term frequencies and field lengths) — it must not need
	// the stored document, so it stays cheap enough to run on every match.
	Score(docMatch DocumentMatch) float64
	// Prepare precomputes statistics over the full candidate set (e.g.
	// average field length for BM25 normalization) before per-document
	// scoring begins. Searchers must call Prepare once with all candidate
	// matches before the first Score call. Prepare mutates scorer state, so
	// a Scorer is single-use per search and must not be shared across
	// concurrent Search calls.
	Prepare(matches []DocumentMatch)
}

type HighlightedRegion interface {
	FieldName() string
	String() string
	Line() int
	CustomSnippet(linesBefore, linesAfter int) string
}

type Highlighter interface {
	Highlight(doc Document) []HighlightedRegion
}

type Query interface {
	SQuery() string
	Scorer() Scorer
}

type Searcher interface {
	Search(q Query, numResults, offset int) ([]Document, error)
}
