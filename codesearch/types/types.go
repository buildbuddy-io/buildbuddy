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
	Positions() []uint64
	Merge(Posting)
}

type DocumentMatch interface {
	Docid() uint64
	FieldNames() []string
	Posting(fieldName string) Posting
}

type Tokenizer interface {
	Reset(io.Reader)
	Next() error

	Type() FieldType
	Ngram() []byte
}

type IndexWriter interface {
	AddDocument(doc Document) error
	UpdateDocument(matchField Field, newDoc Document) error
	DeleteDocument(docID uint64) error
	Flush() error
}

type IndexReader interface {
	GetStoredDocument(docID uint64) (Document, error)
	GetStoredDocumentByMatchField(matchField Field) (Document, error)
	RawQuery(squery string) ([]DocumentMatch, error)
}

type Scorer interface {
	Skip() bool
	Score(docMatch DocumentMatch, doc Document) float64
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
