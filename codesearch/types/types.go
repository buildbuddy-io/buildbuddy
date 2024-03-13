package types

import (
	"fmt"
	"io"
)

type FieldType int32

const (
	TextField FieldType = iota

	DocIDField = "docid"
	AllFields  = "*"
)

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

type Token interface {
	Field() int32
	Ngram() []byte
}

type Tokenizer interface {
	Reset(io.ByteReader)
	Next() (Token, error)
}

type IndexWriter interface {
	AddDocument(doc Document) error
	Flush() error
}

type IndexReader interface {
	PostingList(trigram uint32) ([]uint64, error)
	PostingListF(ngram []byte, field string) ([]uint64, error)
	PostingAnd(list []uint64, trigram uint32) ([]uint64, error)
	PostingOr(list []uint64, trigram uint32) ([]uint64, error)
	GetStoredFieldValue(docID uint64, field string) ([]byte, error)
	GetStoredDocument(docID uint64) (Document, error)
	PostingQuerySX(sExpression []byte) ([]uint64, error)
}

type NamedField struct {
	ftype  FieldType
	name   string
	buf    []byte
	stored bool
}

func (f NamedField) Type() FieldType  { return f.ftype }
func (f NamedField) Name() string     { return f.name }
func (f NamedField) Contents() []byte { return f.buf }
func (f NamedField) Stored() bool     { return f.stored }
func (f NamedField) String() string {
	var snippet string
	if len(f.buf) < 10 {
		snippet = string(f.buf)
	} else {
		snippet = string(f.buf[:10])
	}
	return fmt.Sprintf("field<type: %v, name: %q, buf: %q>", f.ftype, f.name, snippet)
}
func NewNamedField(ftype FieldType, name string, buf []byte, stored bool) NamedField {
	return NamedField{
		ftype:  ftype,
		name:   name,
		buf:    buf,
		stored: stored,
	}
}

type MapDocument struct {
	id     uint64
	fields map[string]NamedField
}

func (d MapDocument) ID() uint64              { return d.id }
func (d MapDocument) Field(name string) Field { return d.fields[name] }
func (d MapDocument) Fields() []string {
	fieldNames := make([]string, 0, len(d.fields))
	for name := range d.fields {
		fieldNames = append(fieldNames, name)
	}
	return fieldNames
}
func NewMapDocument(id uint64, fieldMap map[string]NamedField) MapDocument {
	return MapDocument{id, fieldMap}
}
