package schema

import (
	"fmt"
	"regexp"

	"github.com/buildbuddy-io/buildbuddy/codesearch/token"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	fieldNameRegex = regexp.MustCompile(`^([a-zA-Z0-9][a-zA-Z0-9_]*)$`)

	// The following field names are used in the indexed docs.
	IDField        = "id"
	FilenameField  = "filename"
	ContentField   = "content"
	LanguageField  = "language"
	OwnerField     = "owner"
	RepoField      = "repo"
	SHAField       = "sha"
	LatestSHAField = "latest_sha"

	// TODO(jdelfino): Define CodeSchema and MetadataSchema types that implement the
	// types.DocumentSchema interface. Then we can hide all the flexibility from the clients,
	// without hardcoding a particular schema into the search core.
	gitHubFileSchema = NewDocumentSchema([]types.FieldSchema{
		MustFieldSchema(types.KeywordField, IDField, false),
		MustFieldSchema(types.TrigramField, FilenameField, true),
		MustFieldSchema(types.SparseNgramField, ContentField, true),
		MustFieldSchema(types.KeywordField, LanguageField, true),
		MustFieldSchema(types.KeywordField, OwnerField, true),
		MustFieldSchema(types.KeywordField, RepoField, true),
		MustFieldSchema(types.KeywordField, SHAField, true),
	})
	metadataSchema = NewDocumentSchema([]types.FieldSchema{
		// Repository URL
		MustFieldSchema(types.KeywordField, IDField, false),
		// Last indexed commit SHA
		MustFieldSchema(types.KeywordField, LatestSHAField, true),
	})
)

func GitHubFileSchema() types.DocumentSchema {
	return gitHubFileSchema
}

func MetadataSchema() types.DocumentSchema {
	return metadataSchema
}

// Satisfies the types.FieldSchema interface.
type fieldSchema struct {
	ftype   types.FieldType
	name    string
	stored  bool
	makeTok func() types.Tokenizer
}

func (f fieldSchema) Type() types.FieldType          { return f.ftype }
func (f fieldSchema) Name() string                   { return f.name }
func (f fieldSchema) Stored() bool                   { return f.stored }
func (f fieldSchema) MakeTokenizer() types.Tokenizer { return f.makeTok() }

func (f fieldSchema) MakeField(buf []byte) types.Field {
	return documentField{
		buf:    buf,
		schema: f,
	}
}

func NewFieldSchema(ftype types.FieldType, name string, stored bool) (types.FieldSchema, error) {
	var makeTok func() types.Tokenizer = nil

	if !fieldNameRegex.MatchString(name) {
		return nil, status.InvalidArgumentErrorf("Invalid field name %q", name)
	}

	switch ftype {
	case types.SparseNgramField:
		// TODO(jdelfino): old code used lowercase on query side but not on index side - was it a bug?
		makeTok = func() types.Tokenizer {
			return token.NewSparseNgramTokenizer(token.WithMaxNgramLength(6), token.WithLowerCase(true))
		}
	case types.TrigramField:
		makeTok = func() types.Tokenizer {
			return token.NewTrigramTokenizer()
		}
	case types.KeywordField:
		makeTok = func() types.Tokenizer {
			return token.NewWhitespaceTokenizer()
		}
	default:
		return nil, status.InvalidArgumentErrorf("Invalid field type %q", ftype)
	}

	return fieldSchema{
		ftype:   ftype,
		name:    name,
		stored:  stored,
		makeTok: makeTok,
	}, nil
}

// Schemas are typically static and const, so in the most common situations, it's reasonable to
// panic if creation fails.
func MustFieldSchema(ftype types.FieldType, name string, stored bool) types.FieldSchema {
	fieldSchema, err := NewFieldSchema(ftype, name, stored)
	if err != nil {
		panic(fmt.Sprintf("failed to create field schema for %s: %v", name, err))
	}
	return fieldSchema
}

func (f fieldSchema) String() string {
	return fmt.Sprintf("FieldSchema<type: %v, name: %q, stored: %v>", f.ftype, f.name, f.stored)
}

// Satisfies the types.DocumentSchema interface.
type documentSchema map[string]types.FieldSchema

func (d documentSchema) Fields() []types.FieldSchema {
	fieldSchemas := make([]types.FieldSchema, 0, len(d))
	for _, field := range d {
		fieldSchemas = append(fieldSchemas, field)
	}
	return fieldSchemas
}

func (d documentSchema) Field(name string) types.FieldSchema {
	if schema, ok := d[name]; ok {
		return schema
	}
	return nil
}

func (d documentSchema) String() string {
	schemas := make([]string, 0, len(d))
	for _, field := range d {
		schemas = append(schemas, field.String())
	}
	return fmt.Sprintf("DocumentSchema<fields: [%v]>", schemas)
}

func (d documentSchema) MakeDocument(data map[string][]byte) (types.Document, error) {
	fields := make(map[string]types.Field, len(data))
	for name, field_data := range data {
		fieldSchema, ok := d[name]
		if !ok {
			return nil, status.InvalidArgumentErrorf("Invalid field name %q", name)
		}
		fields[name] = fieldSchema.MakeField(field_data)
	}
	return document(fields), nil
}

func (d documentSchema) MustMakeDocument(data map[string][]byte) types.Document {
	doc, err := d.MakeDocument(data)
	if err != nil {
		panic(fmt.Sprintf("failed to create document: %v", err))
	}
	return doc
}

func NewDocumentSchema(schemas []types.FieldSchema) types.DocumentSchema {
	fieldMap := make(map[string]types.FieldSchema, len(schemas))
	for _, schema := range schemas {
		fieldMap[schema.Name()] = schema
	}
	return documentSchema(fieldMap)
}

// Satisfies the types.DocumentField interface.
type documentField struct {
	buf    []byte
	schema types.FieldSchema
}

func (f documentField) Schema() types.FieldSchema { return f.schema }
func (f documentField) Name() string              { return f.schema.Name() }
func (f documentField) Type() types.FieldType     { return f.schema.Type() }
func (f documentField) Contents() []byte          { return f.buf }
func (f documentField) String() string {
	var snippet string
	if len(f.buf) < 10 {
		snippet = string(f.buf)
	} else {
		snippet = string(f.buf[:10])
	}
	return fmt.Sprintf("field<type: %v, name: %q, buf: %q>", f.schema.Type(), f.schema.Name(), snippet)
}

func NewDocField(schema types.FieldSchema, buf []byte) documentField {
	return documentField{
		buf:    buf,
		schema: schema,
	}
}

// Satisfies the types.Document interface.
type document map[string]types.Field

func (d document) Field(name string) types.Field { return d[name] }
func (d document) Fields() []string {
	fieldNames := make([]string, 0, len(d))
	for name := range d {
		fieldNames = append(fieldNames, name)
	}
	return fieldNames
}
