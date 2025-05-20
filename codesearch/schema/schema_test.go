package schema_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/token"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
	"github.com/stretchr/testify/assert"
)

func TestMakeField(t *testing.T) {
	fschema, err := schema.NewFieldSchema(types.KeywordField, "test", true)
	assert.Nil(t, err, "Expected no error creating field schema")

	assert.Equal(t, fschema.Type(), types.KeywordField)
	assert.Equal(t, fschema.Name(), "test")
	assert.Equal(t, fschema.Stored(), true)
	// Tokenizer type mappings covered in other tests below.
	assert.NotNil(t, fschema.MakeTokenizer())
}

func testInvalidName(t *testing.T, name string) {
	fschema, err := schema.NewFieldSchema(types.KeywordField, name, true)
	assert.NotNilf(t, err, "Expected error for invalid field name %q", name)
	assert.Nil(t, fschema, "Expected nil field schema for invalid name %q", name)
}

func testValidName(t *testing.T, name string) {
	fschema, err := schema.NewFieldSchema(types.KeywordField, name, true)
	assert.Nil(t, err, "Expected no error creating field schema for valid name %q", name)
	assert.NotNil(t, fschema, "Expected non-nil field schema for valid name %q", name)
}

func TestFieldNamePatterns(t *testing.T) {
	testInvalidName(t, "invalid name")
	testInvalidName(t, "_bad_field_name")
	testInvalidName(t, ".bad_field_name")
	testInvalidName(t, "bad field name")

	testValidName(t, "good_field_name")
	testValidName(t, "good_field_name1234")
	testValidName(t, "1good_field_name1234")
}

func TestNgramTokenizer(t *testing.T) {
	fschema, err := schema.NewFieldSchema(types.SparseNgramField, "test", true)
	assert.Nil(t, err, "Expected no error creating field schema")

	tok := fschema.MakeTokenizer()
	assert.NotNil(t, tok, "Expected non-nil tokenizer")
	assert.IsType(t, &token.SparseNgramTokenizer{}, tok)
}

func TestTrigramTokenizer(t *testing.T) {
	fschema, err := schema.NewFieldSchema(types.TrigramField, "test", true)
	assert.Nil(t, err, "Expected no error creating field schema")

	tok := fschema.MakeTokenizer()
	assert.NotNil(t, tok, "Expected non-nil tokenizer")
	assert.IsType(t, &token.TrigramTokenizer{}, tok)
}

func TestKeywordTokenizer(t *testing.T) {
	fschema, err := schema.NewFieldSchema(types.KeywordField, "test", true)
	assert.Nil(t, err, "Expected no error creating field schema")

	tok := fschema.MakeTokenizer()
	assert.NotNil(t, tok, "Expected non-nil tokenizer")
	assert.IsType(t, &token.WhitespaceTokenizer{}, tok)
}

func TestInvalidTokenizer(t *testing.T) {
	fschema, err := schema.NewFieldSchema(100, "test", true)
	assert.NotNil(t, err)
	assert.Nil(t, fschema)
}

func TestFieldSchemaString(t *testing.T) {
	fschema, err := schema.NewFieldSchema(types.KeywordField, "test", true)
	assert.Nil(t, err)
	assert.Equal(t, `FieldSchema<type: KeywordToken, name: "test", stored: true>`, fschema.String())
}
