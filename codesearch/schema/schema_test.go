package schema_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/codesearch/token"
	"github.com/buildbuddy-io/buildbuddy/codesearch/types"
)

func TestMakeField(t *testing.T) {
	fschema, err := schema.NewFieldSchema(types.KeywordField, "test", true)
	if err != nil {
		t.Fatalf("Failed to create field schema: %v", err)
	}
	if fschema.Type() != types.KeywordField {
		t.Errorf("Expected field type %v, got %v", types.KeywordField, fschema.Type())
	}
	if fschema.Name() != "test" {
		t.Errorf("Expected field name %q, got %q", "test", fschema.Name())
	}
	if fschema.Stored() != true {
		t.Errorf("Expected field stored %v, got %v", true, fschema.Stored())
	}
	if fschema.Tokenizer() == nil {
		// Tokenizer type mappings covered in other tests below.
		t.Errorf("Expected field tokenizer %v, got %v", nil, fschema.Tokenizer())
	}
}

func testInvalidName(t *testing.T, name string) {
	fschema, err := schema.NewFieldSchema(types.KeywordField, name, true)
	if err == nil {
		t.Fatalf("Expected error for invalid field name %q, got %v", name, fschema)
	}
	if fschema != nil {
		t.Errorf("Expected nil field schema for invalid name %q, got %v", name, fschema)
	}
}

func testValidName(t *testing.T, name string) {
	fschema, err := schema.NewFieldSchema(types.KeywordField, name, true)
	if err != nil {
		t.Fatalf("Expected no error for valid field name %q, got %v", name, err)
	}
	if fschema == nil {
		t.Errorf("Expected non-nil field schema for valid name %q, got %v", name, fschema)
	}
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
	if err != nil {
		t.Fatalf("Failed to create field schema: %v", err)
	}
	tok := fschema.Tokenizer()
	if tok == nil {
		t.Errorf("Expected non-nil tokenizer, got nil")
	}
	// Check that the tokenizer is of the expected type.
	if _, ok := tok.(*token.SparseNgramTokenizer); !ok {
		t.Errorf("Expected SparseNgramTokenizer, got %T", tok)
	}
}

func TestTrigramTokenizer(t *testing.T) {
	fschema, err := schema.NewFieldSchema(types.TrigramField, "test", true)
	if err != nil {
		t.Fatalf("Failed to create field schema: %v", err)
	}
	tok := fschema.Tokenizer()
	if tok == nil {
		t.Errorf("Expected non-nil tokenizer, got nil")
	}
	// Check that the tokenizer is of the expected type.
	if _, ok := tok.(*token.TrigramTokenizer); !ok {
		t.Errorf("Expected TrigramTokenizer, got %T", tok)
	}
}

func TestKeywordTokenizer(t *testing.T) {
	fschema, err := schema.NewFieldSchema(types.KeywordField, "test", true)
	if err != nil {
		t.Fatalf("Failed to create field schema: %v", err)
	}
	tok := fschema.Tokenizer()
	if tok == nil {
		t.Errorf("Expected non-nil tokenizer, got nil")
	}
	// Check that the tokenizer is of the expected type.
	if _, ok := tok.(*token.WhitespaceTokenizer); !ok {
		t.Errorf("Expected WhitespaceTokenizer, got %T", tok)
	}
}

func TestInvalidTokenizer(t *testing.T) {
	fschema, err := schema.NewFieldSchema(100, "test", true)
	if err == nil {
		t.Fatalf("Expected error for invalid field type, got %v", fschema)
	}
	if fschema != nil {
		t.Errorf("Expected nil field schema for invalid type, got %v", fschema)
	}
}

func TestFieldSchemaString(t *testing.T) {
	fschema, err := schema.NewFieldSchema(types.KeywordField, "test", true)
	if err != nil {
		t.Fatalf("Failed to create field schema: %v", err)
	}
	expected := `FieldSchema<type: KeywordToken, name: "test", stored: true>`
	if fschema.String() != expected {
		t.Errorf("Expected %q, got %q", expected, fschema.String())
	}
}
