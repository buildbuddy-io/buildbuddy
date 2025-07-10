package schema

import (
	"fmt"
	"reflect"
	"regexp"
	"strings"
	
	"github.com/modelcontextprotocol/go-sdk/jsonschema"
)

// REMOVE ONCE https://github.com/modelcontextprotocol/go-sdk/pull/101/files is in.

type jsonInfo struct {
	Omit     bool            // unexported or first tag element is "-"
	Name     string          // Go field name or first tag element. Empty if Omit is true.
	Settings map[string]bool // "omitempty", "omitzero", etc.
}

// FieldJSONInfo reports information about how encoding/json
// handles the given struct field.
// If the field is unexported, JSONInfo.Omit is true and no other JSONInfo field
// is populated.
// If the field is exported and has no tag, then Name is the field's name and all
// other fields are false.
// Otherwise, the information is obtained from the tag.
func fieldJSONInfo(f reflect.StructField) jsonInfo {
	if !f.IsExported() {
		return jsonInfo{Omit: true}
	}
	info := jsonInfo{Name: f.Name}
	if tag, ok := f.Tag.Lookup("json"); ok {
		name, rest, found := strings.Cut(tag, ",")
		// "-" means omit, but "-," means the name is "-"
		if name == "-" && !found {
			return jsonInfo{Omit: true}
		}
		if name != "" {
			info.Name = name
		}
		if len(rest) > 0 {
			info.Settings = map[string]bool{}
			for _, s := range strings.Split(rest, ",") {
				info.Settings[s] = true
			}
		}
	}
	return info
}

func falseSchema() *jsonschema.Schema {
	return &jsonschema.Schema{Not: &jsonschema.Schema{}}
}

// For constructs a JSON schema object for the given type argument.
//
// It translates Go types into compatible JSON schema types, as follows:
//   - Strings have schema type "string".
//   - Bools have schema type "boolean".
//   - Signed and unsigned integer types have schema type "integer".
//   - Floating point types have schema type "number".
//   - Slices and arrays have schema type "array", and a corresponding schema
//     for items.
//   - Maps with string key have schema type "object", and corresponding
//     schema for additionalProperties.
//   - Structs have schema type "object", and disallow additionalProperties.
//     Their properties are derived from exported struct fields, using the
//     struct field JSON name. Fields that are marked "omitempty" are
//     considered optional; all other fields become required properties.
//
// For returns an error if t contains (possibly recursively) any of the following Go
// types, as they are incompatible with the JSON schema spec.
//   - maps with key other than 'string'
//   - function types
//   - complex numbers
//   - unsafe pointers
//
// The types must not have cycles.
//
// For recognizes struct field tags named "jsonschema".
// A jsonschema tag on a field is used as the description for the corresponding property.
// For future compatibility, descriptions must not start with "WORD=", where WORD is a
// sequence of non-whitespace characters.
func For[T any]() (*jsonschema.Schema, error) {
	// TODO: consider skipping incompatible fields, instead of failing.
	s, err := forType(reflect.TypeFor[T]())
	if err != nil {
		var z T
		return nil, fmt.Errorf("For[%T](): %w", z, err)
	}
	return s, nil
}
func forType(t reflect.Type) (*jsonschema.Schema, error) {
	// Follow pointers: the schema for *T is almost the same as for T, except that
	// an explicit JSON "null" is allowed for the pointer.
	allowNull := false
	for t.Kind() == reflect.Pointer {
		allowNull = true
		t = t.Elem()
	}
	var (
		s   = new(jsonschema.Schema)
		err error
	)
	switch t.Kind() {
	case reflect.Bool:
		s.Type = "boolean"
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Uintptr:
		s.Type = "integer"
	case reflect.Float32, reflect.Float64:
		s.Type = "number"
	case reflect.Interface:
		// Unrestricted
	case reflect.Map:
		if t.Key().Kind() != reflect.String {
			return nil, fmt.Errorf("unsupported map key type %v", t.Key().Kind())
		}
		s.Type = "object"
		s.AdditionalProperties, err = forType(t.Elem())
		if err != nil {
			return nil, fmt.Errorf("computing map value schema: %v", err)
		}
	case reflect.Slice, reflect.Array:
		s.Type = "array"
		s.Items, err = forType(t.Elem())
		if err != nil {
			return nil, fmt.Errorf("computing element schema: %v", err)
		}
		if t.Kind() == reflect.Array {
			s.MinItems = jsonschema.Ptr(t.Len())
			s.MaxItems = jsonschema.Ptr(t.Len())
		}
	case reflect.String:
		s.Type = "string"
	case reflect.Struct:
		s.Type = "object"
		// no additional properties are allowed
		s.AdditionalProperties = falseSchema()
		for i := range t.NumField() {
			field := t.Field(i)
			info := fieldJSONInfo(field)
			if info.Omit {
				continue
			}
			if s.Properties == nil {
				s.Properties = make(map[string]*jsonschema.Schema)
			}
			fs, err := forType(field.Type)
			if err != nil {
				return nil, err
			}
			if tag, ok := field.Tag.Lookup("jsonschema"); ok {
				if tag == "" {
					return nil, fmt.Errorf("empty jsonschema tag on struct field %s.%s", t, field.Name)
				}
				if disallowedPrefixRegexp.MatchString(tag) {
					return nil, fmt.Errorf("tag must not begin with 'WORD=': %q", tag)
				}
				fs.Description = tag
			}
			s.Properties[info.Name] = fs
			if err != nil {
				return nil, err
			}
			if !info.Settings["omitempty"] && !info.Settings["omitzero"] {
				s.Required = append(s.Required, info.Name)
			}
		}
	default:
		return nil, fmt.Errorf("type %v is unsupported by jsonschema", t)
	}
	if allowNull && s.Type != "" {
		s.Types = []string{"null", s.Type}
		s.Type = ""
	}
	return s, nil
}

// Disallow jsonschema tag values beginning "WORD=", for future expansion.
var disallowedPrefixRegexp = regexp.MustCompile("^[^ \t\n]*=")
