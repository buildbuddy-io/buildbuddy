package annotations

import (
	"context"
	"path"
	"strings"

	sitter "github.com/smacker/go-tree-sitter"
	"github.com/smacker/go-tree-sitter/c"
	"github.com/smacker/go-tree-sitter/cpp"
)

// C and C++ support.
//
// The identity model is #include-path based and intentionally fuzzy: a file's
// identity (ImportID) is its repo-relative path AND its basename, so an
// #include by either form matches. Each #include contributes the identity term
// for the spec it names AND for the spec's basename. This is ambiguous (two
// headers with the same basename collide) but that is acceptable for an
// in-degree ranking signal. All identity terms live in the `c:` family for
// both C and C++.

// cSymbolQuery captures C declaration names. Function names are nested inside
// a function_declarator's declarator; type names hang off the various
// specifiers; typedefs name a type_identifier declarator.
var cSymbolQuery = mustCompileQuery(`
	(function_definition declarator: (function_declarator declarator: (identifier) @sym))
	(struct_specifier name: (type_identifier) @sym)
	(union_specifier name: (type_identifier) @sym)
	(enum_specifier name: (type_identifier) @sym)
	(type_definition declarator: (type_identifier) @sym)
`, c.GetLanguage())

// cppSymbolQuery is the C query plus C++-only node types: class and namespace
// declarations, methods (field_identifier) and qualified out-of-line
// definitions (Class::method).
var cppSymbolQuery = mustCompileQuery(`
	(function_definition declarator: (function_declarator declarator: (identifier) @sym))
	(function_definition declarator: (function_declarator declarator: (field_identifier) @sym))
	(function_definition declarator: (function_declarator declarator: (qualified_identifier name: (identifier) @sym)))
	(function_definition declarator: (function_declarator declarator: (qualified_identifier name: (qualified_identifier name: (identifier) @sym))))
	(struct_specifier name: (type_identifier) @sym)
	(union_specifier name: (type_identifier) @sym)
	(enum_specifier name: (type_identifier) @sym)
	(class_specifier name: (type_identifier) @sym)
	(namespace_definition name: (namespace_identifier) @sym)
	(type_definition declarator: (type_identifier) @sym)
`, cpp.GetLanguage())

// cIncludeQuery captures the path argument of #include directives, both
// "quoted" (string_literal) and <bracketed> (system_lib_string) forms.
var cIncludeQuery = mustCompileQuery(`
	(preproc_include path: (string_literal) @inc)
	(preproc_include path: (system_lib_string) @inc)
`, c.GetLanguage())

var cppIncludeQuery = mustCompileQuery(`
	(preproc_include path: (string_literal) @inc)
	(preproc_include path: (system_lib_string) @inc)
`, cpp.GetLanguage())

// cTerm formats an include path (or basename) as an identity term.
func cTerm(p string) string {
	return "c:" + strings.ToLower(p)
}

// stripIncludeQuotes removes the surrounding "" or <> from an #include spec's
// captured node text.
func stripIncludeQuotes(s string) string {
	if len(s) >= 2 {
		if (s[0] == '"' && s[len(s)-1] == '"') || (s[0] == '<' && s[len(s)-1] == '>') {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// isCTestFile reports whether the repo-relative path is a test file and so
// should be excluded from import-rank identity. Heuristic: a test/tests path
// segment, or a `foo_test.cc` / `test_foo.cc` filename (anchored on the stem so
// it doesn't misfire on ordinary names like latest_handler.cc or testbed.cc).
// Acceptable fuzziness for a ranking signal; must be the repo-relative path,
// not the absolute one.
func isCTestFile(relPath string) bool {
	for seg := range strings.SplitSeq(relPath, "/") {
		if seg == "test" || seg == "tests" {
			return true
		}
	}
	stem := strings.TrimSuffix(path.Base(relPath), path.Ext(relPath))
	return strings.HasSuffix(stem, "_test") || strings.HasPrefix(stem, "test_")
}

func extractCpp(ctx context.Context, lang, filename string, content []byte, rctx *RepoContext) (*Result, error) {
	symQuery, incQuery, grammar := cSymbolQuery, cIncludeQuery, c.GetLanguage()
	if lang == "c++" {
		symQuery, incQuery, grammar = cppSymbolQuery, cppIncludeQuery, cpp.GetLanguage()
	}

	parser := sitter.NewParser()
	// Close the parser only after the tree: the binding keeps the parser
	// alive for the tree's lifetime, so freeing it first is a use-after-free.
	// Defers run LIFO, so registering parser.Close before tree.Close gives
	// the right order.
	defer parser.Close()
	parser.SetLanguage(grammar)
	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()

	ann := &Result{Symbols: captureAll(symQuery, tree, content)}

	// Identity (imports/import_id) is only meaningful for files inside the
	// repo; symbols are extracted regardless.
	rel := rctx.relPath(filename)
	if rel == "" {
		return ann, nil
	}

	// A file's own identity terms (used both as ImportID and to drop
	// self-edges from imports) are its repo-relative path and its basename.
	selfPath := cTerm(rel)
	selfBase := cTerm(path.Base(rel))
	self := map[string]struct{}{selfPath: {}, selfBase: {}}

	seen := make(map[string]struct{})
	var imports []string
	for _, raw := range captureAllRaw(incQuery, tree, content) {
		spec := stripIncludeQuotes(raw)
		if spec == "" {
			continue
		}
		for _, term := range []string{cTerm(spec), cTerm(path.Base(spec))} {
			if _, ok := self[term]; ok {
				continue
			}
			if _, ok := seen[term]; ok {
				continue
			}
			seen[term] = struct{}{}
			imports = append(imports, term)
		}
	}
	ann.Imports = imports

	if !isCTestFile(rel) {
		ann.ImportID = []string{selfPath, selfBase}
	}
	return ann, nil
}
