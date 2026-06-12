package annotations

import (
	"context"
	"strings"

	sitter "github.com/smacker/go-tree-sitter"
	"github.com/smacker/go-tree-sitter/java"
)

// Java support.
//
// Identity terms are package-scoped: a file's ImportID is `java:<package>`
// from its package declaration, and each import contributes the identity of
// the package it names. Class imports are mapped to their package by Java's
// naming convention (package segments are lowercase, type names are
// capitalized), so `import com.foo.Bar` and `import com.foo.*` both yield
// `java:com.foo`. External-package imports are indexed too; they simply never
// match any document's import_id, so they contribute nothing to in-degree.

// javaSymbolQuery captures declaration names: types (class/interface/enum/
// record/annotation), methods, constructors, fields, and enum constants.
// Local variables are a distinct node type (local_variable_declaration) and
// are excluded by construction.
var javaSymbolQuery = mustCompileQuery(`
	(class_declaration name: (identifier) @sym)
	(interface_declaration name: (identifier) @sym)
	(enum_declaration name: (identifier) @sym)
	(record_declaration name: (identifier) @sym)
	(annotation_type_declaration name: (identifier) @sym)
	(method_declaration name: (identifier) @sym)
	(constructor_declaration name: (identifier) @sym)
	(field_declaration declarator: (variable_declarator name: (identifier) @sym))
	(enum_constant name: (identifier) @sym)
`, java.GetLanguage())

// The (scoped_)identifier must be an immediate child, so nested
// scoped_identifiers inside a dotted name don't also match.
var javaPackageQuery = mustCompileQuery(`
	(package_declaration [(identifier) (scoped_identifier)] @pkg)
`, java.GetLanguage())

var javaImportQuery = mustCompileQuery(`
	(import_declaration [(identifier) (scoped_identifier)] @imp)
`, java.GetLanguage())

// javaTerm formats a Java package as an identity term.
func javaTerm(pkg string) string {
	return "java:" + strings.ToLower(pkg)
}

// javaPackageOf maps a dotted name from an import declaration to the package
// it names, using Java naming convention: the package is the longest leading
// run of lowercase-initial segments. `com.foo.Bar` -> `com.foo`,
// `com.foo.Bar.CONST` (static import) -> `com.foo`, `com.foo` (wildcard
// import's scoped_identifier) -> `com.foo`.
func javaPackageOf(dotted string) string {
	segs := strings.Split(dotted, ".")
	n := 0
	for _, s := range segs {
		if s == "" || !(s[0] >= 'a' && s[0] <= 'z' || s[0] == '_') {
			break
		}
		n++
	}
	return strings.Join(segs[:n], ".")
}

// isJavaTestFile reports whether the repo-relative path is a test file and so
// should be excluded from import-rank identity (mirroring Go's _test.go
// exclusion). The check is segment-based on the relative path: a conventional
// test directory anywhere in the path, or a Test/Tests-suffixed name. This is
// a heuristic — it misfires on a package legitimately named "test"
// (com/example/test/Foo.java) — which is acceptable for a fuzzy ranking
// signal. It must be passed the repo-relative path, not the absolute one, or
// a checkout under e.g. /work/tests/... would misclassify every file.
func isJavaTestFile(relPath string) bool {
	for seg := range strings.SplitSeq(relPath, "/") {
		if seg == "test" || seg == "tests" || seg == "javatests" {
			return true
		}
	}
	return strings.HasSuffix(relPath, "Test.java") || strings.HasSuffix(relPath, "Tests.java")
}

func extractJava(ctx context.Context, filename string, content []byte, rctx *RepoContext) (*Result, error) {
	parser := sitter.NewParser()
	defer parser.Close() // the tree owns its data and outlives the parser
	parser.SetLanguage(java.GetLanguage())
	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()

	ann := &Result{Symbols: captureAll(javaSymbolQuery, tree, content)}

	// Identity (imports/import_id) is only meaningful for files inside the
	// repo; symbols are extracted regardless.
	rel := rctx.relPath(filename)
	if rel == "" {
		return ann, nil
	}

	// Package and import names must keep their case: javaPackageOf depends
	// on the lowercase-package / Capitalized-class convention to find the
	// package boundary. javaTerm lowercases at the end.
	selfPkg := ""
	if pkgs := captureAllRaw(javaPackageQuery, tree, content); len(pkgs) > 0 {
		selfPkg = pkgs[0]
	}

	seen := make(map[string]struct{})
	var imports []string
	for _, imp := range captureAllRaw(javaImportQuery, tree, content) {
		pkg := javaPackageOf(imp)
		if pkg == "" || pkg == selfPkg {
			continue
		}
		term := javaTerm(pkg)
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		imports = append(imports, term)
	}
	ann.Imports = imports

	if selfPkg != "" && !isJavaTestFile(rel) {
		ann.ImportID = []string{javaTerm(selfPkg)}
	}
	return ann, nil
}
