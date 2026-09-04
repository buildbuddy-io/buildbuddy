package annotations

import (
	"context"
	"strings"

	sitter "github.com/smacker/go-tree-sitter"
	"github.com/smacker/go-tree-sitter/python"
)

// Python support.
//
// Identity terms are module-scoped: a file's ImportID is `py:<dotted module>`
// derived from its repo-relative path (foo/bar/baz.py -> py:foo.bar.baz;
// foo/bar/__init__.py -> py:foo.bar). Imports record the dotted module each
// import statement names. `from a.b import c` records both the package `a.b`
// and the submodule candidate `a.b.c`, since `c` may be a module or a name —
// over-recording is harmless: as with Java, external imports (stdlib, third
// party) are indexed too and simply never match any document's import_id, so
// they add nothing to in-degree.

// pythonSymbolQuery captures declaration names: functions and methods (both
// function_definition), classes, and module-level assignments to a bare name.
var pythonSymbolQuery = mustCompileQuery(`
	(function_definition name: (identifier) @sym)
	(class_definition name: (identifier) @sym)
	(module (expression_statement (assignment left: (identifier) @sym)))
`, python.GetLanguage())

// pyImportQuery captures the module named by a plain `import a.b.c` or
// `import a.b.c as x`.
var pyImportQuery = mustCompileQuery(`
	(import_statement name: (dotted_name) @mod)
	(import_statement name: (aliased_import name: (dotted_name) @mod))
`, python.GetLanguage())

// pyFromPairQuery yields one (module, name) match per imported name of a
// `from a.b import c, d` statement; @from is shared across the matches. An
// aliased name (`import c as d`) is an aliased_import node; capture its inner
// dotted_name so @name holds c, not the whole `c as d` — otherwise the
// py:a.b.c submodule edge would be lost (plain `import a.b.c as d` already
// handles this in pyImportQuery). A single imported name parses as a
// one-segment dotted_name, so no bare (identifier) alternative is needed.
var pyFromPairQuery = mustCompileQuery(`
	(import_from_statement
		module_name: [(dotted_name) (relative_import)] @from
		name: [
			(dotted_name) @name
			(aliased_import name: (dotted_name) @name)
		])
`, python.GetLanguage())

// pyFromModuleQuery captures just the from-module, covering wildcard imports
// (`from a.b import *`) and recording the parent package itself.
var pyFromModuleQuery = mustCompileQuery(`
	(import_from_statement module_name: [(dotted_name) (relative_import)] @from)
`, python.GetLanguage())

func pyTerm(module string) string {
	return "py:" + strings.ToLower(module)
}

// pythonModulePath maps a repo-relative path to its dotted module: strip the
// .py suffix, collapse a package's __init__ to the package itself, and join on
// dots. Returns "" for a repo-root __init__.py (no module).
func pythonModulePath(rel string) string {
	rel = strings.TrimSuffix(rel, ".py")
	rel = strings.TrimSuffix(rel, "/__init__")
	if rel == "__init__" {
		return ""
	}
	return strings.ReplaceAll(rel, "/", ".")
}

// pyParentPackage returns the package a module lives in: the module minus its
// last segment ("" for a top-level module).
func pyParentPackage(module string) string {
	if i := strings.LastIndex(module, "."); i >= 0 {
		return module[:i]
	}
	return ""
}

// pyResolveFrom resolves a `from` clause to an absolute dotted module. An
// absolute clause is returned as-is; a relative one (leading dots) is resolved
// against the importing file's package: one dot is the current package, each
// additional dot goes up one more, then the trailing dotted name is appended.
// Returns "" if the relative reference climbs above the repo root.
func pyResolveFrom(clause, selfModule string, isInit bool) string {
	if !strings.HasPrefix(clause, ".") {
		return clause
	}
	dots := 0
	for dots < len(clause) && clause[dots] == '.' {
		dots++
	}
	suffix := clause[dots:]

	// The current package is the file's own package: for a package __init__
	// that is the module itself; for a regular module it is its parent.
	pkg := selfModule
	if !isInit {
		pkg = pyParentPackage(selfModule)
	}
	// One dot = current package; each extra dot climbs one more.
	for i := 1; i < dots; i++ {
		if pkg == "" {
			return ""
		}
		pkg = pyParentPackage(pkg)
	}
	if suffix == "" {
		return pkg
	}
	if pkg == "" {
		return suffix
	}
	return pkg + "." + suffix
}

// isPythonTestFile reports whether the repo-relative path is a test file, so it
// is excluded from import-rank identity (mirroring Go's _test.go exclusion).
func isPythonTestFile(rel string) bool {
	for seg := range strings.SplitSeq(rel, "/") {
		if seg == "test" || seg == "tests" {
			return true
		}
	}
	base := rel
	if i := strings.LastIndex(rel, "/"); i >= 0 {
		base = rel[i+1:]
	}
	return strings.HasPrefix(base, "test_") || strings.HasSuffix(base, "_test.py")
}

func extractPython(ctx context.Context, filename string, content []byte, rctx *RepoContext) (*Result, error) {
	parser := sitter.NewParser()
	// Close the parser only after the tree: the binding keeps the parser
	// alive for the tree's lifetime, so freeing it first is a use-after-free.
	defer parser.Close()
	parser.SetLanguage(python.GetLanguage())
	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()

	ann := &Result{Symbols: captureAll(pythonSymbolQuery, tree, content)}

	rel := rctx.relPath(filename)
	if rel == "" {
		return ann, nil
	}
	selfModule := pythonModulePath(rel)
	isInit := strings.HasSuffix(rel, "/__init__.py") || rel == "__init__.py"
	selfTerm := pyTerm(selfModule)

	seen := make(map[string]struct{})
	add := func(module string) {
		if module == "" {
			return
		}
		term := pyTerm(module)
		if term == selfTerm {
			return // drop self-edges
		}
		if _, ok := seen[term]; ok {
			return
		}
		seen[term] = struct{}{}
		ann.Imports = append(ann.Imports, term)
	}

	for _, mod := range captureAllRaw(pyImportQuery, tree, content) {
		add(mod)
	}
	// Wildcard / bare from-module.
	for _, from := range captureAllRaw(pyFromModuleQuery, tree, content) {
		add(pyResolveFrom(from, selfModule, isInit))
	}
	// (from-module, imported-name) pairs: record the parent module and the
	// submodule candidate the name might be.
	qc := sitter.NewQueryCursor()
	defer qc.Close()
	qc.Exec(pyFromPairQuery, tree.RootNode())
	for {
		m, ok := qc.NextMatch()
		if !ok {
			break
		}
		var from, name string
		for _, c := range m.Captures {
			switch pyFromPairQuery.CaptureNameForId(c.Index) {
			case "from":
				from = c.Node.Content(content)
			case "name":
				name = c.Node.Content(content)
			}
		}
		mod := pyResolveFrom(from, selfModule, isInit)
		add(mod)
		if mod != "" && name != "" {
			add(mod + "." + name)
		}
	}

	if selfModule != "" && !isPythonTestFile(rel) {
		ann.ImportID = []string{selfTerm}
	}
	return ann, nil
}
