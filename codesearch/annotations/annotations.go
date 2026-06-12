// Package annotations derives indexable facts about source files using
// tree-sitter. For each file it produces:
//
//   - the identity terms it imports (the `imports` field) and the terms by
//     which other files import it (the `import_id` field). The index's
//     posting lists over these keyword fields act as the repo's
//     reverse-import graph: the cardinality of the posting list for an
//     identity term is the number of files importing it.
//   - the names it declares (the `symbols` field), for ranking and, in
//     future, code navigation.
//
// Identity terms have the form `<family>:<scope-qualified identity>` (e.g.
// `go:github.com/buildbuddy-io/buildbuddy/codesearch/index`). Terms are
// lowercased to match the whitespace keyword tokenizer's normalization, so
// stored `import_id` values can be looked up directly against indexed
// `imports` terms.
package annotations

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/indexprofile"

	sitter "github.com/smacker/go-tree-sitter"
	gomodfile "golang.org/x/mod/modfile"
)

// RepoContext carries the repo-level inputs needed to resolve import strings
// to identity terms. It is constructed once per indexed repo.
type RepoContext struct {
	rootDir      string
	goModulePath string
}

// NewRepoContext returns a RepoContext for the repo rooted at rootDir,
// reading repo-level metadata (currently just go.mod at the repo root) from
// disk. Missing or unparsable metadata disables import-identity extraction
// for the affected language rather than erroring; symbols are unaffected.
// Callers without a checkout on disk (e.g. incremental updates from a git
// diff) must use NewRepoContextWithGoModule instead.
func NewRepoContext(rootDir string) *RepoContext {
	rc := &RepoContext{rootDir: rootDir}
	if data, err := os.ReadFile(filepath.Join(rootDir, "go.mod")); err == nil {
		rc.goModulePath = gomodfile.ModulePath(data)
	}
	return rc
}

// NewRepoContextWithGoModule returns a RepoContext with an explicit Go module
// path (e.g. read from the repo metadata doc), for callers that don't have a
// checkout on disk.
func NewRepoContextWithGoModule(rootDir, goModulePath string) *RepoContext {
	return &RepoContext{rootDir: rootDir, goModulePath: goModulePath}
}

func (rc *RepoContext) GoModulePath() string {
	return rc.goModulePath
}

// Result holds the annotations extracted from a single file.
type Result struct {
	// Imports are the identity terms of in-repo packages/files this file
	// imports, deduped, with self-edges and external imports dropped. E.g. a
	// Go file importing two in-repo packages:
	//   ["go:github.com/example/repo/util/log", "go:github.com/example/repo/db"]
	Imports []string

	// ImportID are the identity terms by which other files import this file —
	// today always zero terms (e.g. test files, or files with no module
	// context) or one, e.g.:
	//   ["go:github.com/example/repo/util/log"]   // a Go package
	//   ["java:com.example.build"]                // a Java package
	// It is a slice for symmetry with Imports (both populate a
	// space-separated keyword field) and to admit languages where a file has
	// several import identities, e.g. a C header reachable via multiple
	// include paths.
	ImportID []string

	// Symbols are the names this file declares (functions, methods, types,
	// and package-level consts/vars), in source order, lowercased. Local
	// variables, parameters, usages, comments, and string literals
	// contribute nothing: defining a name is strong evidence the file is
	// about it; merely containing it is not. E.g. for a Go file declaring
	// `type Greeter struct{…}` and `func (g *Greeter) Greet()`:
	//   ["greeter", "greet"]
	// Repeated when a name is declared more than once (e.g. overloaded Java
	// methods), so posting-list frequency reflects declaration count.
	Symbols []string
}

// Extract parses the file with tree-sitter and returns its annotations. lang
// is the lowercase enry-detected language name. Symbols are extracted for any
// supported language; import identities additionally require module context
// (go.mod) and an in-repo path.
//
// A nil result means there is nothing to extract — an unsupported language or
// a missing repo context — and is not an error. A file of a supported
// language always parses (tree-sitter is error-tolerant, marking unparseable
// regions with ERROR nodes rather than failing), so it returns non-nil
// annotations that may simply be empty. A non-nil error is reserved for the
// genuinely unexpected: a parse interrupted by context cancellation. Callers
// should not fail indexing on that error, but they should surface it rather
// than silently dropping the file.
func Extract(lang, filename string, content []byte, rctx *RepoContext) (*Result, error) {
	defer indexprofile.Timer(indexprofile.PhaseExtractAnnotations)()
	if rctx == nil {
		return nil, nil
	}
	switch lang {
	case "go":
		return extractGo(filename, content, rctx)
	case "java":
		return extractJava(filename, content, rctx)
	default:
		return nil, nil
	}
}

// mustCompileQuery compiles a tree-sitter query at package init; the pattern
// is a compile-time constant, so failure is a programming error.
func mustCompileQuery(pattern string, lang *sitter.Language) *sitter.Query {
	q, err := sitter.NewQuery([]byte(pattern), lang)
	if err != nil {
		panic(fmt.Sprintf("compiling tree-sitter query: %s", err))
	}
	return q
}

// captureAllRaw returns the text of every capture of q in the parsed tree,
// in source order, case preserved.
func captureAllRaw(q *sitter.Query, tree *sitter.Tree, content []byte) []string {
	qc := sitter.NewQueryCursor()
	defer qc.Close()
	qc.Exec(q, tree.RootNode())

	var out []string
	for {
		m, ok := qc.NextMatch()
		if !ok {
			break
		}
		for _, c := range m.Captures {
			if text := c.Node.Content(content); text != "" {
				out = append(out, text)
			}
		}
	}
	return out
}

// captureAll is captureAllRaw lowercased, matching the keyword tokenizer's
// normalization.
func captureAll(q *sitter.Query, tree *sitter.Tree, content []byte) []string {
	out := captureAllRaw(q, tree, content)
	for i, s := range out {
		out[i] = strings.ToLower(s)
	}
	return out
}

// relPath returns filename slash-separated and relative to the repo root, or
// "" if it isn't under it.
func (rc *RepoContext) relPath(filename string) string {
	rel := filename
	if rc.rootDir != "" {
		r, err := filepath.Rel(rc.rootDir, filename)
		if err != nil {
			return ""
		}
		rel = r
	}
	rel = filepath.ToSlash(rel)
	if rel == ".." || strings.HasPrefix(rel, "../") {
		return ""
	}
	return rel
}
