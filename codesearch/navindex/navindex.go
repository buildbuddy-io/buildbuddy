// Package navindex provides the index-backed lookups that tree-sitter code
// navigation (the annotations package) needs: loading a file's decorate inputs by path, and
// resolving a symbol's definition via the index's symbols/import_id posting
// lists. It is the bridge between annotations (pure tree-sitter, no index deps)
// and the codesearch index, shared by the server's KytheProxy handlers and the
// CLI's nav subcommands so there is one implementation.
package navindex

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/query"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
)

// File holds the per-file inputs annotations.Decorate needs, read from a single
// indexed document.
type File struct {
	// Content is the file's stored source.
	Content []byte
	// Lang is the file's stored language, used to dispatch the right decorator.
	Lang string
	// SelfImportID is the file's own import_id term (empty for files with no
	// package identity, e.g. test files), used to mint same-package tickets.
	SelfImportID string
	// InRepo accepts an import path iff it is one of this file's stored in-repo
	// imports, so only selectors to indexed packages are decorated.
	InRepo func(importPath string) bool
}

// FindFile loads the decorate inputs for an exact repo-relative path. ok is
// false if no document with that filename exists in the reader's namespace.
// The filename field is trigram-indexed, so a literal `file:` query yields a
// superset of candidates that we filter down to the exact match.
func FindFile(ctx context.Context, r *index.Reader, path string) (File, bool) {
	rq, err := query.NewReQuery(ctx, "file:^"+regexp.QuoteMeta(path)+"$")
	if err != nil {
		return File{}, false
	}
	sq := rq.SQuery()
	if sq == "" {
		return File{}, false
	}
	matches, err := r.RawQuery(sq)
	if err != nil {
		return File{}, false
	}
	for _, m := range matches {
		doc := r.GetStoredDocument(m.Docid())
		if string(doc.Field(schema.FilenameField).Contents()) != path {
			continue
		}
		return File{
			Content:      doc.Field(schema.ContentField).Contents(),
			Lang:         string(doc.Field(schema.LanguageField).Contents()),
			SelfImportID: string(doc.Field(schema.ImportIDField).Contents()),
			InRepo:       inRepoFromImports(doc.Field(schema.ImportsField).Contents()),
		}, true
	}
	return File{}, false
}

// inRepoFromImports builds a NavOptions.InRepo predicate from a file's stored
// `imports` field (the in-repo import identity terms the extractor kept).
func inRepoFromImports(imports []byte) func(string) bool {
	set := make(map[string]struct{})
	for term := range strings.FieldsSeq(string(imports)) {
		set[term] = struct{}{}
	}
	return func(importPath string) bool {
		_, ok := set["go:"+strings.ToLower(importPath)]
		return ok
	}
}

// DefLookup implements annotations.DefLookup over the codesearch index, resolving
// "files declaring symbol S in package P" as the intersection of the symbols
// and import_id posting lists.
type DefLookup struct {
	R *index.Reader
}

func (l *DefLookup) FindDefs(ctx context.Context, importID, symbolLower string) ([]annotations.DefFile, error) {
	sq := fmt.Sprintf("(:and (:eq %s %s) (:eq %s %s))",
		schema.SymbolsField, strconv.Quote(symbolLower),
		schema.ImportIDField, strconv.Quote(importID))
	matches, err := l.R.RawQuery(sq)
	if err != nil {
		return nil, err
	}
	out := make([]annotations.DefFile, 0, len(matches))
	for _, m := range matches {
		doc := l.R.GetStoredDocument(m.Docid())
		out = append(out, annotations.DefFile{
			Path:    string(doc.Field(schema.FilenameField).Contents()),
			Content: doc.Field(schema.ContentField).Contents(),
			Lang:    string(doc.Field(schema.LanguageField).Contents()),
		})
	}
	return out, nil
}

// FindReferencingFiles returns the files that may reference a symbol in the
// package: those importing it (cross-package uses) and those belonging to it
// (same-package uses), each with its decorate inputs. DefLookup thus satisfies
// annotations.RefLookup as well.
func (l *DefLookup) FindReferencingFiles(ctx context.Context, importID string) ([]annotations.RefFile, error) {
	q := strconv.Quote(importID)
	sq := fmt.Sprintf("(:or (:eq %s %s) (:eq %s %s))",
		schema.ImportsField, q, schema.ImportIDField, q)
	matches, err := l.R.RawQuery(sq)
	if err != nil {
		return nil, err
	}
	out := make([]annotations.RefFile, 0, len(matches))
	for _, m := range matches {
		doc := l.R.GetStoredDocument(m.Docid())
		out = append(out, annotations.RefFile{
			Path:         string(doc.Field(schema.FilenameField).Contents()),
			Content:      doc.Field(schema.ContentField).Contents(),
			Lang:         string(doc.Field(schema.LanguageField).Contents()),
			SelfImportID: string(doc.Field(schema.ImportIDField).Contents()),
			InRepo:       inRepoFromImports(doc.Field(schema.ImportsField).Contents()),
		})
	}
	return out, nil
}
