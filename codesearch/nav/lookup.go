// Package nav serves tree-sitter code navigation from the codesearch index.
// lookup.go implements the annotations package's DefLookup/RefLookup over the
// index's symbols/import_id/imports posting lists and loads a file's decorate
// inputs by path; handlers.go answers the code browser's navigation requests
// (decorations, go-to-definition, hover documentation, references panel) in
// the kythe reply protos the frontend renders. Shared by the server's
// KytheProxy dispatch and the CLI's nav subcommands so there is one
// implementation.
package nav

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/buildbuddy-io/buildbuddy/codesearch/index"
	"github.com/buildbuddy-io/buildbuddy/codesearch/schema"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
)

// maxReferenceFiles is the max number of files a ref panel can load.
const maxReferenceFiles = 1000

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

// FindFile loads the decorate inputs for a repo file, addressed by its exact
// document id (owner, repo, repo-relative path — the same key indexing wrote).
// ok is false if no such document exists in the reader's namespace. Because the
// lookup is an exact id match it is deterministic and unaffected by other repos
// in the namespace sharing the same path; and the path never passes through the
// `file:` query filter, so paths containing spaces resolve fine.
func FindFile(ctx context.Context, r *index.Reader, owner, repo, path string) (File, bool) {
	id := schema.FileID(owner, repo, path)
	sq := fmt.Sprintf("(:eq %s %s)", schema.IDField, strconv.Quote(string(id)))
	matches, err := r.RawQuery(sq)
	if err != nil || len(matches) == 0 {
		return File{}, false
	}
	doc := r.GetStoredDocument(matches[0].Docid())
	return File{
		Content:      doc.Field(schema.ContentField).Contents(),
		Lang:         string(doc.Field(schema.LanguageField).Contents()),
		SelfImportID: string(doc.Field(schema.ImportIDField).Contents()),
		InRepo:       inRepoFromImports(doc.Field(schema.ImportsField).Contents()),
	}, true
}

// inRepoFromImports builds a NavOptions.InRepo predicate from a file's stored
// `imports` field (the in-repo import identity terms the extractor kept).
//
// NOTE: Go-only. It reconstructs a Go identity term ("go:" + import path) to
// test membership, so it only recognizes Go package selectors. Other languages
// that want in-repo filtering will need their own term derivation here (TS
// resolves relative specifiers directly and ignores InRepo, so it is
// unaffected today).
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
			Owner:   string(doc.Field(schema.OwnerField).Contents()),
			Repo:    string(doc.Field(schema.RepoField).Contents()),
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
	if len(matches) > maxReferenceFiles {
		log.CtxInfof(ctx, "nav: %q has %d referencing files; scanning first %d",
			importID, len(matches), maxReferenceFiles)
		matches = matches[:maxReferenceFiles]
	}
	out := make([]annotations.RefFile, 0, len(matches))
	for _, m := range matches {
		doc := l.R.GetStoredDocument(m.Docid())
		out = append(out, annotations.RefFile{
			Owner:        string(doc.Field(schema.OwnerField).Contents()),
			Repo:         string(doc.Field(schema.RepoField).Contents()),
			Path:         string(doc.Field(schema.FilenameField).Contents()),
			Content:      doc.Field(schema.ContentField).Contents(),
			Lang:         string(doc.Field(schema.LanguageField).Contents()),
			SelfImportID: string(doc.Field(schema.ImportIDField).Contents()),
			InRepo:       inRepoFromImports(doc.Field(schema.ImportsField).Contents()),
		})
	}
	return out, nil
}
