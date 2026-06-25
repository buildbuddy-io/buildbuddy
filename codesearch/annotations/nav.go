// Click-through code navigation ("go to definition", hover, find references)
// derived on demand from tree-sitter — the syntactic, kythe-free nav tier.
// Given a file's contents it produces the clickable references in that file
// (Decorate, per language); given the index's existing `symbols`/`import_id`
// posting lists (via DefLookup/RefLookup) it resolves a clicked reference to
// the file+span that defines it (Resolve), to documentation (Describe), or to
// every use site (FindReferences).
//
// The model is deliberately name-based, the same approach GitHub ships as its
// default "search-based" navigation: a reference is an identifier occurrence
// whose name matches a declared symbol. It needs no compilation and no
// semantic type resolution, so it works on any indexed snapshot of a repo.
//
// References carry an opaque target ticket under the tree-sitter:// scheme (see
// Scheme) encoding a definition's package import_id term and symbol name. The
// package half is exactly an `import_id` term as produced by extraction (e.g.
// `go:<lowercased import path>`). Resolution is then a single posting-list
// intersection — "files whose import_id is this package and whose symbols
// contain this name" — followed by a re-parse of those files to recover the
// declaration. The scheme also drives serving: the server routes a request to
// tree-sitter nav, instead of kythe, precisely when its ticket(s) carry the
// tree-sitter:// scheme, with no enabling flag. This layer is free of kythe and
// proto dependencies; the server maps these types to the kythe reply protos the
// frontend consumes.

package annotations

import (
	"context"
	"net/url"
	"slices"
	"strings"

	sitter "github.com/smacker/go-tree-sitter"
)

// Scheme is the URI scheme of every nav ticket. The server routes
// Decorations/CrossReferences/Docs/Xrefs requests to tree-sitter nav (rather
// than kythe) by this scheme alone — a request carrying tree-sitter:// tickets
// is served here; a kythe:// request goes to kythe. There is no global flag.
const Scheme = "tree-sitter"

const schemePrefix = Scheme + "://"

// HasScheme reports whether a ticket is a tree-sitter nav ticket (a file ticket
// like "tree-sitter://buildbuddy?path=..." or a symbol ticket from
// symbolTicket). The server uses it as the sole routing signal.
func HasScheme(ticket string) bool {
	return strings.HasPrefix(ticket, schemePrefix)
}

// Pos is a position in a file. Line is 1-based; Col is a 0-based byte offset
// within the line; Byte is the 0-based byte offset within the file. These match
// the conventions the code frontend expects (it adds 1 to Col to build a
// 1-based Monaco column, and reads Line directly as the 1-based line).
type Pos struct {
	Byte uint32
	Line uint32
	Col  uint32
}

// Ref is a clickable identifier occurrence within a file: the span to decorate
// plus the opaque ticket that resolves to its definition.
type Ref struct {
	Start Pos
	End   Pos
	// Kind is a kythe edge-kind string the frontend recognizes; references use
	// "/kythe/edge/ref".
	Kind string
	// TargetTicket is an opaque tree-sitter://symbol ticket. The frontend echoes
	// it back to Resolve and uses it as a highlight key, so every occurrence of
	// the same symbol shares one ticket.
	TargetTicket string
}

// Def is a top-level declaration: the symbol name and the span of the name
// token (the span navigation jumps to), plus documentation metadata for hover.
type Def struct {
	Name  string
	Start Pos
	End   Pos
	// Kind is the declaration kind: "func", "method", "type", "struct",
	// "interface", "const", or "var".
	Kind string
	// Signature is a one-line rendering of the declaration (e.g.
	// "func Infof(format string, args ...any)").
	Signature string
	// Doc is the leading doc comment, with comment markers stripped.
	Doc string
}

// Location is a resolved definition site: a repo-relative file path and the
// span of the declared name within it.
type Location struct {
	Path  string
	Start Pos
	End   Pos
}

// Definition is a resolved definition with documentation metadata, for hover.
type Definition struct {
	Path      string
	Start     Pos
	End       Pos
	Kind      string
	Signature string
	Doc       string
}

// DefFile is a candidate file that may declare a looked-up symbol, returned by
// a DefLookup.
type DefFile struct {
	Path    string
	Content []byte
	// Lang is the file's lowercase enry language name (as stored in the index's
	// language field), used to dispatch the right definition finder.
	Lang string
}

// DefLookup finds indexed files that may declare a symbol in a package. The
// server implements it over the codesearch index's `symbols`/`import_id`
// posting lists; tests implement it in memory.
type DefLookup interface {
	// FindDefs returns the files whose import_id term is importID and whose
	// symbols field contains symbolLower (already lowercased to match the
	// keyword tokenizer's normalization).
	FindDefs(ctx context.Context, importID, symbolLower string) ([]DefFile, error)
}

// RefFile is a candidate file that may reference a symbol, with the inputs
// needed to decorate it (the same inputs Decorate takes).
type RefFile struct {
	Path         string
	Content      []byte
	Lang         string
	SelfImportID string
	InRepo       func(importPath string) bool
}

// RefLookup finds candidate files that may reference symbols in a package:
// files that import the package (cross-package uses) and files belonging to it
// (same-package uses). The server implements it over the `imports`/`import_id`
// posting lists.
type RefLookup interface {
	FindReferencingFiles(ctx context.Context, importID string) ([]RefFile, error)
}

// Reference is a use site of a symbol: its span plus the source line it sits
// on, for display in the references panel.
type Reference struct {
	Path    string
	Start   Pos
	End     Pos
	Snippet string
}

const refEdgeKind = "/kythe/edge/ref"

// EdgeKindRef is the kythe edge kind the frontend renders for a reference.
const EdgeKindRef = refEdgeKind

// NavOptions controls reference extraction. Its fields are sourced from the
// viewed file's own indexed document, so no module/checkout context is needed.
type NavOptions struct {
	// SelfImportID is the viewed file's own import_id term (e.g.
	// "go:example.com/repo/svc"), as stored in the index. It is used to mint
	// same-package reference tickets; empty disables same-package references
	// (e.g. for a test file, which has no import_id).
	SelfImportID string

	// Path is the file's repo-relative path. Languages with path-based identity
	// (TS/JS) need it to resolve relative imports against the file's directory —
	// the directory can't be recovered from SelfImportID once an `index`
	// basename is collapsed. Go ignores it (its identities are module-based).
	Path string

	// InRepo reports whether an import path is resolvable within the indexed
	// corpus; package-selector references are emitted only for paths it accepts.
	// A nil predicate accepts everything — best effort, at the cost of
	// unresolvable selectors (e.g. stdlib) becoming clickable no-ops. Used by
	// Go; TS resolves relative specifiers directly and ignores it.
	InRepo func(importPath string) bool
}

// symbolTicket builds an opaque target ticket identifying a definition by its
// package's import_id term and symbol name, e.g.
//
//	tree-sitter://symbol?pkg=go%3Aexample.com%2Frepo%2Futil%2Flog&sym=Infof
//
// The frontend treats it as opaque (it echoes it back to CrossReferences and
// uses it as a per-symbol highlight key); url.Values.Encode sorts its keys, so
// the same (pkg, symbol) always yields the identical string. The format is
// language-neutral: pkg is any family's import_id term, sym any symbol name.
func symbolTicket(importTerm, symbol string) string {
	v := url.Values{}
	v.Set("pkg", importTerm)
	v.Set("sym", symbol)
	return schemePrefix + "symbol?" + v.Encode()
}

// parseSymbolTicket extracts the import_id term and symbol from a symbol
// ticket. ok is false for tickets this layer did not mint (e.g. a kythe ticket,
// or a tree-sitter file ticket).
func parseSymbolTicket(ticket string) (importID, symbol string, ok bool) {
	u, err := url.Parse(ticket)
	if err != nil || u.Scheme != Scheme {
		return "", "", false
	}
	q := u.Query()
	importID, symbol = q.Get("pkg"), q.Get("sym")
	if importID == "" || symbol == "" {
		return "", "", false
	}
	return importID, symbol, true
}

// Decorate parses a file of the given language and returns its clickable
// references. lang is the lowercase enry language name (as stored in the
// index's language field). Unsupported languages return no references and no
// error, so a mixed-language repo degrades to "only supported files decorate".
func Decorate(ctx context.Context, lang string, content []byte, opts NavOptions) ([]Ref, error) {
	switch lang {
	case "go":
		return decorateGo(ctx, content, opts)
	case "typescript", "tsx", "javascript", "jsx":
		return decorateTS(ctx, lang, content, opts)
	}
	return nil, nil
}

// definitions returns the top-level declarations in a file of the given
// language, for resolution (span) and hover (kind/signature/doc). Unsupported
// languages return nothing.
func definitions(ctx context.Context, lang string, content []byte) ([]Def, error) {
	switch lang {
	case "go":
		return goDefinitions(ctx, content)
	case "typescript", "tsx", "javascript", "jsx":
		return tsDefinitions(ctx, lang, content)
	}
	return nil, nil
}

// resolvedDef pairs a matched declaration with the file it was found in.
type resolvedDef struct {
	Path string
	Def  Def
}

// resolve decodes a target ticket, looks up candidate files via lk, and
// re-parses each (in its own language) to recover the matching declarations
// (the posting list tells us which file declares the name, not where or what).
// Tickets this layer did not mint resolve to nothing and no error.
func resolve(ctx context.Context, lk DefLookup, ticket string) ([]resolvedDef, error) {
	importID, sym, ok := parseSymbolTicket(ticket)
	if !ok {
		return nil, nil
	}
	symLower := strings.ToLower(sym)
	files, err := lk.FindDefs(ctx, importID, symLower)
	if err != nil {
		return nil, err
	}
	var out []resolvedDef
	for _, f := range files {
		defs, err := definitions(ctx, f.Lang, f.Content)
		if err != nil {
			continue // tolerate a single unparseable candidate
		}
		for _, d := range defs {
			if strings.ToLower(d.Name) == symLower {
				out = append(out, resolvedDef{Path: f.Path, Def: d})
			}
		}
	}
	return out, nil
}

// Resolve returns every definition site for a target ticket (the
// go-to-definition / CrossReferences path).
func Resolve(ctx context.Context, lk DefLookup, ticket string) ([]Location, error) {
	rs, err := resolve(ctx, lk, ticket)
	if err != nil || len(rs) == 0 {
		return nil, err
	}
	locs := make([]Location, 0, len(rs))
	for _, r := range rs {
		locs = append(locs, Location{Path: r.Path, Start: r.Def.Start, End: r.Def.End})
	}
	return locs, nil
}

// Describe returns every definition site for a target ticket with documentation
// metadata (kind, signature, doc comment) for hover.
func Describe(ctx context.Context, lk DefLookup, ticket string) ([]Definition, error) {
	rs, err := resolve(ctx, lk, ticket)
	if err != nil || len(rs) == 0 {
		return nil, err
	}
	defs := make([]Definition, 0, len(rs))
	for _, r := range rs {
		defs = append(defs, Definition{
			Path:      r.Path,
			Start:     r.Def.Start,
			End:       r.Def.End,
			Kind:      r.Def.Kind,
			Signature: r.Def.Signature,
			Doc:       r.Def.Doc,
		})
	}
	return defs, nil
}

// FindReferences returns every use site of a ticket's symbol across files that
// import its package or belong to it. It decorates each candidate file and
// keeps the references whose target ticket matches — exactly the occurrences
// the editor would make clickable and point back at this symbol. Tickets this
// layer did not mint return nothing.
func FindReferences(ctx context.Context, rl RefLookup, ticket string) ([]Reference, error) {
	importID, _, ok := parseSymbolTicket(ticket)
	if !ok {
		return nil, nil
	}
	files, err := rl.FindReferencingFiles(ctx, importID)
	if err != nil {
		return nil, err
	}
	var refs []Reference
	for _, f := range files {
		decs, err := Decorate(ctx, f.Lang, f.Content, NavOptions{SelfImportID: f.SelfImportID, Path: f.Path, InRepo: f.InRepo})
		if err != nil {
			continue // tolerate a single unparseable candidate
		}
		for _, d := range decs {
			if d.TargetTicket != ticket {
				continue
			}
			refs = append(refs, Reference{
				Path:    f.Path,
				Start:   d.Start,
				End:     d.End,
				Snippet: lineAt(f.Content, d.Start.Byte),
			})
		}
	}
	return refs, nil
}

// lineAt returns the trimmed source line containing the byte offset.
func lineAt(content []byte, offset uint32) string {
	if int(offset) > len(content) {
		return ""
	}
	start := 0
	for i := int(offset) - 1; i >= 0; i-- {
		if content[i] == '\n' {
			start = i + 1
			break
		}
	}
	end := len(content)
	for i := int(offset); i < len(content); i++ {
		if content[i] == '\n' {
			end = i
			break
		}
	}
	return strings.TrimSpace(string(content[start:end]))
}

// firstLine returns the first line of s, trimmed and with a trailing brace (the
// start of a body/struct/class block) dropped.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	return strings.TrimSpace(strings.TrimRight(strings.TrimSpace(s), "{"))
}

// collectComments gathers comment nodes directly above node, each on the line
// immediately above the previous, in source order. Works for any grammar whose
// comments are named nodes of type "comment" (Go, TS/JS, C/C++, ...).
func collectComments(node *sitter.Node, content []byte) string {
	var lines []string
	target := node
	for {
		prev := target.PrevNamedSibling()
		if prev == nil || prev.Type() != "comment" {
			break
		}
		if prev.EndPoint().Row+1 != target.StartPoint().Row {
			break // a blank line breaks the doc-comment block
		}
		lines = append(lines, cleanComment(prev.Content(content)))
		target = prev
	}
	slices.Reverse(lines)
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

// cleanComment strips comment markers from a comment node's text: a `//` line
// prefix, or `/* */` block delimiters with a leading `*` removed from each
// line (so JSDoc `/** * foo */` blocks read cleanly).
func cleanComment(s string) string {
	s = strings.TrimSpace(s)
	if rest, ok := strings.CutPrefix(s, "//"); ok {
		return strings.TrimSpace(rest)
	}
	s = strings.TrimSuffix(strings.TrimPrefix(s, "/*"), "*/")
	var out []string
	for line := range strings.SplitSeq(s, "\n") {
		line = strings.TrimSpace(line)
		line = strings.TrimPrefix(line, "*")
		out = append(out, strings.TrimSpace(line))
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

// nodeStart returns the start position of a tree-sitter node.
func nodeStart(n *sitter.Node) Pos {
	p := n.StartPoint()
	return Pos{Byte: n.StartByte(), Line: p.Row + 1, Col: p.Column}
}

// nodeEnd returns the end position of a tree-sitter node.
func nodeEnd(n *sitter.Node) Pos {
	p := n.EndPoint()
	return Pos{Byte: n.EndByte(), Line: p.Row + 1, Col: p.Column}
}
