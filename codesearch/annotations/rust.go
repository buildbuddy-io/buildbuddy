package annotations

import (
	"context"
	"strings"

	sitter "github.com/smacker/go-tree-sitter"
	"github.com/smacker/go-tree-sitter/rust"
)

// Rust support.
//
// Identity is module-scoped and pragmatic rather than exact: a file's ImportID
// is `rust:<module-path>` derived from its repo-relative path (crate-relative,
// `::`-joined), and each `use` declaration contributes the identity term(s) of
// the module path(s) it names. `crate::` is dropped (it refers to the current
// crate root); external crates are recorded too — like Java/Go external
// imports they never match any document's import_id and so add no in-degree.
// Exactness is not a goal; this is a fuzzy ranking signal.

// rustSymbolQuery captures declaration names: functions, types
// (struct/enum/trait/union/type-alias), modules, consts/statics, and
// macro definitions. Type-bearing items name a (type_identifier); the rest
// name an (identifier).
var rustSymbolQuery = mustCompileQuery(`
	(function_item name: (identifier) @sym)
	(struct_item name: (type_identifier) @sym)
	(enum_item name: (type_identifier) @sym)
	(trait_item name: (type_identifier) @sym)
	(union_item name: (type_identifier) @sym)
	(type_item name: (type_identifier) @sym)
	(mod_item name: (identifier) @sym)
	(const_item name: (identifier) @sym)
	(static_item name: (identifier) @sym)
	(macro_definition name: (identifier) @sym)
`, rust.GetLanguage())

// rustTerm formats a Rust module path as an identity term.
func rustTerm(path string) string {
	return "rust:" + strings.ToLower(path)
}

// rustModulePath maps a repo-relative file path to its crate module path. A
// leading `src/` is stripped, the `.rs` extension is dropped, `lib.rs` and
// `main.rs` collapse to the crate root (empty path), `mod.rs` collapses to its
// containing directory, and `/` is replaced by `::`. Returns "" for the crate
// root (no module identity).
func rustModulePath(relPath string) string {
	p := strings.TrimPrefix(relPath, "src/")
	p = strings.TrimSuffix(p, ".rs")
	// Crate roots have no module path.
	if p == "lib" || p == "main" {
		return ""
	}
	// `foo/mod.rs` is module `foo`.
	p = strings.TrimSuffix(p, "/mod")
	if p == "mod" {
		return ""
	}
	return strings.ReplaceAll(p, "/", "::")
}

// isRustTestFile reports whether the repo-relative path / module path is a test
// file and so should be excluded from import-rank identity. Rust unit tests
// live inline (`#[cfg(test)] mod tests`), so the only file-level conventions
// are an integration-test `tests/` directory and a `tests` module (a tests.rs
// file or tests/mod.rs, whose module path ends in `::tests`). A filename
// heuristic isn't needed and would misfire on ordinary names like
// latest_handler.rs / my_testbed.rs.
func isRustTestFile(relPath, modulePath string) bool {
	for seg := range strings.SplitSeq(relPath, "/") {
		if seg == "tests" {
			return true
		}
	}
	return strings.HasSuffix(modulePath, "::tests")
}

// rustUsePaths returns, in source order, the use-path strings of every
// `use_declaration` in the parsed file (the text following `use`, with the
// trailing `;` removed). Grouping and globs are preserved verbatim; resolution
// happens in rustResolveUse.
func rustUsePaths(tree *sitter.Tree, content []byte) []string {
	var paths []string
	root := tree.RootNode()
	var walk func(n *sitter.Node)
	walk = func(n *sitter.Node) {
		if n.Type() == "use_declaration" {
			// The argument field holds the path/list/wildcard following `use`.
			arg := n.ChildByFieldName("argument")
			if arg != nil {
				paths = append(paths, arg.Content(content))
			}
			return
		}
		for i := 0; i < int(n.NamedChildCount()); i++ {
			walk(n.NamedChild(i))
		}
	}
	walk(root)
	return paths
}

// rustResolveUse expands a single use-path string into zero or more module
// identity terms. `selfModule` is the importing file's module path, used to
// resolve `self::`/`super::`. Grouped imports (`a::{b, c}`) expand to the group
// prefix plus each member; globs (`a::*`) yield the prefix; `crate::` is
// dropped. Terms are accumulated into out (deduped by the caller's seen map is
// not done here; the caller dedupes).
func rustResolveUse(use, selfModule string) []string {
	use = strings.TrimSpace(use)
	if use == "" {
		return nil
	}

	// Grouped import: split off the `{...}` suffix and expand each member
	// against the shared prefix. Members may themselves be groups
	// (`a::{b, c::{d, e}}`), so split on top-level commas and recurse rather
	// than splitting the whole inner string naively.
	if open := strings.Index(use, "{"); open >= 0 && strings.HasSuffix(use, "}") {
		prefix := strings.TrimSuffix(strings.TrimSpace(use[:open]), "::")
		inner := use[open+1 : len(use)-1]
		var terms []string
		// The group prefix itself is a module identity (`a` in `a::{b, c}`).
		if base := rustResolvePath(prefix, selfModule); base != "" {
			terms = append(terms, rustTerm(base))
		}
		for _, member := range splitTopLevelCommas(inner) {
			member = strings.TrimSpace(member)
			if member == "" || member == "self" {
				continue
			}
			full := member
			if prefix != "" {
				full = prefix + "::" + member
			}
			terms = append(terms, rustResolveUse(full, selfModule)...)
		}
		return terms
	}

	// Glob import `a::*`: the module being glob-imported is the prefix.
	if strings.HasSuffix(use, "::*") || use == "*" {
		prefix := strings.TrimSuffix(use, "::*")
		if use == "*" {
			prefix = ""
		}
		if resolved := rustResolvePath(prefix, selfModule); resolved != "" {
			return []string{rustTerm(resolved)}
		}
		return nil
	}

	// Plain path, possibly with an `as` alias — drop the alias.
	if i := strings.Index(use, " as "); i >= 0 {
		use = strings.TrimSpace(use[:i])
	}
	if resolved := rustResolvePath(use, selfModule); resolved != "" {
		return []string{rustTerm(resolved)}
	}
	return nil
}

// splitTopLevelCommas splits on commas that aren't nested inside `{...}`, so a
// grouped-use member that is itself a group stays intact for recursion.
func splitTopLevelCommas(s string) []string {
	var parts []string
	depth, start := 0, 0
	for i, r := range s {
		switch r {
		case '{':
			depth++
		case '}':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	return append(parts, s[start:])
}

// rustResolvePath maps a single `::`-joined path to a crate-relative module
// path string (no `rust:` prefix). `crate::` is stripped; `self::x` resolves
// to selfModule + x; each leading `super::` climbs one parent of selfModule.
// External crate paths pass through verbatim (harmless, like Java external
// imports). Returns "" if nothing meaningful remains.
func rustResolvePath(path, selfModule string) string {
	path = strings.TrimSpace(path)
	if path == "" {
		return ""
	}
	switch {
	case path == "crate" || path == "self":
		return ""
	case strings.HasPrefix(path, "crate::"):
		return strings.TrimPrefix(path, "crate::")
	case strings.HasPrefix(path, "self::"):
		rest := strings.TrimPrefix(path, "self::")
		if selfModule == "" {
			return rest
		}
		return selfModule + "::" + rest
	case path == "super" || strings.HasPrefix(path, "super::"):
		// Climb one module per leading `super::`, so chains like
		// `super::super::x` reach the right ancestor.
		mod := selfModule
		for path == "super" || strings.HasPrefix(path, "super::") {
			mod = rustParentModule(mod)
			if path == "super" {
				path = ""
				break
			}
			path = strings.TrimPrefix(path, "super::")
		}
		switch {
		case path == "":
			return mod
		case mod == "":
			return path
		default:
			return mod + "::" + path
		}
	default:
		return path
	}
}

// rustParentModule returns the parent of a `::`-joined module path, or "" at
// the crate root.
func rustParentModule(module string) string {
	if i := strings.LastIndex(module, "::"); i >= 0 {
		return module[:i]
	}
	return ""
}

func extractRust(ctx context.Context, filename string, content []byte, rctx *RepoContext) (*Result, error) {
	parser := sitter.NewParser()
	// Close the parser only after the tree: the binding keeps the parser
	// alive for the tree's lifetime, so freeing it first is a use-after-free.
	// Defers run LIFO, so registering parser.Close before tree.Close gives
	// the right order.
	defer parser.Close()
	parser.SetLanguage(rust.GetLanguage())
	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()

	ann := &Result{Symbols: captureAll(rustSymbolQuery, tree, content)}

	// Identity (imports/import_id) is only meaningful for files inside the
	// repo; symbols are extracted regardless.
	rel := rctx.relPath(filename)
	if rel == "" {
		return ann, nil
	}

	selfModule := rustModulePath(rel)
	var selfTerm string
	if selfModule != "" {
		selfTerm = rustTerm(selfModule)
	}

	seen := make(map[string]struct{})
	var imports []string
	for _, use := range rustUsePaths(tree, content) {
		for _, term := range rustResolveUse(use, selfModule) {
			// Drop self-edges.
			if term == selfTerm && selfTerm != "" {
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

	if selfModule != "" && !isRustTestFile(rel, selfModule) {
		ann.ImportID = []string{selfTerm}
	}
	return ann, nil
}
