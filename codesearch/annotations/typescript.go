// TypeScript / JavaScript extraction.

package annotations

import (
	"context"
	"path"
	"strings"

	sitter "github.com/smacker/go-tree-sitter"
	"github.com/smacker/go-tree-sitter/javascript"
	"github.com/smacker/go-tree-sitter/typescript/tsx"
	"github.com/smacker/go-tree-sitter/typescript/typescript"
)

// TypeScript/JavaScript support.
//
// Identity is PATH-based and fuzzy: a file's identity term is its repo-relative
// path with the extension stripped and an `index` basename collapsed to its
// directory (`foo/index` -> `foo`). Imports are resolved relatively against the
// importing file's directory, so the identity is self-consistent even though it
// keeps any `src/` prefix. TS and JS share the `ts:` family because they import
// each other freely.

// Functions, methods, and top-level variable declarators have identical shapes
// in both grammars; class names differ (JS: identifier, TS: type_identifier),
// so the class clause lives in each grammar's query. Variable declarators are
// scoped to top-level and exported declarations so that locals inside function
// bodies (loop counters, temporaries) don't flood the symbol index — per the
// Symbols contract, only names a file *declares* about itself count.
const tsSymbolQueryCommon = `
	(function_declaration name: (identifier) @sym)
	(method_definition name: (property_identifier) @sym)
	(program (lexical_declaration (variable_declarator name: (identifier) @sym)))
	(program (variable_declaration (variable_declarator name: (identifier) @sym)))
	(export_statement (lexical_declaration (variable_declarator name: (identifier) @sym)))
	(export_statement (variable_declaration (variable_declarator name: (identifier) @sym)))
`

// tsSymbolQueryJS is the JavaScript-grammar query: the JS grammar has no
// interface/type/enum nodes and names classes with a plain identifier.
const tsSymbolQueryJS = tsSymbolQueryCommon + `
	(class_declaration name: (identifier) @sym)
`

// tsSymbolQueryTS additionally captures TypeScript-only declarations:
// interfaces, type aliases, and enums. These node types don't exist in the
// JavaScript grammar, so this query is only compiled against the TS/TSX
// grammars. The TS grammar names classes with a type_identifier.
const tsSymbolQueryTS = tsSymbolQueryCommon + `
	(class_declaration name: (type_identifier) @sym)
	(interface_declaration name: (type_identifier) @sym)
	(type_alias_declaration name: (type_identifier) @sym)
	(enum_declaration name: (identifier) @sym)
`

// Import specifiers come from static `import`/`export ... from "spec"` (the
// `source` field), dynamic `import("spec")` (an (import) node as the call
// target), and `require("spec")`. The call_expression arm captures the callee
// (@fn) so non-import calls can be filtered out in tsImportSpecs — the query
// language alone can't restrict it to `require`, and a bare f("string") would
// otherwise forge a phantom import edge.
const tsImportPattern = `
	(import_statement source: (string) @spec)
	(export_statement source: (string) @spec)
	(call_expression
		function: [(identifier) @fn (import)]
		arguments: (arguments (string) @spec))
`

var (
	tsxSymbolQuery = mustCompileQuery(tsSymbolQueryTS, tsx.GetLanguage())
	tsSymbolQuery  = mustCompileQuery(tsSymbolQueryTS, typescript.GetLanguage())
	jsSymbolQuery  = mustCompileQuery(tsSymbolQueryJS, javascript.GetLanguage())

	tsxImportQuery = mustCompileQuery(tsImportPattern, tsx.GetLanguage())
	tsImportQuery  = mustCompileQuery(tsImportPattern, typescript.GetLanguage())
	jsImportQuery  = mustCompileQuery(tsImportPattern, javascript.GetLanguage())
)

// tsGrammar returns the tree-sitter language and the matching symbol/import
// queries for the enry language name.
func tsGrammar(lang string) (*sitter.Language, *sitter.Query, *sitter.Query) {
	switch lang {
	case "tsx":
		return tsx.GetLanguage(), tsxSymbolQuery, tsxImportQuery
	case "typescript":
		return typescript.GetLanguage(), tsSymbolQuery, tsImportQuery
	default: // "javascript", "jsx"
		return javascript.GetLanguage(), jsSymbolQuery, jsImportQuery
	}
}

// tsTerm formats a normalized path as an identity term.
func tsTerm(p string) string {
	return "ts:" + strings.ToLower(p)
}

// tsExtensions are the source extensions stripped to normalize a path. `.d.ts`
// is handled before this list (it must be checked as a compound suffix).
var tsExtensions = []string{".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs"}

// tsNormalize maps a repo-relative (or import-resolved) path to its identity
// path: strip a known source extension (including the compound `.d.ts`) and
// collapse an `index` basename to its directory (`foo/index` -> `foo`).
func tsNormalize(p string) string {
	if before, ok := strings.CutSuffix(p, ".d.ts"); ok {
		p = before
	} else {
		for _, ext := range tsExtensions {
			if before, ok := strings.CutSuffix(p, ext); ok {
				p = before
				break
			}
		}
	}
	if path.Base(p) == "index" {
		if dir := path.Dir(p); dir != "." {
			p = dir
		}
	}
	return p
}

// isTSTestFile reports whether the repo-relative path is a test file and so
// should be excluded from import-rank identity. Heuristic: a basename
// containing `.test.` or `.spec.`, or a `__tests__` path segment.
func isTSTestFile(relPath string) bool {
	base := path.Base(relPath)
	if strings.Contains(base, ".test.") || strings.Contains(base, ".spec.") {
		return true
	}
	for seg := range strings.SplitSeq(relPath, "/") {
		if seg == "__tests__" {
			return true
		}
	}
	return false
}

// tsStringLiteral unquotes a string-literal node's text, dropping the
// surrounding quotes (single, double, or backtick).
func tsStringLiteral(s string) string {
	if len(s) >= 2 {
		q := s[0]
		if (q == '"' || q == '\'' || q == '`') && s[len(s)-1] == q {
			return s[1 : len(s)-1]
		}
	}
	return s
}

// tsImportSpecs returns the raw import-specifier string-literal node texts from
// a parsed file: the source of static import/export statements, the argument of
// dynamic import("..."), and the argument of require("..."). The callee name
// (@fn) is never returned, and any other f("string") call is skipped so it
// can't forge a phantom import edge.
func tsImportSpecs(q *sitter.Query, tree *sitter.Tree, content []byte) []string {
	qc := sitter.NewQueryCursor()
	defer qc.Close()
	qc.Exec(q, tree.RootNode())

	var specs []string
	for {
		m, ok := qc.NextMatch()
		if !ok {
			break
		}
		var fn, spec string
		haveFn, haveSpec := false, false
		for _, c := range m.Captures {
			switch q.CaptureNameForId(c.Index) {
			case "fn":
				fn, haveFn = c.Node.Content(content), true
			case "spec":
				spec, haveSpec = c.Node.Content(content), true
			}
		}
		if !haveSpec {
			continue
		}
		// A named-function call counts only when it's require(...); dynamic
		// import(...) matches the (import) alternative and has no @fn.
		if haveFn && fn != "require" {
			continue
		}
		specs = append(specs, spec)
	}
	return specs
}

func extractTypeScript(ctx context.Context, lang, filename string, content []byte, rctx *RepoContext) (*Result, error) {
	grammar, symbolQuery, importQuery := tsGrammar(lang)

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

	ann := &Result{Symbols: captureAll(symbolQuery, tree, content)}

	// Identity (imports/import_id) is only meaningful for files inside the
	// repo; symbols are extracted regardless.
	rel := rctx.relPath(filename)
	if rel == "" {
		return ann, nil
	}

	self := tsNormalize(rel)
	selfDir := path.Dir(rel)

	seen := make(map[string]struct{})
	addTerm := func(p string) []string {
		term := tsTerm(p)
		if term == tsTerm(self) {
			return nil // drop self-edges
		}
		if _, ok := seen[term]; ok {
			return nil
		}
		seen[term] = struct{}{}
		return []string{term}
	}

	var imports []string
	for _, raw := range tsImportSpecs(importQuery, tree, content) {
		spec := tsStringLiteral(raw)
		if spec == "" {
			continue
		}
		if strings.HasPrefix(spec, "./") || strings.HasPrefix(spec, "../") {
			// Relative spec: resolve against the importing file's directory,
			// then normalize. Record both the plain-file candidate and the
			// `/index`-collapsed candidate so either on-disk layout matches.
			resolved := path.Clean(path.Join(selfDir, spec))
			norm := tsNormalize(resolved)
			imports = append(imports, addTerm(norm)...)
			imports = append(imports, addTerm(norm+"/index")...)
		} else {
			// Bare spec (`react`, `@scope/pkg`, `node:fs`): external. Record it
			// anyway; it's harmless and never matches any in-repo ImportID.
			imports = append(imports, addTerm(spec)...)
		}
	}
	ann.Imports = imports

	if !isTSTestFile(rel) {
		ann.ImportID = []string{tsTerm(self)}
	}
	return ann, nil
}

// ── Code navigation ──────────────────────────────────────────────────────────
//
// TS/JS resolve imported symbols by their local binding, not a package
// qualifier: `import {foo as f} from "./m"` makes a bare `f` a reference to
// foo in module m. So decoration builds a local-name -> (module, symbol) map
// from the import statements (mirroring the extractor's relative-path
// resolution so tickets join against stored import_id terms), then walks uses.

// tsDefPatternCommon binds @sym (the declared name) and @decl (the declaration
// node, for kind/signature/doc) for declarations shared by both grammars.
// Variable declarators are scoped to top-level/exported, matching the symbol
// query, so locals don't become navigable definitions.
const tsDefPatternCommon = `
	(function_declaration name: (identifier) @sym) @decl
	(program (lexical_declaration (variable_declarator name: (identifier) @sym)) @decl)
	(program (variable_declaration (variable_declarator name: (identifier) @sym)) @decl)
	(export_statement (lexical_declaration (variable_declarator name: (identifier) @sym)) @decl)
	(export_statement (variable_declaration (variable_declarator name: (identifier) @sym)) @decl)
`

const tsDefPatternJS = tsDefPatternCommon + `
	(class_declaration name: (identifier) @sym) @decl
`

const tsDefPatternTS = tsDefPatternCommon + `
	(class_declaration name: (type_identifier) @sym) @decl
	(interface_declaration name: (type_identifier) @sym) @decl
	(type_alias_declaration name: (type_identifier) @sym) @decl
	(enum_declaration name: (identifier) @sym) @decl
`

var (
	tsxDefQuery = mustCompileQuery(tsDefPatternTS, tsx.GetLanguage())
	tsDefQuery  = mustCompileQuery(tsDefPatternTS, typescript.GetLanguage())
	jsDefQuery  = mustCompileQuery(tsDefPatternJS, javascript.GetLanguage())
)

// tsNavGrammar returns the grammar and definition query for the enry language.
func tsNavGrammar(lang string) (*sitter.Language, *sitter.Query) {
	switch lang {
	case "tsx":
		return tsx.GetLanguage(), tsxDefQuery
	case "typescript":
		return typescript.GetLanguage(), tsDefQuery
	default: // "javascript", "jsx"
		return javascript.GetLanguage(), jsDefQuery
	}
}

// tsModuleTerm resolves a relative import specifier against the importing
// file's directory to the imported module's identity term, matching how
// extraction derives import_id. ok is false for bare (external) specifiers.
func tsModuleTerm(selfDir, spec string) (string, bool) {
	if !strings.HasPrefix(spec, "./") && !strings.HasPrefix(spec, "../") {
		return "", false
	}
	resolved := path.Clean(path.Join(selfDir, spec))
	return tsTerm(tsNormalize(resolved)), true
}

// tsBinding is an imported symbol's target: its module identity term and its
// name in that module (the original name, not the local alias).
type tsBinding struct {
	module string
	sym    string
}

// tsImportBindings builds, from a file's import statements, the maps used to
// resolve uses: named bindings (local name -> module+symbol, for bare
// identifier uses) and namespace bindings (local name -> module, for `ns.foo`
// member uses). Only relative (in-repo) imports are recorded; default imports
// are skipped (a module's default export has no indexed symbol identity).
func tsImportBindings(root *sitter.Node, content []byte, selfDir string) (named map[string]tsBinding, ns map[string]string) {
	named = map[string]tsBinding{}
	ns = map[string]string{}
	for i := 0; i < int(root.NamedChildCount()); i++ {
		stmt := root.NamedChild(i)
		if stmt.Type() != "import_statement" {
			continue
		}
		src := stmt.ChildByFieldName("source")
		if src == nil {
			continue
		}
		module, ok := tsModuleTerm(selfDir, tsStringLiteral(src.Content(content)))
		if !ok {
			continue
		}
		for j := 0; j < int(stmt.NamedChildCount()); j++ {
			clause := stmt.NamedChild(j)
			if clause.Type() != "import_clause" {
				continue
			}
			for k := 0; k < int(clause.NamedChildCount()); k++ {
				c := clause.NamedChild(k)
				switch c.Type() {
				case "named_imports":
					for s := 0; s < int(c.NamedChildCount()); s++ {
						spec := c.NamedChild(s)
						if spec.Type() != "import_specifier" {
							continue
						}
						nameNode := spec.ChildByFieldName("name")
						if nameNode == nil {
							continue
						}
						local := nameNode
						if alias := spec.ChildByFieldName("alias"); alias != nil {
							local = alias
						}
						named[local.Content(content)] = tsBinding{module: module, sym: nameNode.Content(content)}
					}
				case "namespace_import":
					// `* as ns`: the binding is the identifier child.
					for s := 0; s < int(c.NamedChildCount()); s++ {
						if id := c.NamedChild(s); id.Type() == "identifier" {
							ns[id.Content(content)] = module
						}
					}
				}
				// A bare (identifier) child is a default import — skipped.
			}
		}
	}
	return named, ns
}

// decorateTS parses a TS/JS file and returns its clickable references: uses of
// named imports (resolved to their source module + original name), uses of
// namespace members (`ns.foo`), and uses of this file's own top-level
// declarations (same-module refs, via opts.SelfImportID).
func decorateTS(ctx context.Context, lang string, content []byte, opts NavOptions) ([]Ref, error) {
	grammar, _ := tsNavGrammar(lang)
	parser := sitter.NewParser()
	defer parser.Close()
	parser.SetLanguage(grammar)
	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()
	root := tree.RootNode()

	named, ns := tsImportBindings(root, content, path.Dir(opts.Path))
	selfID := opts.SelfImportID

	declNames := make(map[string]struct{})
	declSites := make(map[uint32]struct{})
	defs, err := tsDefinitions(ctx, lang, content)
	if err != nil {
		return nil, err
	}
	for _, d := range defs {
		declNames[d.Name] = struct{}{}
		declSites[d.Start.Byte] = struct{}{}
	}

	consumed := make(map[uint32]struct{})
	var refs []Ref

	var walk func(n *sitter.Node)
	walk = func(n *sitter.Node) {
		switch n.Type() {
		case "import_statement":
			return // bindings handled above; don't decorate the import clause
		case "member_expression":
			object := n.ChildByFieldName("object")
			property := n.ChildByFieldName("property")
			if object != nil && object.Type() == "identifier" && property != nil {
				consumed[object.StartByte()] = struct{}{}
				if module, ok := ns[object.Content(content)]; ok {
					consumed[property.StartByte()] = struct{}{}
					refs = append(refs, Ref{
						Start:        nodeStart(property),
						End:          nodeEnd(property),
						Kind:         refEdgeKind,
						TargetTicket: symbolTicket(module, property.Content(content)),
					})
				}
			}
		case "identifier", "type_identifier":
			if _, skip := consumed[n.StartByte()]; skip {
				break
			}
			if _, isDecl := declSites[n.StartByte()]; isDecl {
				break
			}
			name := n.Content(content)
			if b, ok := named[name]; ok {
				refs = append(refs, Ref{
					Start:        nodeStart(n),
					End:          nodeEnd(n),
					Kind:         refEdgeKind,
					TargetTicket: symbolTicket(b.module, b.sym),
				})
			} else if selfID != "" {
				if _, ok := declNames[name]; ok {
					refs = append(refs, Ref{
						Start:        nodeStart(n),
						End:          nodeEnd(n),
						Kind:         refEdgeKind,
						TargetTicket: symbolTicket(selfID, name),
					})
				}
			}
		}
		for i := 0; i < int(n.NamedChildCount()); i++ {
			walk(n.NamedChild(i))
		}
	}
	walk(root)

	return refs, nil
}

// tsDefinitions returns the top-level declarations in a TS/JS file, each with
// its name, name-token span, and documentation metadata.
func tsDefinitions(ctx context.Context, lang string, content []byte) ([]Def, error) {
	grammar, defQuery := tsNavGrammar(lang)
	parser := sitter.NewParser()
	defer parser.Close()
	parser.SetLanguage(grammar)
	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()

	qc := sitter.NewQueryCursor()
	defer qc.Close()
	qc.Exec(defQuery, tree.RootNode())

	var defs []Def
	for {
		m, ok := qc.NextMatch()
		if !ok {
			break
		}
		var nameNode, declNode *sitter.Node
		for _, c := range m.Captures {
			switch defQuery.CaptureNameForId(c.Index) {
			case "sym":
				nameNode = c.Node
			case "decl":
				declNode = c.Node
			}
		}
		if nameNode == nil {
			continue
		}
		name := nameNode.Content(content)
		if name == "" {
			continue
		}
		d := Def{Name: name, Start: nodeStart(nameNode), End: nodeEnd(nameNode)}
		if declNode != nil {
			d.Kind = tsDeclKind(declNode, content)
			d.Signature = tsSignature(declNode, content)
			d.Doc = tsLeadingComment(declNode, content)
		}
		defs = append(defs, d)
	}
	return defs, nil
}

// tsDeclKind classifies a TS/JS declaration node.
func tsDeclKind(decl *sitter.Node, content []byte) string {
	switch decl.Type() {
	case "function_declaration":
		return "func"
	case "class_declaration":
		return "class"
	case "interface_declaration":
		return "interface"
	case "type_alias_declaration":
		return "type"
	case "enum_declaration":
		return "enum"
	case "variable_declaration":
		return "var"
	case "lexical_declaration":
		if decl.NamedChildCount() > 0 && strings.HasPrefix(decl.Content(content), "let") {
			return "var"
		}
		return "const"
	}
	return ""
}

// tsSignature renders a TS/JS declaration as a single line: for declarations
// with a body block, the text up to the body; otherwise the first line.
func tsSignature(decl *sitter.Node, content []byte) string {
	switch decl.Type() {
	case "function_declaration", "class_declaration", "interface_declaration", "enum_declaration":
		if body := decl.ChildByFieldName("body"); body != nil {
			return strings.Join(strings.Fields(string(content[decl.StartByte():body.StartByte()])), " ")
		}
	}
	return firstLine(decl.Content(content))
}

// tsLeadingComment returns the doc comment above a declaration, falling back to
// the comment above the enclosing export_statement for exported declarations
// (where the comment sits above `export`, not the declaration itself).
func tsLeadingComment(decl *sitter.Node, content []byte) string {
	doc := collectComments(decl, content)
	if doc == "" {
		if p := decl.Parent(); p != nil && p.Type() == "export_statement" {
			doc = collectComments(p, content)
		}
	}
	return doc
}
