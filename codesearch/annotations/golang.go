// Go extraction.

package annotations

import (
	"context"
	"path"
	"strconv"
	"strings"

	sitter "github.com/smacker/go-tree-sitter"
	"github.com/smacker/go-tree-sitter/golang"
)

// goTerm formats a Go import path as an identity term. Terms are lowercased
// to match the keyword tokenizer's normalization.
func goTerm(importPath string) string {
	return "go:" + strings.ToLower(importPath)
}

// goPackageImportPath returns the import path of the package containing the
// repo-relative file path.
func goPackageImportPath(modulePath, relPath string) string {
	dir := path.Dir(relPath)
	if dir == "." {
		return modulePath
	}
	return modulePath + "/" + dir
}

func extractGo(ctx context.Context, filename string, content []byte, rctx *RepoContext) (*Result, error) {
	parser := sitter.NewParser()
	// Close the parser only after the tree: the binding keeps the parser
	// alive for the tree's lifetime, so freeing it first is a use-after-free.
	// Defers run LIFO, so registering parser.Close before tree.Close gives
	// the right order.
	defer parser.Close()
	parser.SetLanguage(golang.GetLanguage())
	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()

	ann := &Result{Symbols: goSymbols(tree, content)}

	// Import-identity terms additionally need a module path to resolve
	// against, and a path within the module.
	modPath := rctx.goModulePath
	if modPath == "" {
		return ann, nil
	}
	rel := rctx.relPath(filename)
	if rel == "" {
		return ann, nil
	}

	importPaths := goImportPaths(tree, content)

	// Module "std" (the Go project itself) is special: its packages import
	// each other by bare path ("fmt", "cmd/internal/obj") with no module
	// prefix, and everything importable inside it is in-repo.
	std := modPath == "std"

	selfPath := goPackageImportPath(modPath, rel)
	if std {
		if dir := path.Dir(rel); dir == "." {
			selfPath = modPath
		} else {
			selfPath = dir
		}
	}
	// A vendored package is imported by its original path, not its on-disk
	// path: src/vendor/golang.org/x/net identifies as golang.org/x/net.
	if i := strings.LastIndex("/"+selfPath, "/vendor/"); i >= 0 {
		selfPath = ("/" + selfPath)[i+len("/vendor/"):]
	}

	seen := make(map[string]struct{}, len(importPaths))
	imports := make([]string, 0, len(importPaths))
	for _, p := range importPaths {
		// Keep only in-repo imports, and drop edges to the importing
		// file's own package. In std every import is in-repo.
		if !std && p != modPath && !strings.HasPrefix(p, modPath+"/") {
			continue
		}
		if p == selfPath {
			continue
		}
		term := goTerm(p)
		if _, ok := seen[term]; ok {
			continue
		}
		seen[term] = struct{}{}
		imports = append(imports, term)
	}

	ann.Imports = imports
	if !strings.HasSuffix(rel, "_test.go") {
		ann.ImportID = []string{goTerm(selfPath)}
	}
	return ann, nil
}

// goSymbolQuery captures declaration names: functions, methods, types
// (including aliases), and package-level consts/vars. Const/var captures are
// anchored to source_file so function-local declarations are excluded.
var goSymbolQuery = mustCompileQuery(`
	(function_declaration name: (identifier) @sym)
	(method_declaration name: (field_identifier) @sym)
	(type_spec name: (type_identifier) @sym)
	(type_alias name: (type_identifier) @sym)
	(source_file (const_declaration (const_spec name: (identifier) @sym)))
	(source_file (var_declaration (var_spec name: (identifier) @sym)))
`, golang.GetLanguage())

// goSymbols returns the declared names in the parsed file, in source order,
// lowercased to match the keyword tokenizer's normalization.
func goSymbols(tree *sitter.Tree, content []byte) []string {
	return captureAll(goSymbolQuery, tree, content)
}

// goImportPaths returns the unquoted import paths of the parsed file, in
// source order.
func goImportPaths(tree *sitter.Tree, content []byte) []string {
	var paths []string
	var collectSpecs func(n *sitter.Node)
	collectSpecs = func(n *sitter.Node) {
		if n.Type() == "import_spec" {
			pathNode := n.ChildByFieldName("path")
			if pathNode != nil {
				if p, err := strconv.Unquote(pathNode.Content(content)); err == nil && p != "" {
					paths = append(paths, p)
				}
			}
			return
		}
		for i := 0; i < int(n.NamedChildCount()); i++ {
			collectSpecs(n.NamedChild(i))
		}
	}

	// Import declarations only appear at the top level of a source file, so
	// only their subtrees need to be walked.
	root := tree.RootNode()
	for i := 0; i < int(root.NamedChildCount()); i++ {
		child := root.NamedChild(i)
		if child.Type() == "import_declaration" {
			collectSpecs(child)
		}
	}
	return paths
}

// Code navigation (decorateGo / goDefinitions) for Go. These reuse the same
// identity vocabulary as extraction (goTerm / goPackageImportPath) so nav
// tickets join against the `import_id` terms the index already stores.

// GoSelfImportID returns the import_id term for the package containing the
// repo-relative file relPath in a module rooted at modulePath, or "" when it
// can't be determined (no module path, a path outside the module, or a test
// file — test files aren't part of a package's importable surface, mirroring
// the extractor's `import_id` rule). It's a convenience for callers that have
// module context; the server instead reads the term from the index.
func GoSelfImportID(modulePath, relPath string) string {
	if modulePath == "" || relPath == "" || strings.HasSuffix(relPath, "_test.go") {
		return ""
	}
	return goTerm(goPackageImportPath(modulePath, relPath))
}

// GoModuleInRepo returns a NavOptions.InRepo predicate that accepts import
// paths within the module rooted at modulePath. It returns nil for an empty
// module path, which Decorate treats as "accept everything".
func GoModuleInRepo(modulePath string) func(string) bool {
	if modulePath == "" {
		return nil
	}
	return func(imp string) bool {
		return imp == modulePath || strings.HasPrefix(imp, modulePath+"/")
	}
}

// goImportMap maps each imported package's local name (its alias, or the last
// path element by default) to its import path. Blank and dot imports are
// skipped — neither yields a usable package selector.
func goImportMap(root *sitter.Node, content []byte) map[string]string {
	out := make(map[string]string)
	var visit func(n *sitter.Node)
	visit = func(n *sitter.Node) {
		if n.Type() == "import_spec" {
			pathNode := n.ChildByFieldName("path")
			if pathNode == nil {
				return
			}
			imp, err := strconv.Unquote(pathNode.Content(content))
			if err != nil || imp == "" {
				return
			}
			local := path.Base(imp)
			if nameNode := n.ChildByFieldName("name"); nameNode != nil {
				name := nameNode.Content(content)
				if name == "_" || name == "." {
					return // blank/dot import: no usable selector
				}
				local = name
			}
			out[local] = imp
			return
		}
		for i := 0; i < int(n.NamedChildCount()); i++ {
			visit(n.NamedChild(i))
		}
	}
	for i := 0; i < int(root.NamedChildCount()); i++ {
		if child := root.NamedChild(i); child.Type() == "import_declaration" {
			visit(child)
		}
	}
	return out
}

// decorateGo parses a Go file and returns the clickable references in it. It
// produces two kinds of reference, all sharing the tree-sitter://symbol ticket
// shape so they resolve through one path:
//
//   - package selectors (`log.Infof`): the field is decorated, targeting the
//     imported package's identity. opts.InRepo limits these to resolvable
//     packages; with no predicate, external selectors are still decorated but
//     resolve to nothing on click.
//   - same-package names: a bare identifier whose name matches a file-scope
//     declaration in this file, targeting this file's own package
//     (opts.SelfImportID). Disabled when SelfImportID is empty.
//
// This is name-based, like GitHub's syntactic nav: it does not perform scope
// resolution, so a local variable shadowing a declared name still decorates.
func decorateGo(ctx context.Context, content []byte, opts NavOptions) ([]Ref, error) {
	parser := sitter.NewParser()
	// Close the parser only after the tree: the binding keeps the parser
	// alive for the tree's lifetime (defers run LIFO).
	defer parser.Close()
	parser.SetLanguage(golang.GetLanguage())
	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()
	root := tree.RootNode()

	importMap := goImportMap(root, content)
	selfID := opts.SelfImportID
	inRepo := opts.InRepo
	if inRepo == nil {
		inRepo = func(string) bool { return true }
	}

	// File-scope declaration names (lowercased) and the byte offsets of the
	// declaration name tokens themselves, so a declaration site isn't also
	// decorated as a reference to itself.
	declNames := make(map[string]struct{})
	declSites := make(map[uint32]struct{})
	defs, err := goDefinitions(ctx, content)
	if err != nil {
		return nil, err
	}
	for _, d := range defs {
		declNames[strings.ToLower(d.Name)] = struct{}{}
		declSites[d.Start.Byte] = struct{}{}
	}

	// consumed marks byte offsets handled as part of a package selector (the
	// operand and field tokens), so the same tokens aren't reconsidered as
	// same-package references.
	consumed := make(map[uint32]struct{})
	var refs []Ref

	var walk func(n *sitter.Node)
	walk = func(n *sitter.Node) {
		switch n.Type() {
		case "selector_expression":
			operand := n.ChildByFieldName("operand")
			field := n.ChildByFieldName("field")
			if operand != nil && operand.Type() == "identifier" && field != nil {
				consumed[operand.StartByte()] = struct{}{}
				if imp, ok := importMap[operand.Content(content)]; ok && inRepo(imp) {
					consumed[field.StartByte()] = struct{}{}
					refs = append(refs, Ref{
						Start:        nodeStart(field),
						End:          nodeEnd(field),
						Kind:         refEdgeKind,
						TargetTicket: symbolTicket(goTerm(imp), field.Content(content)),
					})
				}
			}
		case "identifier", "type_identifier":
			if selfID == "" {
				break
			}
			if _, skip := consumed[n.StartByte()]; skip {
				break
			}
			if _, isDecl := declSites[n.StartByte()]; isDecl {
				break
			}
			name := n.Content(content)
			if _, ok := declNames[strings.ToLower(name)]; ok {
				refs = append(refs, Ref{
					Start:        nodeStart(n),
					End:          nodeEnd(n),
					Kind:         refEdgeKind,
					TargetTicket: symbolTicket(selfID, name),
				})
			}
		}
		for i := 0; i < int(n.NamedChildCount()); i++ {
			walk(n.NamedChild(i))
		}
	}
	walk(root)

	return refs, nil
}

// goDefSpecQuery captures file-scope declarations: functions, methods, types
// (incl. aliases), and package-level consts/vars. Each match binds @sym (the
// name token, for the span) and @decl (the enclosing declaration node, for
// kind/signature/doc). Const/var captures are anchored to source_file so
// function-local declarations are excluded.
var goDefSpecQuery = mustCompileQuery(`
	(function_declaration name: (identifier) @sym) @decl
	(method_declaration name: (field_identifier) @sym) @decl
	(type_spec name: (type_identifier) @sym) @decl
	(type_alias name: (type_identifier) @sym) @decl
	(source_file (const_declaration (const_spec name: (identifier) @sym) @decl))
	(source_file (var_declaration (var_spec name: (identifier) @sym) @decl))
`, golang.GetLanguage())

// goDefinitions returns the file-scope declarations in a Go file, each with its
// original-case name, the span of the name token, and documentation metadata
// (kind, one-line signature, leading doc comment).
func goDefinitions(ctx context.Context, content []byte) ([]Def, error) {
	parser := sitter.NewParser()
	defer parser.Close()
	parser.SetLanguage(golang.GetLanguage())
	tree, err := parser.ParseCtx(ctx, nil, content)
	if err != nil {
		return nil, err
	}
	defer tree.Close()

	qc := sitter.NewQueryCursor()
	defer qc.Close()
	qc.Exec(goDefSpecQuery, tree.RootNode())

	var defs []Def
	for {
		m, ok := qc.NextMatch()
		if !ok {
			break
		}
		var nameNode, declNode *sitter.Node
		for _, c := range m.Captures {
			switch goDefSpecQuery.CaptureNameForId(c.Index) {
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
			d.Kind = goDeclKind(declNode)
			d.Signature = goSignature(declNode, content)
			d.Doc = goLeadingComment(declNode, content)
		}
		defs = append(defs, d)
	}
	return defs, nil
}

// goDeclKind classifies a declaration node.
func goDeclKind(decl *sitter.Node) string {
	switch decl.Type() {
	case "function_declaration":
		return "func"
	case "method_declaration":
		return "method"
	case "type_alias":
		return "type"
	case "type_spec":
		if t := decl.ChildByFieldName("type"); t != nil {
			switch t.Type() {
			case "struct_type":
				return "struct"
			case "interface_type":
				return "interface"
			}
		}
		return "type"
	case "const_spec":
		return "const"
	case "var_spec":
		return "var"
	}
	return ""
}

// goSignature renders a declaration as a single line: for funcs/methods, the
// text up to the body; for others, the first line, with the leading keyword
// restored (the spec nodes don't include it).
func goSignature(decl *sitter.Node, content []byte) string {
	switch decl.Type() {
	case "function_declaration", "method_declaration":
		if body := decl.ChildByFieldName("body"); body != nil {
			sig := string(content[decl.StartByte():body.StartByte()])
			return strings.Join(strings.Fields(sig), " ")
		}
		return firstLine(decl.Content(content))
	case "type_spec", "type_alias":
		return "type " + firstLine(decl.Content(content))
	case "const_spec":
		return "const " + firstLine(decl.Content(content))
	case "var_spec":
		return "var " + firstLine(decl.Content(content))
	}
	return firstLine(decl.Content(content))
}

// goLeadingComment returns the contiguous doc-comment lines immediately above a
// declaration, with comment markers stripped. For const/var specs the comment
// often sits above the enclosing declaration, so that is tried as a fallback.
func goLeadingComment(decl *sitter.Node, content []byte) string {
	doc := collectComments(decl, content)
	if doc == "" {
		switch decl.Type() {
		case "const_spec", "var_spec":
			if parent := decl.Parent(); parent != nil {
				doc = collectComments(parent, content)
			}
		}
	}
	return doc
}
