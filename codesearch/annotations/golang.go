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
	tree, err := parseGo(ctx, content)
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

func parseGo(ctx context.Context, content []byte) (*sitter.Tree, error) {
	parser := sitter.NewParser()
	defer parser.Close() // the returned tree owns its data and outlives the parser
	parser.SetLanguage(golang.GetLanguage())
	return parser.ParseCtx(ctx, nil, content)
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
