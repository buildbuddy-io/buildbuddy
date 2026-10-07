// Package bzlmod inspects a workspace's MODULE.bazel, including any files it
// pulls in with include().
//
// Repos often split their module file across several files with include()
// (e.g. one for bazel_deps, one for go_deps), so anything that asks "does this
// module already depend on X?" needs to look at all of them, not just
// MODULE.bazel itself.
package bzlmod

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/bazel-contrib/buildtools/v10/build"
)

const ModuleFileName = "MODULE.bazel"

// Module is a parsed MODULE.bazel plus everything it transitively includes.
type Module struct {
	// Files holds the root MODULE.bazel first, then included files in the
	// order they're included.
	Files []*build.File
}

// Load parses <workspaceDir>/MODULE.bazel and every file it transitively
// include()s.
func Load(workspaceDir string) (*Module, error) {
	m := &Module{}
	if err := m.load(workspaceDir, ModuleFileName, map[string]bool{}); err != nil {
		return nil, err
	}
	return m, nil
}

// load parses the file at relPath (relative to workspaceDir) and recurses into
// its includes.
func (m *Module) load(workspaceDir, relPath string, seen map[string]bool) error {
	if seen[relPath] {
		return nil
	}
	seen[relPath] = true
	data, err := os.ReadFile(filepath.Join(workspaceDir, relPath))
	if err != nil {
		return err
	}
	f, err := build.ParseModule(relPath, data)
	if err != nil {
		return err
	}
	m.Files = append(m.Files, f)
	for _, call := range calls(f) {
		if (&build.Rule{Call: call}).Kind() != "include" {
			continue
		}
		label, ok := firstStringArg(call)
		if !ok {
			return fmt.Errorf("%s: include() must be given a string label", relPath)
		}
		includePath, err := labelToPath(label)
		if err != nil {
			return fmt.Errorf("%s: %w", relPath, err)
		}
		if err := m.load(workspaceDir, includePath, seen); err != nil {
			return err
		}
	}
	return nil
}

// labelToPath converts an include() label to a workspace-relative path.
// Bazel only accepts labels of the form "//pkg:file" in include().
func labelToPath(label string) (string, error) {
	pkg, name, ok := strings.Cut(strings.TrimPrefix(label, "//"), ":")
	if !strings.HasPrefix(label, "//") || !ok {
		return "", fmt.Errorf("bad include label %q: include() must be called with labels of the form //pkg:file", label)
	}
	return filepath.Join(pkg, name), nil
}

// BazelDep returns the version of the bazel_dep with the given module name, if
// there is one. The version is empty if the bazel_dep doesn't set one (e.g.
// because the module has an override).
func (m *Module) BazelDep(name string) (version string, ok bool) {
	for _, f := range m.Files {
		for _, call := range calls(f) {
			r := &build.Rule{Call: call}
			if r.Kind() == "bazel_dep" && r.AttrString("name") == name {
				return r.AttrString("version"), true
			}
		}
	}
	return "", false
}

// ParseBazelDep returns the module name and version of the first bazel_dep in
// a MODULE.bazel snippet.
func ParseBazelDep(snippet string) (name, version string, err error) {
	f, err := build.ParseModule("snippet", []byte(snippet))
	if err != nil {
		return "", "", err
	}
	for _, call := range calls(f) {
		r := &build.Rule{Call: call}
		if r.Kind() == "bazel_dep" {
			return r.AttrString("name"), r.AttrString("version"), nil
		}
	}
	return "", "", fmt.Errorf("no bazel_dep found in snippet:\n%s", snippet)
}

// calls returns the top-level function calls in f, including calls whose
// result is assigned (e.g. `go_deps = use_extension(...)`).
func calls(f *build.File) []*build.CallExpr {
	var out []*build.CallExpr
	for _, stmt := range f.Stmt {
		switch s := stmt.(type) {
		case *build.CallExpr:
			out = append(out, s)
		case *build.AssignExpr:
			if c, ok := s.RHS.(*build.CallExpr); ok {
				out = append(out, c)
			}
		}
	}
	return out
}

func firstStringArg(call *build.CallExpr) (string, bool) {
	if len(call.List) == 0 {
		return "", false
	}
	s, ok := call.List[0].(*build.StringExpr)
	if !ok {
		return "", false
	}
	return s.Value, true
}
