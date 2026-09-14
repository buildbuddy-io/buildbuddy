package golang

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/bazel-contrib/bazel-gazelle/v2/label"
	"github.com/bazelbuild/bazel-gazelle/language"
	"golang.org/x/mod/modfile"

	gazelleGolang "github.com/bazelbuild/bazel-gazelle/language/go"
)

const (
	goFileExtensions = ".go"
	goModFileName    = "go.mod"
	goWorkFileName   = "go.work"
	gazellePrefix    = "gazelle:prefix"

	// TODO(siggisim): Make these configurable or infer them from the repo
	defaultGoVersion         = "1.20"
	defaultRulesGoVersion    = "0.36.0"
	defaultGazelleVersion    = "0.26.0"
	defaultRulesProtoVersion = "5.3.0-21.7"
)

type Golang struct {
	language.Language
	language.RepoImporter
	language.ModuleAwareLanguage
}

func NewLanguage() language.Language {
	l := gazelleGolang.NewLanguage()
	ri := l.(language.RepoImporter)
	mal := l.(language.ModuleAwareLanguage)
	return &Golang{
		Language:            l,
		RepoImporter:        ri,
		ModuleAwareLanguage: mal,
	}
}

func (g *Golang) Deps() []string {
	return []string{
		"github/bazelbuild/rules_go@" + defaultRulesGoVersion,
		"github/bazelbuild/bazel-gazelle@" + defaultGazelleVersion,
		"~github/bazelbuild/rules_proto@" + defaultRulesProtoVersion, // Transitive
	}
}

func (g *Golang) IsSourceFile(path string) bool {
	return strings.HasSuffix(path, goFileExtensions)
}

func (g *Golang) IsDepFile(path string) bool {
	return strings.HasSuffix(path, goModFileName)
}

func (g *Golang) ConsolidateDepFiles(deps map[string][]string) (map[string][]string, error) {
	goModFiles, foundGoModFiles := deps[goModFileName]
	goWorkFiles, foundGoWorkFiles := deps[goWorkFileName]

	if len(goWorkFiles) > 1 {
		return nil, fmt.Errorf("found multiple %s files, not sure what to do: %+v", goWorkFileName, goWorkFiles)
	}
	if err := g.ensureModulePrefixesAreSet(goModFiles); err != nil {
		return nil, err
	}

	if foundGoModFiles && len(goModFiles) == 1 {
		return deps, nil
	}
	if foundGoModFiles && len(goModFiles) > 1 {
		var goWorkContents strings.Builder
		goWorkContents.WriteString("go " + defaultGoVersion + "\n")
		for _, m := range goModFiles {
			goWorkContents.WriteString("use ./" + path.Dir(m) + "\n")
		}
		if err := os.WriteFile(goWorkFileName, []byte(goWorkContents.String()), 0777); err != nil {
			return nil, fmt.Errorf("write %q: %w", goWorkFileName, err)
		}
		delete(deps, goModFileName)
	}
	if foundGoWorkFiles {
		delete(deps, goModFileName)
	}
	return deps, nil
}

func (g *Golang) ensureModulePrefixesAreSet(goModFiles []string) error {
	for _, f := range goModFiles {
		fileContents, err := os.ReadFile(f)
		if err != nil {
			return fmt.Errorf("read go.mod file %q: %w", f, err)
		}
		mod, err := modfile.Parse(f, fileContents, nil)
		if err != nil {
			return fmt.Errorf("parse go.mod file %q: %w", f, err)
		}
		if mod.Module == nil {
			return fmt.Errorf("go.mod file %q has no module directive", f)
		}
		contents, filename, err := getBuildFileContents(filepath.Dir(f))
		if err != nil {
			return err
		}
		if !strings.Contains(contents, gazellePrefix) {
			if err := appendToFile(filename, fmt.Sprintf("\n\n# %s %s\n", gazellePrefix, mod.Module.Mod.Path)); err != nil {
				return fmt.Errorf("write module prefix to %q: %w", filename, err)
			}
		}
	}
	return nil
}

// Only missing BUILD files may be skipped. Other read failures must not cause us
// to silently create or append to a different BUILD file.
func getBuildFileContents(dir string) (string, string, error) {
	for _, basename := range []string{"BUILD", "BUILD.bazel"} {
		filename := filepath.Join(dir, basename)
		contents, err := os.ReadFile(filename)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return "", "", fmt.Errorf("read BUILD file %q: %w", filename, err)
		}
		return string(contents), filename, nil
	}
	return "", filepath.Join(dir, "BUILD.bazel"), nil
}

func appendToFile(fileName, contents string) error {
	f, err := os.OpenFile(fileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.WriteString(contents); err != nil {
		return err
	}
	return f.Close()
}

const goDepsSnippet = `
go_deps = use_extension("@gazelle//:extensions.bzl", "go_deps")
go_deps.from_file(go_mod = "//:%s")

use_repo(
    go_deps,
%s)
`

func (g *Golang) RegisterDeps(path string, modulePath string) error {
	moduleFileContents, err := os.ReadFile(modulePath)
	if err != nil {
		return fmt.Errorf("read module file %q: %w", modulePath, err)
	}
	goModContents, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read go.mod file %q: %w", path, err)
	}

	// TODO(siggisim): merge with existing deps
	if !strings.Contains(string(moduleFileContents), "go_deps") {
		mod, err := modfile.Parse(path, goModContents, nil)
		if err != nil {
			return fmt.Errorf("parse go.mod file %q: %w", path, err)
		}

		imports := ""
		for _, m := range mod.Require {
			if m.Indirect {
				continue
			}
			imports = imports + `    "` + label.ImportPathToBazelRepoName(m.Mod.Path) + "\",\n"
		}
		if err := appendToFile(modulePath, fmt.Sprintf(goDepsSnippet, path, imports)); err != nil {
			return fmt.Errorf("register Go dependencies in %q: %w", modulePath, err)
		}
	}
	return nil
}
