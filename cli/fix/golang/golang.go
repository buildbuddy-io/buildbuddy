package golang

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/bazelbuild/bazel-gazelle/label"
	"github.com/bazelbuild/bazel-gazelle/language"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
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

func (g *Golang) ConsolidateDepFiles(deps map[string][]string) map[string][]string {
	goModFiles, foundGoModFiles := deps[goModFileName]
	goWorkFiles, foundGoWorkFiles := deps[goWorkFileName]

	g.ensureModulePrefixesAreSet(goModFiles)

	if foundGoModFiles && len(goModFiles) == 1 {
		return deps
	}
	if foundGoModFiles && len(goModFiles) > 1 {
		goWorkContents := "go " + defaultGoVersion + "\n"
		for _, m := range goModFiles {
			goWorkContents += "use ./" + path.Dir(m) + "\n"
		}
		os.WriteFile(goWorkFileName, []byte(goWorkContents), 0777)
		delete(deps, goModFileName)
	}
	if foundGoWorkFiles && len(goWorkFiles) > 1 {
		log.Fatalf("Found multiple %s files, not sure what to do: %+v", goWorkFileName, goWorkFiles)
	}
	if foundGoWorkFiles {
		delete(deps, goModFileName)
	}
	return deps
}

func (g *Golang) ensureModulePrefixesAreSet(goModFiles []string) {
	for _, f := range goModFiles {
		fileContents, err := os.ReadFile(f)
		if err != nil {
			log.Warnf("error reading go.mod file %q: %s", f, err)
			continue
		}
		moduleName := modfile.ModulePath(fileContents)
		contents, filename := workspace.GetBuildFileContents(filepath.Dir(f))
		if !strings.Contains(contents, gazellePrefix) {
			appendToFile(filename, fmt.Sprintf("\n\n# %s %s\n", gazellePrefix, moduleName))
		}
	}
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
	return nil
}

const goDepsSnippet = `
go_deps = use_extension("@gazelle//:extensions.bzl", "go_deps")
go_deps.from_file(go_mod = "//:%s")

use_repo(
    go_deps,
%s)
`

func (g *Golang) RegisterDeps(path string, modulePath string) {
	moduleFileContents, err := os.ReadFile(modulePath)
	if err != nil {
		log.Warnf("error reading module file %q: %s", modulePath, err)
		return
	}
	goModContents, err := os.ReadFile(path)
	if err != nil {
		log.Warnf("error reading go.mod file %q: %s", path, err)
		return
	}

	// TODO(siggisim): merge with existing deps
	if !strings.Contains(string(moduleFileContents), "go_deps") {
		mod, err := modfile.Parse("go.mod", goModContents, nil)
		if err != nil {
			log.Warnf("error parsing go.mod file %q: %s", path, err)
			return
		}

		imports := ""
		for _, m := range mod.Require {
			if m.Indirect {
				continue
			}
			imports = imports + `    "` + label.ImportPathToBazelRepoName(m.Mod.Path) + "\",\n"
		}
		appendToFile(modulePath, fmt.Sprintf(goDepsSnippet, path, imports))
	}

}
