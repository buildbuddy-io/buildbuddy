package golang

import (
	"os"
	"path"
	"strings"

	"github.com/bazelbuild/bazel-gazelle/language"
	"github.com/buildbuddy-io/buildbuddy/cli/log"

	gazelleGolang "github.com/bazelbuild/bazel-gazelle/language/go"
)

const (
	goFileExtensions = ".go"
	goModFileName    = "go.mod"
	goWorkFileName   = "go.work"

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
		"github/bazelbuild/rules_proto@" + defaultRulesProtoVersion,
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
