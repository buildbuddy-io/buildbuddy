//go:build pure
// +build pure

package typescript

import (
	"flag"

	"github.com/bazelbuild/bazel-gazelle/config"
	"github.com/bazelbuild/bazel-gazelle/label"
	"github.com/bazelbuild/bazel-gazelle/language"
	"github.com/bazelbuild/bazel-gazelle/repo"
	"github.com/bazelbuild/bazel-gazelle/resolve"
	"github.com/bazelbuild/bazel-gazelle/rule"
)

// NewLanguage returns a stub implementation for pure builds (without CGO).
// The typescript language support requires CGO and is not available in pure builds.
func NewLanguage() language.Language {
	return &stubLanguage{}
}

type stubLanguage struct{}

func (s *stubLanguage) Name() string {
	return "ts"
}

func (s *stubLanguage) Kinds() map[string]rule.KindInfo {
	return map[string]rule.KindInfo{}
}

func (s *stubLanguage) Loads() []rule.LoadInfo {
	return []rule.LoadInfo{}
}

func (s *stubLanguage) GenerateRules(args language.GenerateArgs) language.GenerateResult {
	return language.GenerateResult{}
}

func (s *stubLanguage) Fix(c *config.Config, f *rule.File) {}

func (s *stubLanguage) Imports(c *config.Config, r *rule.Rule, f *rule.File) []resolve.ImportSpec {
	return []resolve.ImportSpec{}
}

func (s *stubLanguage) Embeds(r *rule.Rule, from label.Label) []label.Label {
	return []label.Label{}
}

func (s *stubLanguage) Resolve(c *config.Config, ix *resolve.RuleIndex, rc *repo.RemoteCache, r *rule.Rule, imports interface{}, from label.Label) {}

func (s *stubLanguage) RegisterFlags(fs *flag.FlagSet, cmd string, c *config.Config) {}

func (s *stubLanguage) CheckFlags(fs *flag.FlagSet, c *config.Config) error {
	return nil
}

func (s *stubLanguage) KnownDirectives() []string {
	return []string{}
}

func (s *stubLanguage) Configure(c *config.Config, rel string, f *rule.File) {}

