package typescript

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/bazel-gazelle/config"
	"github.com/bazelbuild/bazel-gazelle/label"
	"github.com/bazelbuild/bazel-gazelle/language"
	"github.com/bazelbuild/bazel-gazelle/rule"
	"github.com/stretchr/testify/require"
)

// Covers the mixed TSX import forms used by the parser, including type-only,
// namespace, dynamic, and tslib-triggering syntax.
func TestGenerateRules_ReadsTSXImports(t *testing.T) {
	repoRoot := t.TempDir()
	src := `import React from "react";
import type { Foo } from "./types";
import { Button } from "./button";
import * as Icons from "@heroicons/react/24/solid";
import "./side_effect";

export async function Widget() {
  const chunk = await import("./lazy");
  return <Button icon={Icons.AcademicCapIcon} />;
}
`
	require.NoError(t, os.WriteFile(filepath.Join(repoRoot, "widget.tsx"), []byte(src), 0o644))

	ts := NewLanguage().(*TS)
	cfg := newTSConfig(repoRoot)
	result := ts.GenerateRules(language.GenerateArgs{
		Config:       cfg,
		Dir:          repoRoot,
		RegularFiles: []string{"widget.tsx"},
	})

	require.Len(t, result.Gen, 1)
	require.Equal(t, "widget", result.Gen[0].Name())
	require.Equal(t, []string{"widget.tsx"}, result.Gen[0].AttrStrings(srcAttribute))
	require.Equal(t, [][]string{{
		"react",
		"./types",
		"./button",
		"@heroicons/react/24/solid",
		"./lazy",
		tslibImport,
	}}, normalizeImports(result.Imports))
}

// Verifies that rule generation ignores non-TypeScript files in the directory.
func TestGenerateRules_OnlyProcessesTSAndTSXFiles(t *testing.T) {
	repoRoot := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(repoRoot, "widget.ts"), []byte(`import {x} from "./x"`), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repoRoot, "widget.js"), []byte(`import {x} from "./x"`), 0o644))

	ts := NewLanguage().(*TS)
	result := ts.GenerateRules(language.GenerateArgs{
		Config:       newTSConfig(repoRoot),
		Dir:          repoRoot,
		RegularFiles: []string{"widget.ts", "widget.js"},
	})

	require.Len(t, result.Gen, 1)
	require.Equal(t, "widget", result.Gen[0].Name())
	require.Equal(t, [][]string{{"./x"}}, normalizeImports(result.Imports))
}

// Verifies that the ts extension directive can disable rule generation entirely.
func TestGenerateRules_DisabledModeSkipsGeneration(t *testing.T) {
	repoRoot := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(repoRoot, "widget.tsx"), []byte(`import {x} from "./x"`), 0o644))

	cfg := newTSConfig(repoRoot)
	cfg.Exts[languageName] = tsConfig{Mode: disableMode}

	ts := NewLanguage().(*TS)
	result := ts.GenerateRules(language.GenerateArgs{
		Config:       cfg,
		Dir:          repoRoot,
		RegularFiles: []string{"widget.tsx"},
	})

	require.Empty(t, result.Gen)
	require.Empty(t, result.Imports)
}

// Mirrors the repo's React.lazy(() => import(...)) pattern to pin dynamic import extraction.
func TestGenerateRules_ReadsReactLazyDynamicImports(t *testing.T) {
	repoRoot := t.TempDir()
	src := `import React from "react";

const SettingsThemeTransition = React.lazy(() => import("../settings/settings_theme_transition"));
const CodeComponent = React.lazy(() => import("../code/code"));
const CodeComponentV2 = React.lazy(() => import("../code/code_v2"));
const CodeReviewComponent = React.lazy(() => import("../review/review"));
`
	require.NoError(t, os.WriteFile(filepath.Join(repoRoot, "root.tsx"), []byte(src), 0o644))

	ts := NewLanguage().(*TS)
	result := ts.GenerateRules(language.GenerateArgs{
		Config:       newTSConfig(repoRoot),
		Dir:          repoRoot,
		RegularFiles: []string{"root.tsx"},
	})

	require.Len(t, result.Gen, 1)
	require.Equal(t, [][]string{{
		"react",
		"../settings/settings_theme_transition",
		"../code/code",
		"../code/code_v2",
		"../review/review",
	}}, normalizeImports(result.Imports))
}

// Covers dependency resolution for relative imports, npm imports, and inferred @types packages.
func TestResolve_MapsRelativeAndNPMImports(t *testing.T) {
	cfg := newTSConfig("/repo")
	cfg.Exts[languageName] = tsConfig{
		PackageJSON: struct {
			Dependencies    map[string]string `json:"dependencies"`
			DevDependencies map[string]string `json:"devDependencies"`
		}{
			DevDependencies: map[string]string{
				"@types/react": "18.0.0",
			},
		},
	}

	r := rule.NewRule(tsProjectRuleName, "widget")
	NewLanguage().(*TS).Resolve(
		cfg,
		nil,
		nil,
		r,
		[]string{"./button", "react", "@heroicons/react/24/solid"},
		label.Label{Pkg: "app/components"},
	)

	require.Equal(t, []string{
		"//:node_modules/@heroicons/react",
		"//:node_modules/@types/react",
		"//:node_modules/react",
		"//app/components:button",
	}, r.AttrStrings(depsAttribute))
}

// Verifies that disabled mode leaves generated rules without deps.
func TestResolve_DisabledModeSkipsDeps(t *testing.T) {
	cfg := newTSConfig("/repo")
	cfg.Exts[languageName] = tsConfig{Mode: disableMode}
	r := rule.NewRule(tsProjectRuleName, "widget")

	NewLanguage().(*TS).Resolve(
		cfg,
		nil,
		nil,
		r,
		[]string{"react"},
		label.Label{Pkg: "app/components"},
	)

	require.Nil(t, r.Attr(depsAttribute))
}

// Verifies that package.json and ts directives are both folded into Gazelle config.
func TestConfigure_LoadsPackageJSONAndDirective(t *testing.T) {
	repoRoot := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(repoRoot, packageFileName), []byte(`{
  "dependencies": {"react": "18.2.0"},
  "devDependencies": {"@types/react": "18.2.0"}
}`), 0o644))

	cfg := config.New()
	cfg.RepoRoot = repoRoot

	f := rule.EmptyFile(filepath.Join(repoRoot, "BUILD"), "")
	f.Directives = []rule.Directive{{Key: languageName, Value: disableMode}}

	NewLanguage().(*TS).Configure(cfg, "", f)

	got := cfg.Exts[languageName].(tsConfig)
	require.Equal(t, disableMode, got.Mode)
	require.Equal(t, "18.2.0", got.PackageJSON.Dependencies["react"])
	require.Equal(t, "18.2.0", got.PackageJSON.DevDependencies["@types/react"])
}

func newTSConfig(repoRoot string) *config.Config {
	cfg := config.New()
	cfg.RepoRoot = repoRoot
	cfg.Exts = map[string]interface{}{
		languageName: tsConfig{},
	}
	return cfg
}

func normalizeImports(imports []any) [][]string {
	out := make([][]string, 0, len(imports))
	for _, imp := range imports {
		out = append(out, imp.([]string))
	}
	return out
}
