package annotations_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testModule = "github.com/example/Repo"

func testRepoContext(t *testing.T) (*annotations.RepoContext, string) {
	t.Helper()
	dir := t.TempDir()
	gomod := "module " + testModule + "\n\ngo 1.24\n"
	require.NoError(t, os.WriteFile(filepath.Join(dir, "go.mod"), []byte(gomod), 0644))
	return annotations.NewRepoContext(dir), dir
}

func extract(t *testing.T, rctx *annotations.RepoContext, rootDir, relPath, content string) *annotations.Result {
	t.Helper()
	ann, err := annotations.Extract("go", filepath.Join(rootDir, relPath), []byte(content), rctx)
	require.NoError(t, err)
	return ann
}

func TestGoModulePathFromGoMod(t *testing.T) {
	rctx, _ := testRepoContext(t)
	assert.Equal(t, testModule, rctx.GoModulePath())
}

func TestGoSingleImport(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extract(t, rctx, dir, "server/main.go", `package main

import "github.com/example/Repo/util/log"

func main() {}
`)
	require.NotNil(t, ann)
	assert.Equal(t, []string{"go:github.com/example/repo/util/log"}, ann.Imports)
	assert.Equal(t, []string{"go:github.com/example/repo/server"}, ann.ImportID)
}

func TestGoFactoredImportsWithAliasBlankAndDot(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extract(t, rctx, dir, "server/main.go", `package main

import (
	"fmt"
	xlog "github.com/example/Repo/util/log"
	_ "github.com/example/Repo/util/sideeffect"
	. "github.com/example/Repo/util/dot"
	"github.com/other/module/pkg"
)
`)
	require.NotNil(t, ann)
	assert.Equal(t, []string{
		"go:github.com/example/repo/util/log",
		"go:github.com/example/repo/util/sideeffect",
		"go:github.com/example/repo/util/dot",
	}, ann.Imports, "stdlib and external imports are dropped; aliased/blank/dot imports kept")
}

func TestGoImportsDeduped(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extract(t, rctx, dir, "a/a.go", `package a

import (
	l1 "github.com/example/Repo/util/log"
	l2 "github.com/example/Repo/util/log"
)
`)
	require.NotNil(t, ann)
	assert.Equal(t, []string{"go:github.com/example/repo/util/log"}, ann.Imports)
}

func TestGoSelfEdgeDropped(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extract(t, rctx, dir, "util/log/log_helpers.go", `package log

import "github.com/example/Repo/util/log"
`)
	require.NotNil(t, ann)
	assert.Empty(t, ann.Imports)
	assert.Equal(t, []string{"go:github.com/example/repo/util/log"}, ann.ImportID)
}

func TestGoTestFileGetsNoImportID(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extract(t, rctx, dir, "util/log/log_test.go", `package log_test

import (
	"testing"

	"github.com/example/Repo/util/log"
	"github.com/example/Repo/testutil"
)
`)
	require.NotNil(t, ann)
	assert.Equal(t, []string{
		// The test file's own package is a self-edge even for external
		// test packages.
		"go:github.com/example/repo/testutil",
	}, ann.Imports)
	assert.Empty(t, ann.ImportID, "test files should not receive import-rank boost")
}

func TestGoRepoRootPackage(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extract(t, rctx, dir, "main.go", `package main

import "github.com/example/Repo/sub"
`)
	require.NotNil(t, ann)
	assert.Equal(t, []string{"go:github.com/example/repo/sub"}, ann.Imports)
	assert.Equal(t, []string{"go:github.com/example/repo"}, ann.ImportID)
}

func TestGoParseErrorTolerated(t *testing.T) {
	rctx, dir := testRepoContext(t)
	// Tree-sitter is error-tolerant; garbage before/after the import should
	// not lose the parseable import declaration, and must never panic.
	ann := extract(t, rctx, dir, "a/a.go", `package a

import "github.com/example/Repo/util/log"

func { this is not valid go at all ((
`)
	require.NotNil(t, ann)
	assert.Equal(t, []string{"go:github.com/example/repo/util/log"}, ann.Imports)
}

func TestUnsupportedLanguageReturnsNil(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann, err := annotations.Extract("python", filepath.Join(dir, "a.py"), []byte("import os\n"), rctx)
	require.NoError(t, err)
	assert.Nil(t, ann)
}

func TestNoGoModDisablesGoImports(t *testing.T) {
	dir := t.TempDir()
	rctx := annotations.NewRepoContext(dir)
	ann := extract(t, rctx, dir, "a/a.go", `package a

import "github.com/example/Repo/util/log"

func DoThing() {}
`)
	// Symbols don't need module context; import identities do.
	require.NotNil(t, ann)
	assert.Empty(t, ann.Imports)
	assert.Empty(t, ann.ImportID)
	assert.Equal(t, []string{"dothing"}, ann.Symbols)
}

func TestFileOutsideRepoRootGetsNoImports(t *testing.T) {
	rctx, _ := testRepoContext(t)
	ann, err := annotations.Extract("go", filepath.Join(t.TempDir(), "b.go"), []byte("package b\n\nfunc Helper() {}\n"), rctx)
	require.NoError(t, err)
	require.NotNil(t, ann)
	assert.Empty(t, ann.Imports)
	assert.Empty(t, ann.ImportID)
	assert.Equal(t, []string{"helper"}, ann.Symbols)
}

func TestGoSymbols(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extract(t, rctx, dir, "greet/greet.go", `package greet

import "fmt"

// Greeter says hello. Comment words like Banana must not be indexed.
type Greeter struct{ Name string }

type Salutation = string

const DefaultName = "World"

var GreetCount int

func (g *Greeter) Greet(loudly bool) {
	count := GreetCount
	var local int
	fmt.Println(g.Name, count, local, "string literals are not symbols")
}

func NewGreeter() *Greeter { return &Greeter{} }
`)
	require.NotNil(t, ann)
	// Declarations only, source order, lowercased: no package name, no
	// receivers/params/locals (including var-declared locals), no usages,
	// no field names, nothing from comments or strings.
	assert.Equal(t, []string{
		"greeter",     // type Greeter
		"salutation",  // type alias
		"defaultname", // package-level const
		"greetcount",  // package-level var
		"greet",       // method
		"newgreeter",  // function
	}, ann.Symbols)
}

func TestJavaSymbolsAndImports(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann, err := annotations.Extract("java", filepath.Join(dir, "src/main/java/com/example/build/RuleHelper.java"), []byte(`
package com.example.build;

import com.example.build.util.Label;
import com.example.build.util.*;
import static com.example.other.Constants.MAX_DEPTH;
import java.util.List;

// Comment words like Banana must not be indexed.
public class RuleHelper {
	public static final int DEFAULT_DEPTH = 3;
	private Label label;

	public RuleHelper(Label label) {
		this.label = label;
	}

	public List<String> expandRule(String name) {
		int localVar = MAX_DEPTH;
		return List.of(name, "string literals are not symbols");
	}

	interface Expander {
		void expand();
	}

	enum Mode { STRICT, LENIENT }
}
`), rctx)
	require.NoError(t, err)
	require.NotNil(t, ann)
	assert.Equal(t, []string{
		"rulehelper",        // class
		"default_depth",     // field (constant)
		"label",             // field
		"rulehelper",        // constructor
		"expandrule",        // method
		"expander",          // nested interface
		"expand",            // interface method
		"mode",              // nested enum
		"strict", "lenient", // enum constants
	}, ann.Symbols)
	// Class import, wildcard import, and static import all resolve to their
	// packages; java.util is external but indexed (it never matches any
	// import_id, so it adds no in-degree); self-package edges are dropped.
	assert.Equal(t, []string{
		"java:com.example.build.util",
		"java:com.example.other",
		"java:java.util",
	}, ann.Imports)
	assert.Equal(t, []string{"java:com.example.build"}, ann.ImportID)
}

func TestJavaTestFileGetsNoImportID(t *testing.T) {
	rctx, dir := testRepoContext(t)
	for _, name := range []string{
		"src/test/java/com/example/RuleHelperTest.java",
		"javatests/com/example/Helpers.java",
	} {
		ann, err := annotations.Extract("java", filepath.Join(dir, name), []byte(`
package com.example;
public class X {}
`), rctx)
		require.NoError(t, err, name)
		require.NotNil(t, ann, name)
		assert.Empty(t, ann.ImportID, name)
		assert.Equal(t, []string{"x"}, ann.Symbols, name)
	}
}
