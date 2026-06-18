package annotations_test

import (
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func extractLang(t *testing.T, rctx *annotations.RepoContext, lang, rootDir, relPath, content string) *annotations.Result {
	t.Helper()
	ann, err := annotations.Extract(t.Context(), lang, filepath.Join(rootDir, relPath), []byte(content), rctx)
	require.NoError(t, err)
	require.NotNil(t, ann)
	return ann
}

func TestCSymbols(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extractLang(t, rctx, "c", dir, "src/foo.c", `
#include "foo.h"

typedef int MyInt;

struct Point { int x; int y; };

enum Color { RED, GREEN };

union Value { int i; float f; };

int add(int a, int b) {
	int local = a + b; // local, not a symbol
	return local;
}
`)
	assert.Equal(t, []string{
		"myint", // typedef
		"point", // struct
		"color", // enum
		"value", // union
		"add",   // function
	}, ann.Symbols)
}

func TestCppSymbols(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extractLang(t, rctx, "c++", dir, "src/widget.cc", `
namespace ui {

class Widget {
public:
	void Draw();
	int Size() { return 0; }
};

} // namespace ui

void ui::Widget::Draw() {
	// out-of-line method definition
}

void freeFunc() {}
`)
	assert.Contains(t, ann.Symbols, "ui")       // namespace
	assert.Contains(t, ann.Symbols, "widget")   // class
	assert.Contains(t, ann.Symbols, "size")     // inline method
	assert.Contains(t, ann.Symbols, "draw")     // qualified out-of-line method
	assert.Contains(t, ann.Symbols, "freefunc") // free function
}

func TestCppImportIDHasPathAndBasename(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extractLang(t, rctx, "c++", dir, "src/foo/bar.h", `
struct Bar {};
`)
	assert.Equal(t, []string{"c:src/foo/bar.h", "c:bar.h"}, ann.ImportID)
}

func TestCQuotedIncludeRecordsPathAndBasename(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extractLang(t, rctx, "c", dir, "src/main.c", `
#include "a/b.h"

int main() { return 0; }
`)
	assert.Contains(t, ann.Imports, "c:a/b.h")
	assert.Contains(t, ann.Imports, "c:b.h")
}

func TestCSystemIncludeRecorded(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extractLang(t, rctx, "c", dir, "src/main.c", `
#include <stdio.h>

int main() { return 0; }
`)
	// <stdio.h> has no directory part, so spec and basename coincide.
	assert.Contains(t, ann.Imports, "c:stdio.h")
}

func TestCTestFileGetsNoImportID(t *testing.T) {
	rctx, dir := testRepoContext(t)
	for _, relPath := range []string{
		"test/foo.h",
		"src/foo_test.c",
	} {
		ann := extractLang(t, rctx, "c", dir, relPath, `struct X {};`)
		assert.Empty(t, ann.ImportID, relPath)
	}
}

func TestCNonTestFilenameKeepsImportID(t *testing.T) {
	rctx, dir := testRepoContext(t)
	// "latest_handler" contains "test_" and "testbed" contains "test", but
	// neither is a test file (the heuristic is anchored on the stem), so they
	// must keep their identity in the reverse-import graph.
	for _, relPath := range []string{"src/latest_handler.c", "src/testbed.h"} {
		ann := extractLang(t, rctx, "c", dir, relPath, `struct X {};`)
		assert.NotEmpty(t, ann.ImportID, relPath)
	}
}

func TestCSelfEdgeDropped(t *testing.T) {
	rctx, dir := testRepoContext(t)
	// A header at src/foo/bar.h whose identity terms are c:src/foo/bar.h and
	// c:bar.h. An include of "bar.h" is a self-edge by basename and must be
	// dropped; an include of "src/foo/bar.h" is a self-edge by path.
	ann := extractLang(t, rctx, "c", dir, "src/foo/bar.h", `
#include "bar.h"
#include "src/foo/bar.h"
#include "other.h"
`)
	assert.NotContains(t, ann.Imports, "c:bar.h", "self-edge by basename dropped")
	assert.NotContains(t, ann.Imports, "c:src/foo/bar.h", "self-edge by path dropped")
	assert.Contains(t, ann.Imports, "c:other.h")
}
