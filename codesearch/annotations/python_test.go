package annotations_test

import (
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func extractPy(t *testing.T, dir, relPath, content string) *annotations.Result {
	t.Helper()
	rctx := annotations.NewRepoContext(dir, "")
	ann, err := annotations.Extract(t.Context(), "python", filepath.Join(dir, relPath), []byte(content), rctx)
	require.NoError(t, err)
	require.NotNil(t, ann)
	return ann
}

func TestPythonSymbols(t *testing.T) {
	dir := t.TempDir()
	ann := extractPy(t, dir, "pkg/mod.py", `MAX_SIZE = 10


def helper():
    pass


class Greeter:
    def greet(self):
        return "hi"
`)
	assert.Equal(t, []string{"max_size", "helper", "greeter", "greet"}, ann.Symbols)
}

func TestPythonModuleIdentity(t *testing.T) {
	dir := t.TempDir()
	ann := extractPy(t, dir, "app/db/models.py", "x = 1\n")
	assert.Equal(t, []string{"py:app.db.models"}, ann.ImportID)
}

func TestPythonInitPackageIdentity(t *testing.T) {
	dir := t.TempDir()
	ann := extractPy(t, dir, "app/db/__init__.py", "x = 1\n")
	assert.Equal(t, []string{"py:app.db"}, ann.ImportID)
}

func TestPythonImports(t *testing.T) {
	dir := t.TempDir()
	ann := extractPy(t, dir, "app/main.py", `import os
import app.util.log
from app.db import models
from app.db.models import User
`)
	// `from a.b import c` records both the package and the submodule candidate.
	assert.Contains(t, ann.Imports, "py:app.util.log")
	assert.Contains(t, ann.Imports, "py:app.db")
	assert.Contains(t, ann.Imports, "py:app.db.models")
	assert.Contains(t, ann.Imports, "py:app.db.models.user")
	// External imports are recorded too; they just never match an import_id.
	assert.Contains(t, ann.Imports, "py:os")
	assert.Equal(t, []string{"py:app.main"}, ann.ImportID)
}

func TestPythonRelativeImports(t *testing.T) {
	dir := t.TempDir()
	ann := extractPy(t, dir, "pkg/sub/mod.py", `from . import helper
from ..other import thing
from .local import x
`)
	assert.Contains(t, ann.Imports, "py:pkg.sub") // from .
	assert.Contains(t, ann.Imports, "py:pkg.sub.helper")
	assert.Contains(t, ann.Imports, "py:pkg.other") // from ..other
	assert.Contains(t, ann.Imports, "py:pkg.other.thing")
	assert.Contains(t, ann.Imports, "py:pkg.sub.local") // from .local
	assert.Contains(t, ann.Imports, "py:pkg.sub.local.x")
}

func TestPythonSelfEdgeDropped(t *testing.T) {
	dir := t.TempDir()
	ann := extractPy(t, dir, "pkg/mod.py", "import pkg.mod\n")
	assert.Empty(t, ann.Imports)
	assert.Equal(t, []string{"py:pkg.mod"}, ann.ImportID)
}

func TestPythonTestFileNoImportID(t *testing.T) {
	dir := t.TempDir()
	ann := extractPy(t, dir, "tests/test_app.py", "def test_it():\n    pass\n")
	assert.Empty(t, ann.ImportID, "test files should not receive import-rank identity")
	assert.Equal(t, []string{"test_it"}, ann.Symbols)
}
