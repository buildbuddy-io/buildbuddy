package annotations_test

import (
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func extractRust(t *testing.T, dir, relPath, content string) *annotations.Result {
	t.Helper()
	ann, err := annotations.Extract(t.Context(), "rust",
		filepath.Join(dir, relPath), []byte(content),
		annotations.NewRepoContext(dir, ""))
	require.NoError(t, err)
	require.NotNil(t, ann)
	return ann
}

func TestRustSymbols(t *testing.T) {
	dir := t.TempDir()
	ann := extractRust(t, dir, "src/foo/bar.rs", `
// Comment words like Banana must not be indexed.
use crate::other::Thing;

const MAX_DEPTH: u32 = 3;
static GREETING: &str = "hello, string literals are not symbols";

pub struct Greeter {
    name: String,
}

pub enum Mode {
    Strict,
    Lenient,
}

pub trait Greet {
    fn greet(&self);
}

pub mod helpers {
    pub fn helper() {}
}

type Salutation = String;

pub fn new_greeter(local: u32) -> Greeter {
    let unused = local;
    Greeter { name: "x".to_string() }
}
`)
	assert.Equal(t, []string{
		"max_depth", // const
		"greeting",  // static
		"greeter",   // struct
		"mode",      // enum
		"greet",     // trait (the trait's fn signature is a
		// function_signature_item, not function_item, so it is not captured)
		"helpers",     // mod
		"helper",      // fn in mod
		"salutation",  // type alias
		"new_greeter", // fn
	}, ann.Symbols)
}

func TestRustModuleImportID(t *testing.T) {
	dir := t.TempDir()
	ann := extractRust(t, dir, "src/foo/bar.rs", `pub fn f() {}`)
	assert.Equal(t, []string{"rust:foo::bar"}, ann.ImportID)
}

func TestRustModDotRsImportID(t *testing.T) {
	dir := t.TempDir()
	ann := extractRust(t, dir, "src/foo/mod.rs", `pub fn f() {}`)
	assert.Equal(t, []string{"rust:foo"}, ann.ImportID)
}

func TestRustLibRsHasNoImportID(t *testing.T) {
	dir := t.TempDir()
	ann := extractRust(t, dir, "src/lib.rs", `pub fn f() {}`)
	assert.Empty(t, ann.ImportID)
	assert.Equal(t, []string{"f"}, ann.Symbols)
}

func TestRustMainRsHasNoImportID(t *testing.T) {
	dir := t.TempDir()
	ann := extractRust(t, dir, "src/main.rs", `fn main() {}`)
	assert.Empty(t, ann.ImportID)
}

func TestRustCrateImport(t *testing.T) {
	dir := t.TempDir()
	ann := extractRust(t, dir, "src/foo/bar.rs", `
use crate::a::b;
use serde::Deserialize;

pub fn f() {}
`)
	assert.Equal(t, []string{"rust:a::b", "rust:serde::deserialize"}, ann.Imports)
	assert.Equal(t, []string{"rust:foo::bar"}, ann.ImportID)
}

func TestRustGroupedImport(t *testing.T) {
	dir := t.TempDir()
	ann := extractRust(t, dir, "src/foo/bar.rs", `
use crate::a::{b, c};

pub fn f() {}
`)
	assert.Equal(t, []string{"rust:a", "rust:a::b", "rust:a::c"}, ann.Imports)
}

func TestRustGlobImport(t *testing.T) {
	dir := t.TempDir()
	ann := extractRust(t, dir, "src/foo/bar.rs", `
use crate::a::*;

pub fn f() {}
`)
	assert.Equal(t, []string{"rust:a"}, ann.Imports)
}

func TestRustSelfEdgeDropped(t *testing.T) {
	dir := t.TempDir()
	// A file at src/foo/bar.rs (module foo::bar) that somehow names its own
	// module path must not produce a self-edge.
	ann := extractRust(t, dir, "src/foo/bar.rs", `
use crate::foo::bar;
use crate::a::b;

pub fn f() {}
`)
	assert.Equal(t, []string{"rust:a::b"}, ann.Imports)
	assert.Equal(t, []string{"rust:foo::bar"}, ann.ImportID)
}

func TestRustTestsDirGetsNoImportID(t *testing.T) {
	dir := t.TempDir()
	ann := extractRust(t, dir, "tests/integration.rs", `
use crate::a::b;

fn it_works() {}
`)
	assert.Empty(t, ann.ImportID)
	assert.Equal(t, []string{"it_works"}, ann.Symbols)
}

func TestRustNonTestFilenameKeepsImportID(t *testing.T) {
	dir := t.TempDir()
	// "latest_handler" contains "test_" but is not a test file; it must keep
	// its identity so it stays in the reverse-import graph.
	ann := extractRust(t, dir, "src/latest_handler.rs", `pub fn f() {}`)
	assert.Equal(t, []string{"rust:latest_handler"}, ann.ImportID)
}

func TestRustNestedGroupedImport(t *testing.T) {
	dir := t.TempDir()
	// A nested group must expand to the real edges, not junk like
	// `rust:a::c::{d`.
	ann := extractRust(t, dir, "src/foo/bar.rs", `
use crate::a::{b, c::{d, e}};

pub fn f() {}
`)
	assert.ElementsMatch(t,
		[]string{"rust:a", "rust:a::b", "rust:a::c", "rust:a::c::d", "rust:a::c::e"},
		ann.Imports)
}

func TestRustSuperChainImport(t *testing.T) {
	dir := t.TempDir()
	// In module a::b::c, `super::super::x` climbs two parents to a::x.
	ann := extractRust(t, dir, "src/a/b/c.rs", `
use super::super::widget;

pub fn f() {}
`)
	assert.Equal(t, []string{"rust:a::widget"}, ann.Imports)
}
