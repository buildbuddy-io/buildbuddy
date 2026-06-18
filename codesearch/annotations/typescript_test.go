package annotations_test

import (
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/codesearch/annotations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func extractTS(t *testing.T, rctx *annotations.RepoContext, rootDir, lang, relPath, content string) *annotations.Result {
	t.Helper()
	ann, err := annotations.Extract(t.Context(), lang, filepath.Join(rootDir, relPath), []byte(content), rctx)
	require.NoError(t, err)
	require.NotNil(t, ann)
	return ann
}

func TestTSSymbols(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extractTS(t, rctx, dir, "typescript", "src/app.ts", `
// Comment words like Banana must not be indexed.
export function greet(name: string): string {
	const local = "string literals are not symbols";
	return local + name;
}

export class Greeter {
	greet(): void {}
}

interface Speaker {
	speak(): void;
}

type Salutation = string;

enum Mode { Strict, Lenient }

export const DefaultName = "World";
`)
	// Declarations only, source order, lowercased. Interface/type/enum are
	// TypeScript-only and captured by the richer TS query.
	assert.Equal(t, []string{
		"greet",   // function
		"local",   // const declarator inside the function
		"greeter", // class
		"greet",   // method (class body; interface method signatures are
		// a distinct node type and not captured)
		"speaker",     // interface (TS only)
		"salutation",  // type alias (TS only)
		"mode",        // enum (TS only)
		"defaultname", // const declarator
	}, ann.Symbols)
}

func TestJSSymbols(t *testing.T) {
	rctx, dir := testRepoContext(t)
	// The JS grammar has no interface/type/enum nodes; the JS-compatible query
	// captures functions, classes, methods, and variable declarators.
	ann := extractTS(t, rctx, dir, "javascript", "src/app.js", `
export function greet(name) {
	return "hi " + name;
}

export class Greeter {
	greet() {}
}

export const DefaultName = "World";
`)
	assert.Equal(t, []string{
		"greet",
		"greeter",
		"greet",
		"defaultname",
	}, ann.Symbols)
}

func TestTSSelfImportIDFromPath(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extractTS(t, rctx, dir, "typescript", "src/utils/log.ts", `export const x = 1;`)
	assert.Equal(t, []string{"ts:src/utils/log"}, ann.ImportID)
}

func TestTSIndexCollapsesToDirectory(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extractTS(t, rctx, dir, "typescript", "src/utils/index.ts", `export const x = 1;`)
	assert.Equal(t, []string{"ts:src/utils"}, ann.ImportID)
}

func TestTSRelativeImportResolved(t *testing.T) {
	rctx, dir := testRepoContext(t)
	// src/app.ts importing "./util/log" resolves to src/util/log. Both the
	// plain-file and the /index-collapsed candidate are recorded so either
	// on-disk layout matches.
	ann := extractTS(t, rctx, dir, "typescript", "src/app.ts", `
import { log } from "./util/log";
import foo from "../shared/foo";
`)
	assert.Contains(t, ann.Imports, "ts:src/util/log")
	assert.Contains(t, ann.Imports, "ts:src/util/log/index")
	// "../shared/foo" resolves against src/ -> shared/foo.
	assert.Contains(t, ann.Imports, "ts:shared/foo")
}

func TestTSBareImportRecordedHarmless(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extractTS(t, rctx, dir, "typescript", "src/app.ts", `
import React from "react";
import { join } from "node:path";
import x from "@scope/pkg";
`)
	// Bare specs are external: recorded verbatim under ts:, never matching any
	// in-repo ImportID.
	assert.Contains(t, ann.Imports, "ts:react")
	assert.Contains(t, ann.Imports, "ts:node:path")
	assert.Contains(t, ann.Imports, "ts:@scope/pkg")
}

func TestTSDynamicImportAndRequire(t *testing.T) {
	rctx, dir := testRepoContext(t)
	ann := extractTS(t, rctx, dir, "typescript", "src/app.ts", `
const a = require("./util/log");
async function f() {
	const b = await import("./util/log");
}
`)
	assert.Contains(t, ann.Imports, "ts:src/util/log")
}

func TestTSTestFileGetsNoImportID(t *testing.T) {
	rctx, dir := testRepoContext(t)
	for _, name := range []string{
		"src/app.test.ts",
		"src/app.spec.ts",
		"src/__tests__/app.ts",
	} {
		ann := extractTS(t, rctx, dir, "typescript", name, `export const x = 1;`)
		assert.Empty(t, ann.ImportID, name)
	}
}

func TestTSSelfEdgeDropped(t *testing.T) {
	rctx, dir := testRepoContext(t)
	// src/util/log.ts importing "./log" resolves to src/util/log, which is the
	// file's own identity: the self-edge is dropped.
	ann := extractTS(t, rctx, dir, "typescript", "src/util/log.ts", `
import { helper } from "./log";
`)
	assert.NotContains(t, ann.Imports, "ts:src/util/log")
	assert.Equal(t, []string{"ts:src/util/log"}, ann.ImportID)
}
