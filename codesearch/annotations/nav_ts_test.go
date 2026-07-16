package annotations

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fileByName is a DefLookup/RefLookup over in-memory TS files keyed by their
// import_id term. FindDefs/FindReferencingFiles return the same candidate set;
// resolve/decorate filter by symbol themselves.
type tsFiles struct {
	byID  map[string][]DefFile // importID -> files declaring (used for FindDefs)
	byMod map[string][]RefFile // moduleID -> files importing/owning it
}

func (f tsFiles) FindDefs(_ context.Context, importID, _ string) ([]DefFile, error) {
	return f.byID[importID], nil
}
func (f tsFiles) FindReferencingFiles(_ context.Context, importID string) ([]RefFile, error) {
	return f.byMod[importID], nil
}

// log.ts declares Print and a helper; app.ts imports Print (aliased) and a
// namespace, and uses a same-module declaration.
const tsLog = `// Print writes a greeting.
export function Print(): void {}

export const VERSION = "1";
`

const tsApp = `import { Print as P } from "./util/log";
import * as log from "./util/log";

function greet(): void {
  P();
  log.Print();
  helper();
}

function helper(): void {}
`

func tsAppRefFile() RefFile {
	return RefFile{
		Path:         "src/app.ts",
		Content:      []byte(tsApp),
		Lang:         "typescript",
		SelfImportID: "ts:src/app",
	}
}

func TestDecorateTS(t *testing.T) {
	refs, err := Decorate(context.Background(), "typescript", []byte(tsApp), NavOptions{
		SelfImportID: "ts:src/app",
		Path:         "src/app.ts",
	})
	require.NoError(t, err)

	got := map[string]bool{}
	for _, r := range refs {
		assert.Equal(t, EdgeKindRef, r.Kind)
		got[r.TargetTicket] = true
	}
	logMod := "ts:src/util/log"
	// Named import `P` (alias of Print) -> module log, original name Print.
	assert.True(t, got[symbolTicket(logMod, "Print")],
		"aliased named import P() should target log#Print; got %v", keys(got))
	// Namespace member `log.Print` -> same ticket.
	// (both P() and log.Print() produce the same ticket, deduped in the map)
	// Same-module use of `helper` -> self.
	assert.True(t, got[symbolTicket("ts:src/app", "helper")],
		"same-module helper() should target app#helper; got %v", keys(got))
}

func TestTSDefinitions(t *testing.T) {
	defs, err := tsDefinitions(context.Background(), "typescript", []byte(tsLog))
	require.NoError(t, err)
	byName := map[string]Def{}
	for _, d := range defs {
		byName[d.Name] = d
	}
	require.Contains(t, byName, "Print")
	require.Contains(t, byName, "VERSION")
	assert.Equal(t, "func", byName["Print"].Kind)
	// The function_declaration node excludes the `export` keyword wrapper.
	assert.Equal(t, "function Print(): void", byName["Print"].Signature)
	assert.Equal(t, "Print writes a greeting.", byName["Print"].Doc)
	assert.Equal(t, "const", byName["VERSION"].Kind)
}

func TestResolveAndFindReferencesTS(t *testing.T) {
	logFile := DefFile{Path: "src/util/log.ts", Content: []byte(tsLog), Lang: "typescript"}
	logRef := RefFile{Path: "src/util/log.ts", Content: []byte(tsLog), Lang: "typescript", SelfImportID: "ts:src/util/log"}
	lk := tsFiles{
		byID:  map[string][]DefFile{"ts:src/util/log": {logFile}},
		byMod: map[string][]RefFile{"ts:src/util/log": {logRef, tsAppRefFile()}},
	}

	// Resolve the Print ticket -> its declaration in log.ts (line 2).
	locs, err := Resolve(context.Background(), lk, symbolTicket("ts:src/util/log", "Print"))
	require.NoError(t, err)
	require.Len(t, locs, 1)
	assert.Equal(t, "src/util/log.ts", locs[0].Path)
	assert.Equal(t, uint32(2), locs[0].Start.Line)

	// Find references to Print across the importing app.ts: the aliased P() and
	// the namespace log.Print() are both uses. log.ts itself doesn't use Print.
	refs, err := FindReferences(context.Background(), lk, symbolTicket("ts:src/util/log", "Print"))
	require.NoError(t, err)
	lines := map[uint32]string{}
	for _, r := range refs {
		assert.Equal(t, "src/app.ts", r.Path)
		lines[r.Start.Line] = r.Snippet
	}
	assert.Contains(t, lines, uint32(5), "P() use; got %v", lines)         // P();
	assert.Contains(t, lines, uint32(6), "log.Print() use; got %v", lines) // log.Print();
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
