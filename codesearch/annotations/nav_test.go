package annotations

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testModule = "example.com/repo"

// refByTicket returns the first ref with the given target ticket, or nil.
func refByTicket(refs []Ref, ticket string) *Ref {
	for i := range refs {
		if refs[i].TargetTicket == ticket {
			return &refs[i]
		}
	}
	return nil
}

func tickets(refs []Ref) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.TargetTicket)
	}
	return out
}

func TestDecorateSelector(t *testing.T) {
	src := []byte(`package main

import (
	"fmt"
	"example.com/repo/util/log"
	lg "example.com/repo/util/altlog"
)

func main() {
	log.Infof("hi")
	lg.Warn("yo")
	fmt.Println("external")
}
`)
	refs, err := Decorate(context.Background(), "go", src, NavOptions{
		SelfImportID: GoSelfImportID(testModule, "cmd/main.go"),
		InRepo:       GoModuleInRepo(testModule),
	})
	require.NoError(t, err)

	// In-repo selector, default local name.
	logRef := refByTicket(refs, symbolTicket("go:example.com/repo/util/log", "Infof"))
	require.NotNil(t, logRef, "expected ref for log.Infof; got %v", tickets(refs))
	assert.Equal(t, EdgeKindRef, logRef.Kind)
	assert.Equal(t, uint32(10), logRef.Start.Line)
	// The decorated span covers only the symbol token, not "log.".
	assert.Equal(t, "Infof", string(src[logRef.Start.Byte:logRef.End.Byte]))

	// In-repo selector via an import alias resolves to the real import path.
	assert.NotNil(t, refByTicket(refs, symbolTicket("go:example.com/repo/util/altlog", "Warn")),
		"expected aliased ref; got %v", tickets(refs))

	// External package (fmt) is not in the repo, so it is not decorated.
	for _, r := range refs {
		_, sym, _ := parseSymbolTicket(r.TargetTicket)
		assert.NotEqual(t, "Println", sym, "fmt.Println should not be decorated")
	}
}

func TestDecorateLocal(t *testing.T) {
	src := []byte(`package svc

type Greeter struct{}

func Greet(g Greeter) string {
	return helper()
}

func helper() string {
	return "hi"
}
`)
	refs, err := Decorate(context.Background(), "go", src, NavOptions{
		SelfImportID: GoSelfImportID(testModule, "svc/svc.go"),
	})
	require.NoError(t, err)

	selfID := "go:example.com/repo/svc"

	// Use of the file-scope func `helper` is decorated, targeting this package.
	helperRef := refByTicket(refs, symbolTicket(selfID, "helper"))
	require.NotNil(t, helperRef, "expected local ref to helper; got %v", tickets(refs))
	assert.Equal(t, uint32(6), helperRef.Start.Line) // the call site, not the decl

	// Use of the file-scope type `Greeter` (the parameter type) is decorated.
	assert.NotNil(t, refByTicket(refs, symbolTicket(selfID, "Greeter")),
		"expected local ref to Greeter; got %v", tickets(refs))

	// A declaration site is never decorated as a reference to itself: there is
	// exactly one occurrence of `helper` decorated (the call on line 6), not
	// the declaration on line 9.
	count := 0
	for _, r := range refs {
		if r.TargetTicket == symbolTicket(selfID, "helper") {
			count++
		}
	}
	assert.Equal(t, 1, count, "only the call site should be decorated")
}

func TestDecorateNoSelfIDForTestFile(t *testing.T) {
	src := []byte(`package svc

func helper() string { return "" }

func use() string { return helper() }
`)
	// _test.go files have no import_id, so same-package references can't be
	// minted (nothing to resolve against).
	refs, err := Decorate(context.Background(), "go", src, NavOptions{
		SelfImportID: GoSelfImportID(testModule, "svc/svc_test.go"),
	})
	require.NoError(t, err)
	assert.Empty(t, refs)
}

func TestGoDefinitions(t *testing.T) {
	src := []byte(`package svc

type Greeter struct{}

const Version = "1"

func (g Greeter) Greet() {}

func Helper() {}
`)
	defs, err := goDefinitions(context.Background(), src)
	require.NoError(t, err)

	byName := make(map[string]Def)
	for _, d := range defs {
		byName[d.Name] = d
	}
	require.Contains(t, byName, "Greeter")
	require.Contains(t, byName, "Version")
	require.Contains(t, byName, "Greet")
	require.Contains(t, byName, "Helper")

	assert.Equal(t, uint32(3), byName["Greeter"].Start.Line)
	assert.Equal(t, uint32(9), byName["Helper"].Start.Line)
	// Span covers exactly the name token.
	g := byName["Greeter"]
	assert.Equal(t, "Greeter", string(src[g.Start.Byte:g.End.Byte]))
}

// fakeLookup resolves an import_id to a fixed set of files, ignoring the
// symbol filter (Resolve re-parses and filters by symbol itself).
type fakeLookup map[string][]DefFile

func (f fakeLookup) FindDefs(_ context.Context, importID, _ string) ([]DefFile, error) {
	return f[importID], nil
}

func TestResolve(t *testing.T) {
	defFile := DefFile{
		Path: "util/log/log.go",
		Lang: "go",
		Content: []byte(`package log

func Debugf(string) {}

func Infof(format string) {}
`),
	}
	lk := fakeLookup{"go:example.com/repo/util/log": {defFile}}

	locs, err := Resolve(context.Background(), lk, symbolTicket("go:example.com/repo/util/log", "Infof"))
	require.NoError(t, err)
	require.Len(t, locs, 1)
	assert.Equal(t, "util/log/log.go", locs[0].Path)
	assert.Equal(t, uint32(5), locs[0].Start.Line)

	// A symbol the file doesn't declare resolves to nothing.
	locs, err = Resolve(context.Background(), lk, symbolTicket("go:example.com/repo/util/log", "Missing"))
	require.NoError(t, err)
	assert.Empty(t, locs)
}

func TestDescribe(t *testing.T) {
	defFile := DefFile{
		Path: "util/log/log.go",
		Lang: "go",
		Content: []byte(`package log

// Infof logs a formatted message.
// It is safe for concurrent use.
func Infof(format string, args ...any) {}

type Greeter struct{}
`),
	}
	lk := fakeLookup{"go:example.com/repo/util/log": {defFile}}

	defs, err := Describe(context.Background(), lk, symbolTicket("go:example.com/repo/util/log", "Infof"))
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "func", defs[0].Kind)
	assert.Equal(t, "func Infof(format string, args ...any)", defs[0].Signature)
	assert.Equal(t, "Infof logs a formatted message.\nIt is safe for concurrent use.", defs[0].Doc)
	assert.Equal(t, uint32(5), defs[0].Start.Line)

	// A struct type with no doc comment.
	defs, err = Describe(context.Background(), lk, symbolTicket("go:example.com/repo/util/log", "Greeter"))
	require.NoError(t, err)
	require.Len(t, defs, 1)
	assert.Equal(t, "struct", defs[0].Kind)
	assert.Equal(t, "type Greeter struct{}", defs[0].Signature)
	assert.Empty(t, defs[0].Doc)
}

// fakeRefLookup returns a fixed set of candidate files for any import_id.
type fakeRefLookup map[string][]RefFile

func (f fakeRefLookup) FindReferencingFiles(_ context.Context, importID string) ([]RefFile, error) {
	return f[importID], nil
}

func TestFindReferences(t *testing.T) {
	// A cross-package importer using log.Print twice.
	mainFile := RefFile{
		Path: "app/main.go",
		Lang: "go",
		Content: []byte(`package main

import "example.com/repo/util/log"

func main() {
	log.Print()
	log.Print()
}
`),
		SelfImportID: "go:example.com/repo/app",
		InRepo:       func(imp string) bool { return imp == "example.com/repo/util/log" },
	}
	// The declaring file, which also uses Print within itself.
	defFile := RefFile{
		Path: "util/log/log.go",
		Lang: "go",
		Content: []byte(`package log

func Print() {}

func reprint() { Print() }
`),
		SelfImportID: "go:example.com/repo/util/log",
		InRepo:       func(string) bool { return false },
	}
	rl := fakeRefLookup{"go:example.com/repo/util/log": {mainFile, defFile}}

	refs, err := FindReferences(context.Background(), rl, symbolTicket("go:example.com/repo/util/log", "Print"))
	require.NoError(t, err)
	// Two cross-package uses in main.go + one same-file use in log.go. The
	// Print declaration itself is not a reference.
	require.Len(t, refs, 3)

	byPath := map[string][]string{}
	for _, r := range refs {
		byPath[r.Path] = append(byPath[r.Path], r.Snippet)
	}
	assert.Equal(t, []string{"log.Print()", "log.Print()"}, byPath["app/main.go"])
	assert.Equal(t, []string{"func reprint() { Print() }"}, byPath["util/log/log.go"])
}

func TestResolveIgnoresForeignTickets(t *testing.T) {
	locs, err := Resolve(context.Background(), fakeLookup{}, "kythe://buildbuddy?path=x.go")
	require.NoError(t, err)
	assert.Nil(t, locs)
}

func TestTicketRoundTrip(t *testing.T) {
	ticket := symbolTicket("go:example.com/repo/util/log", "Infof")
	assert.True(t, HasScheme(ticket), "minted ticket carries the scheme: %s", ticket)

	id, sym, ok := parseSymbolTicket(ticket)
	require.True(t, ok)
	assert.Equal(t, "go:example.com/repo/util/log", id)
	assert.Equal(t, "Infof", sym)

	// Non-tree-sitter and non-symbol tickets are rejected.
	_, _, ok = parseSymbolTicket("kythe://buildbuddy?path=x.go")
	assert.False(t, ok, "kythe ticket")
	_, _, ok = parseSymbolTicket("tree-sitter://buildbuddy?path=x.go")
	assert.False(t, ok, "file ticket has no pkg/sym")

	// HasScheme accepts file tickets too (used for request routing).
	assert.True(t, HasScheme("tree-sitter://buildbuddy?path=x.go"))
	assert.False(t, HasScheme("kythe://buildbuddy?path=x.go"))
}
