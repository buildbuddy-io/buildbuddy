package execution_experiments

import (
	_ "embed"
	"regexp"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

//go:embed execution_experiments.go
var source string

// expflagDecl matches a package-level declaration that calls an expflag
// constructor, however the call is wrapped, and captures the constructor's
// first argument, the flag name.
var expflagDecl = regexp.MustCompile(`(?m)^\s*(?:var\s+)?\w+\s*=.*?\bexpflag\.\w+\(\s*([^,]+?)\s*,`)

// TestAllListsEveryFlag checks that every expflag declared in this package is
// in all, which means it was declared with register, so that All sends every
// experiment to executors.
func TestAllListsEveryFlag(t *testing.T) {
	var declared []string
	for _, m := range expflagDecl.FindAllStringSubmatch(source, -1) {
		name, err := strconv.Unquote(m[1])
		require.NoError(t, err, "expflag name must be a string literal, got %s", m[1])
		declared = append(declared, name)
	}
	var listed []string
	for _, f := range all {
		listed = append(listed, f.Name())
	}
	require.ElementsMatch(t, declared, listed, "every expflag declared in this package must be declared with register, exactly once")
}
