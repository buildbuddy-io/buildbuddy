package testbazelisk

import (
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/stretchr/testify/require"
)

// set by x_defs in BUILD file
var bazeliskRunfilePath string

// BinaryPath returns the path to bazelisk in runfiles.
func BinaryPath(t *testing.T) string {
	path, err := runfiles.Rlocation(bazeliskRunfilePath)
	require.NoError(t, err, "failed to locate bazelisk")
	return path
}
