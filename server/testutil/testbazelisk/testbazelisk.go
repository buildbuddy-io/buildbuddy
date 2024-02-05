package testbazelisk

import (
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/stretchr/testify/require"
)

// BinaryPath returns the path to bazelisk in runfiles.
func BinaryPath(t *testing.T) string {
	path, err := runfiles.Rlocation("com_github_bazelbuild_bazelisk/bazelisk_/bazelisk")
	require.NoError(t, err, "failed to locate bazelisk")
	return path
}
