package testbazelisk

import (
	"testing"

	"github.com/stretchr/testify/require"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
)

// BinaryPath returns the path to bazelisk in runfiles.
func BinaryPath(t *testing.T) string {
	path, err := bazelgo.Runfile("external/com_github_bazelbuild_bazelisk/bazelisk_/bazelisk")
	require.NoError(t, err, "failed to locate bazelisk")
	return path
}
