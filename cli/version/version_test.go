package version_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/version"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedVersion(t *testing.T) {
	require.Equal(t, "unknown", version.String())
}
