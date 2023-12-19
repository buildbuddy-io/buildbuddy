package common_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/common"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedVersion(t *testing.T) {
	require.Contains(t, common.Version(), "unknown")
}
