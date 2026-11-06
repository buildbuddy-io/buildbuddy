package tunhelperutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalAddr(t *testing.T) {
	addr, err := LocalAddr("198.18.0.0/16")
	require.NoError(t, err)
	require.Equal(t, "198.18.0.1", addr.String())
	_, err = LocalAddr("fd00::/64")
	require.Error(t, err)
}
