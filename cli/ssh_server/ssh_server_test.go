package ssh_server

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestActivityReader(t *testing.T) {
	resets := 0
	r := &activityReader{
		r:     strings.NewReader("abc"),
		reset: func() { resets++ },
	}

	buf := make([]byte, 2)
	n, err := r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Equal(t, 1, resets, "read with data should reset")

	n, err = r.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, 2, resets)

	_, err = r.Read(buf)
	require.Equal(t, io.EOF, err)
	require.Equal(t, 2, resets, "EOF read should not reset")
}
