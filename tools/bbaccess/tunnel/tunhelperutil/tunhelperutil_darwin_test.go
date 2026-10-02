//go:build darwin && !ios

package tunhelperutil

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// socketPath returns a short path: unix socket paths are limited to ~100 bytes.
func socketPath(t *testing.T) string {
	dir, err := os.MkdirTemp("/tmp", "tunhelper")
	require.NoError(t, err)
	t.Cleanup(func() { os.RemoveAll(dir) })
	return filepath.Join(dir, "s")
}

func TestHandsOutDescriptor(t *testing.T) {
	path := socketPath(t)
	l, err := net.Listen("unix", path)
	require.NoError(t, err)
	defer l.Close()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()
	go Serve(l, int(w.Fd()), "utun9", os.Getuid())

	fd, name, conn, err := receive(path)
	require.NoError(t, err)
	defer conn.Close()
	require.Equal(t, "utun9", name)
	got := os.NewFile(uintptr(fd), name)
	defer got.Close()
	_, err = got.WriteString("hi")
	require.NoError(t, err)
	buf := make([]byte, 2)
	_, err = io.ReadFull(r, buf)
	require.NoError(t, err)
	require.Equal(t, "hi", string(buf))

	// The helper keeps the connection open, so the client can watch for it.
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = conn.Read(buf)
	require.ErrorIs(t, err, os.ErrDeadlineExceeded)
}

func TestRefusesOtherUsers(t *testing.T) {
	if os.Getuid() == 0 {
		t.Skip("root is always allowed")
	}
	path := socketPath(t)
	l, err := net.Listen("unix", path)
	require.NoError(t, err)
	defer l.Close()
	r, w, err := os.Pipe()
	require.NoError(t, err)
	defer r.Close()
	defer w.Close()
	go Serve(l, int(w.Fd()), "utun9", os.Getuid()+1)

	_, _, _, err = receive(path)
	require.ErrorContains(t, err, "refused: not the owner")
}
