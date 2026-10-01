//go:build linux

package dirtools

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestPublishDownloadedExecutableWaitsForWriter(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cancel bool
	}{
		{name: "writer closes"},
		{name: "canceled", cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "executable")
			contents := make([]byte, BatchReadLimitBytes+1)
			copy(contents, "#!/bin/true\n")
			require.NoError(t, os.WriteFile(path, contents, 0755))
			requireExecutableReadinessSupported(t, path)
			writer, err := os.OpenFile(path, os.O_WRONLY, 0)
			require.NoError(t, err)
			t.Cleanup(func() { writer.Close() })

			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			fileCache := &publicationTestFileCache{added: make(chan string, 1)}
			ff := &BatchFileFetcher{
				ctx: ctx,
				env: &publicationTestEnv{fileCache: fileCache},
			}
			fp := &FilePointer{
				FullPath: path,
				FileNode: &repb.FileNode{
					Name:         "executable",
					Digest:       &repb.Digest{SizeBytes: int64(len(contents))},
					IsExecutable: true,
				},
			}
			done := make(chan error, 1)
			go func() { done <- ff.publishDownloadedFile(ctx, fp) }()
			select {
			case err := <-done:
				t.Fatalf("publication returned while a writer was open: %v", err)
			case path := <-fileCache.added:
				t.Fatalf("file cache received %q while a writer was open", path)
			case <-time.After(25 * time.Millisecond):
			}

			if tc.cancel {
				cancel()
				select {
				case err := <-done:
					require.ErrorIs(t, err, context.Canceled)
				case <-time.After(time.Second):
					t.Fatal("publication did not stop after cancellation")
				}
				select {
				case path := <-fileCache.added:
					t.Fatalf("file cache received %q after readiness was canceled", path)
				default:
				}
				return
			}

			require.NoError(t, writer.Close())
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-ctx.Done():
				t.Fatal("publication did not complete after the writer closed")
			}
			select {
			case addedPath := <-fileCache.added:
				require.Equal(t, path, addedPath)
			default:
				t.Fatal("file cache did not receive the executable")
			}
			require.NoError(t, exec.CommandContext(ctx, path).Run())
		})
	}
}

func TestWaitForExecutableReadyReleasesLease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executable")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/true\n"), 0755))
	requireExecutableReadinessSupported(t, path)
	require.NoError(t, waitForExecutableReady(t.Context(), path))

	writer, err := os.OpenFile(path, os.O_WRONLY|syscall.O_NONBLOCK, 0)
	require.NoError(t, err)
	require.NoError(t, writer.Close())
}

func TestWaitForExecutableReadyPreservesBehaviorWhenLeasesUnsupported(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executable")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/true\n"), 0755))
	for _, leaseErr := range []error{unix.EINVAL, unix.EOPNOTSUPP, unix.ENOSYS} {
		t.Run(leaseErr.Error(), func(t *testing.T) {
			fcntl := func(uintptr, int, int) (int, error) { return 0, leaseErr }
			require.NoError(t, waitForExecutableReadyWithFcntl(t.Context(), path, fcntl))
			require.NoError(t, exec.CommandContext(t.Context(), path).Run())
		})
	}
}

func TestWaitForExecutableReadyReturnsUnexpectedLeaseError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executable")
	require.NoError(t, os.WriteFile(path, []byte("#!/bin/true\n"), 0755))
	fcntl := func(uintptr, int, int) (int, error) { return 0, unix.EIO }
	require.ErrorIs(t, waitForExecutableReadyWithFcntl(t.Context(), path, fcntl), unix.EIO)
}

func TestWaitForExecutableReadyDoesNotIgnoreNonRegularFile(t *testing.T) {
	fcntl := func(uintptr, int, int) (int, error) { return 0, unix.EINVAL }
	require.ErrorIs(t, waitForExecutableReadyWithFcntl(t.Context(), t.TempDir(), fcntl), unix.EINVAL)
}

func requireExecutableReadinessSupported(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	_, err = unix.FcntlInt(f.Fd(), unix.F_SETLEASE, unix.F_RDLCK)
	if errors.Is(err, unix.EINVAL) || errors.Is(err, unix.EOPNOTSUPP) || errors.Is(err, unix.ENOSYS) {
		t.Skipf("file leases are unsupported: %s", err)
	}
	require.NoError(t, err)
	_, err = unix.FcntlInt(f.Fd(), unix.F_SETLEASE, unix.F_UNLCK)
	require.NoError(t, err)
}

type publicationTestEnv struct {
	environment.Env
	fileCache interfaces.FileCache
}

func (e *publicationTestEnv) GetFileCache() interfaces.FileCache {
	return e.fileCache
}

type publicationTestFileCache struct {
	interfaces.FileCache
	added chan string
}

func (c *publicationTestFileCache) AddFile(ctx context.Context, node *repb.FileNode, path string) error {
	c.added <- path
	return nil
}
