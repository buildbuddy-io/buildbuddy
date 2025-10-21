package disk_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
)

func TestGetDirUsage(t *testing.T) {
	dir := testfs.MakeTempDir(t)
	testfs.MakeTempFile(t, dir, t.Name()+"-*")

	usage, err := disk.GetDirUsage(dir)
	require.NoError(t, err)
	require.Greater(t, usage.TotalBytes, uint64(0))
	require.Equal(t, usage.TotalBytes, usage.UsedBytes+usage.FreeBytes)
	require.GreaterOrEqual(t, usage.FreeBytes, usage.AvailBytes)
}

func TestWriteMover_CloseCleansUp(t *testing.T) {
	for _, shouldCancel := range []bool{true, false} {
		t.Run(fmt.Sprintf("cancel=%v", shouldCancel), func(t *testing.T) {
			for _, shouldCommit := range []bool{true, false} {
				t.Run(fmt.Sprintf("commit=%v", shouldCommit), func(t *testing.T) {
					dir := testfs.MakeTempDir(t)
					ctx, cancel := context.WithCancel(context.Background())
					defer cancel()
					path := filepath.Join(dir, "testfile")
					w, err := disk.FileWriter(ctx, path)
					require.NoError(t, err)
					_, err = w.Write([]byte("hello"))
					require.NoError(t, err)

					if shouldCommit {
						require.NoError(t, w.Commit())
					}

					if shouldCancel {
						// Cancel the context to simulate a timeout or RPC cancellation.
						// This should cause w.Close to fail to get writer quota, but it
						// shoud still clean up the temp file.
						cancel()
					}
					w.Close()
					entries, err := os.ReadDir(dir)
					require.NoError(t, err)
					for _, ent := range entries {
						if shouldCommit {
							require.Equal(t, "testfile", ent.Name())
						} else {
							t.Errorf("Unexpected file %v", ent.Name())
						}
					}
				})
			}
		})
	}
}
