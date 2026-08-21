// These tests fail most of the time when run with `--config=remote --remote_header=x-buildbuddy-platform.enable-vfs=true`
package disk_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/fsync"
)

func TestFileWriterParallel(t *testing.T) {
	ctx := context.Background()
	root := testfs.MakeTempDir(t)
	filePath := filepath.Join(root, "testfile")
	var buf []byte
	for i := range 10000 {
		buf = append(buf, 'a'+byte(i%26))
	}

	procs := runtime.GOMAXPROCS(0) * 2
	iterations := 1000
	wg := sync.WaitGroup{}
	wg.Add(procs)
	for range procs {
		go func() {
			defer wg.Done()
			for range iterations {
				w, err := disk.FileWriter(ctx, filePath)
				require.NoError(t, err)

				_, err = w.Write(buf)
				require.NoError(t, err)

				require.NoError(t, w.Commit())
				require.NoError(t, w.Close())

				r, err := disk.FileReader(ctx, filePath, 0, 0)
				require.NoError(t, err)
				got, err := io.ReadAll(r)
				require.NoError(t, err)
				require.Equal(t, buf, got)
				require.NoError(t, r.Close())
			}
		}()
	}
	wg.Wait()
}

func TestRawParallel(t *testing.T) {
	root := testfs.MakeTempDir(t)
	filePath := filepath.Join(root, "testfile")
	var buf []byte
	for i := range 10000 {
		buf = append(buf, 'a'+byte(i%26))
	}

	procs := runtime.GOMAXPROCS(0) * 2
	iterations := 1000
	wg := sync.WaitGroup{}
	wg.Add(procs)
	for p := range procs {
		go func() {
			defer wg.Done()
			for i := range iterations {
				tmp := fmt.Sprintf("%s.%d.%d.tmp", filePath, p, i)
				require.NoError(t, os.WriteFile(tmp, buf, 0644))

				// Reading from the temp file works
				got, err := os.ReadFile(tmp)
				require.NoError(t, err)
				require.Equal(t, buf, got)

				// Renaming to the final path works
				require.NoError(t, os.Rename(tmp, filePath))
				// Sync doesn't seem to help
				require.NoError(t, fsync.SyncPath(filePath))

				// Reading from the final path sometimes fails.
				got, err = os.ReadFile(filePath)
				require.NoError(t, err)
				require.Equal(t, buf, got)
			}
		}()
	}
	wg.Wait()
}
