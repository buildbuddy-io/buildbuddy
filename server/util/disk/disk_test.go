package disk_test

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
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

func BenchmarkFileWriter(b *testing.B) {
	for _, limit := range []int{0, 5_000} {
		b.Run(fmt.Sprintf("limit=%d", limit), func(b *testing.B) {
			flags.Set(b, "file_writer_concurrency_limit", limit)
			// Make a bunch of random dirs to make sure we're not bottlenecked on
			// dir inode contention.
			dirs := make([]string, 1000)
			for i := range dirs {
				dirs[i] = testfs.MakeTempDir(b)
			}
			i := int64(-1)
			ctx := context.Background()
			var eg errgroup.Group
			for b.Loop() {
				for range 1_000 {
					eg.Go(func() error {
						dir := dirs[int(atomic.AddInt64(&i, 1))%len(dirs)]
						fw, err := disk.FileWriter(ctx, dir+"/"+strconv.Itoa(int(i)))
						if err != nil {
							return err
						}
						fw.Write([]byte("hello"))
						if err := fw.Commit(); err != nil {
							return err
						}
						if err := fw.Close(); err != nil {
							return err
						}
						return nil
					})
				}
				if err := eg.Wait(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
