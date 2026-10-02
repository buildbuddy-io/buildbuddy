//go:build linux && !android

package filecache_test

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestFileCacheLinuxFileTimestamps(t *testing.T) {
	initialAge := testmetrics.GaugeValue(t, metrics.FileCacheLastEvictionAgeUsec)
	t.Cleanup(func() { metrics.FileCacheLastEvictionAgeUsec.Set(initialAge) })
	for _, testCase := range []struct {
		name          string
		supportsBtime bool
		probeErr      error
		fileBtime     bool
		birthTime     time.Time
	}{
		{name: "birth_time", supportsBtime: true, fileBtime: true, birthTime: time.Now().Add(-time.Hour)},
		{name: "unsupported_filesystem"},
		{name: "unavailable_syscall", probeErr: unix.ENOSYS},
		{name: "blocked_syscall", probeErr: unix.EPERM},
		{name: "inode_without_birth_time", supportsBtime: true},
		{name: "zero_birth_time", supportsBtime: true, fileBtime: true, birthTime: time.Unix(0, 0)},
		{name: "negative_birth_time", supportsBtime: true, fileBtime: true, birthTime: time.Unix(-1, 0)},
		{name: "future_birth_time", supportsBtime: true, fileBtime: true, birthTime: time.Now().Add(time.Hour)},
	} {
		for _, recovery := range []string{"startup_scan", "disk_fallback"} {
			t.Run(testCase.name+"/"+recovery, func(t *testing.T) {
				ctx := t.Context()
				root := t.TempDir()
				node := nodeFromString("A", false)
				path := writeFileContent(t, root, "ANON/"+node.GetDigest().GetHash(), "A", false)
				var st unix.Stat_t
				require.NoError(t, unix.Stat(path, &st))
				ctime := time.Unix(int64(st.Ctim.Sec), int64(st.Ctim.Nsec))
				statxCtime := time.Now().Add(-30 * time.Minute)

				// Model unavailable birth time at the filesystem or inode level.
				// Count calls to ensure an unsupported constructor probe prevents
				// every later statx call, including file additions.
				var probes, fileStats atomic.Int64
				t.Cleanup(filecache.SetStatxForTest(func(dirfd int, p string, flags, mask int, result *unix.Statx_t) error {
					if p == root {
						probes.Add(1)
						if testCase.probeErr != nil {
							return testCase.probeErr
						}
						if testCase.supportsBtime {
							result.Mask = unix.STATX_BASIC_STATS | unix.STATX_BTIME
						}
						return nil
					}
					fileStats.Add(1)
					if err := unix.Statx(dirfd, p, flags, mask, result); err != nil {
						return err
					}
					result.Ctime = unix.StatxTimestamp{Sec: statxCtime.Unix(), Nsec: uint32(statxCtime.Nanosecond())}
					result.Mask &^= unix.STATX_BTIME
					if testCase.fileBtime {
						result.Mask |= unix.STATX_BTIME
						result.Btime = unix.StatxTimestamp{Sec: testCase.birthTime.Unix(), Nsec: uint32(testCase.birthTime.Nanosecond())}
					}
					return nil
				}))

				// Age the on-disk file so ordinary stat's ctime can be distinguished
				// from a timestamp assigned when the cache starts.
				time.Sleep(100 * time.Millisecond)
				if recovery == "disk_fallback" {
					filecache.DisableInitialDirectoryScanForTest()
					t.Cleanup(filecache.EnableInitialDirectoryScanForTest)
				}
				started := time.Now()
				fc, err := filecache.NewFileCache(root, st.Blocks*512, false)
				require.NoError(t, err)
				t.Cleanup(func() { fc.Close() })
				if recovery == "startup_scan" {
					fc.WaitForDirectoryScanToComplete()
				}
				require.True(t, fc.ContainsFile(ctx, node))

				// A second file fills the cache and evicts the recovered entry.
				// Its age must reflect the selected timestamp and its disk blocks
				// must count toward the cache capacity.
				source := writeFileContent(t, t.TempDir(), "B", "B", false)
				require.NoError(t, fc.AddFile(ctx, nodeFromString("B", false), source))
				require.NoFileExists(t, path)
				require.Equal(t, int64(1), probes.Load())
				if testCase.supportsBtime {
					require.Equal(t, int64(2), fileStats.Load())
				} else {
					require.Zero(t, fileStats.Load())
				}

				age := testmetrics.GaugeValue(t, metrics.FileCacheLastEvictionAgeUsec)
				expectedTime := ctime
				if testCase.supportsBtime {
					expectedTime = statxCtime
					if testCase.fileBtime {
						expectedTime = testCase.birthTime
					}
				}
				if expectedTime.UnixMicro() <= 0 || expectedTime.After(started) {
					// Invalid timestamps must not produce negative or enormous ages.
					require.GreaterOrEqual(t, age, float64(0))
					require.LessOrEqual(t, age, float64(time.Since(started).Microseconds()))
				} else {
					require.InDelta(t, time.Since(expectedTime).Microseconds(), age, float64((50 * time.Millisecond).Microseconds()))
				}
			})
		}
	}
}

func TestFileCacheEvictionAgeAfterHardlinkUse(t *testing.T) {
	initialAge := testmetrics.GaugeValue(t, metrics.FileCacheLastEvictionAgeUsec)
	t.Cleanup(func() { metrics.FileCacheLastEvictionAgeUsec.Set(initialAge) })
	ctx := t.Context()
	root := t.TempDir()
	node := nodeFromString("A", false)
	path := writeFileContent(t, root, "ANON/"+node.GetDigest().GetHash(), "A", false)
	var st unix.Statx_t
	require.NoError(t, unix.Statx(unix.AT_FDCWD, path, 0, unix.STATX_BASIC_STATS|unix.STATX_BTIME, &st))
	if st.Mask&unix.STATX_BTIME == 0 {
		t.Skip("filesystem does not support birth time")
	}
	birthTime := time.Unix(st.Btime.Sec, int64(st.Btime.Nsec))

	// Linking a cached file into a workspace and removing that link updates
	// ctime. After a restart, eviction age should retain the earlier birth time.
	time.Sleep(100 * time.Millisecond)
	link := filepath.Join(t.TempDir(), "workspace-input")
	require.NoError(t, os.Link(path, link))
	require.NoError(t, os.Remove(link))
	require.NoError(t, unix.Statx(unix.AT_FDCWD, path, 0, unix.STATX_BASIC_STATS|unix.STATX_BTIME, &st))
	require.Greater(t, time.Unix(st.Ctime.Sec, int64(st.Ctime.Nsec)).Sub(birthTime), 50*time.Millisecond)

	fc, err := filecache.NewFileCache(root, int64(st.Blocks)*512, false)
	require.NoError(t, err)
	t.Cleanup(func() { fc.Close() })
	fc.WaitForDirectoryScanToComplete()
	source := writeFileContent(t, t.TempDir(), "B", "B", false)
	require.NoError(t, fc.AddFile(ctx, nodeFromString("B", false), source))
	require.NoFileExists(t, path)
	age := testmetrics.GaugeValue(t, metrics.FileCacheLastEvictionAgeUsec)
	require.GreaterOrEqual(t, age, float64((100 * time.Millisecond).Microseconds()))
	require.InDelta(t, time.Since(birthTime).Microseconds(), age, float64((50 * time.Millisecond).Microseconds()))
}
