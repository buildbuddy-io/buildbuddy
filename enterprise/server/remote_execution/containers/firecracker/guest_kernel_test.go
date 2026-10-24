package firecracker

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestSelectGuestKernel(t *testing.T) {
	for _, tc := range []struct {
		name, requested, defaultVersion, want string
		experiment, unavailable               bool
		wantInvalid, wantUnavailable          bool
	}{
		{name: "default 5.15", defaultVersion: "5.15", want: "5.15"},
		{name: "default 6.18", defaultVersion: "6.18", want: "6.18"},
		{name: "rollout", defaultVersion: "5.15", experiment: true, want: "6.18"},
		{name: "explicit beats rollout", requested: "5.15", defaultVersion: "5.15", experiment: true, want: "5.15"},
		{name: "explicit beats default", requested: "5.15", defaultVersion: "6.18", want: "5.15"},
		{name: "explicit upgrade", requested: "6.18", defaultVersion: "5.15", want: "6.18"},
		{name: "unavailable explicit", requested: "6.18", defaultVersion: "5.15", unavailable: true, wantUnavailable: true},
		{name: "unavailable rollout", defaultVersion: "5.15", unavailable: true, experiment: true, want: "5.15"},
		{name: "unknown", requested: "6.17", defaultVersion: "5.15", wantInvalid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &ExecutorConfig{
				GuestKernelImagePath:     "/kernels/" + tc.defaultVersion,
				GuestKernelVersion:       "hash-" + tc.defaultVersion,
				GuestKernelImagePath5_15: "/kernels/5.15", GuestKernelVersion5_15: "hash-5.15",
				GuestKernelImagePath6_18: "/kernels/6.18", GuestKernelVersion6_18: "hash-6.18",
			}
			if tc.unavailable {
				cfg.GuestKernelImagePath6_18, cfg.GuestKernelVersion6_18 = "", ""
			}
			var experiments []string
			if tc.experiment {
				experiments = []string{"upgrade-fc-guest-kernel"}
			}
			path, hash, err := cfg.selectGuestKernel(tc.requested, experiments)
			if tc.wantInvalid {
				require.True(t, status.IsInvalidArgumentError(err), "%v", err)
				return
			}
			if tc.wantUnavailable {
				require.True(t, status.IsFailedPreconditionError(err), "%v", err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, "/kernels/"+tc.want, path)
			require.Equal(t, "hash-"+tc.want, hash)
		})
	}
}

func TestGuestKernelBootAndSnapshot(t *testing.T) {
	// Configure the mmap budget before constructing the shared COW cache.
	require.NoError(t, resources.Configure(true /* snapshotSharingEnabled */))
	flags.Set(t, "executor.enable_local_snapshot_sharing", true)
	flags.Set(t, "executor.enable_remote_snapshot_sharing", false)
	flags.Set(t, "executor.firecracker_cgroup_version", "2")
	env := testenv.GetTestEnv(t)
	fc, err := filecache.NewFileCache(t.TempDir(), 1_000_000, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc)
	_, run, lis := testenv.RegisterLocalGRPCServer(t, env)
	testcache.Setup(t, env, lis)
	go run()
	// The jailer imposes a 38-character limit, shorter than Bazel's test temp path.
	root, err := os.MkdirTemp("/tmp", "fc-kernel-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(root)) })
	write := func(t *testing.T, path, contents string) string {
		t.Helper()
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0755))
		require.NoError(t, os.WriteFile(path, []byte(contents), 0644))
		return path
	}
	kernel515 := write(t, filepath.Join(root, "vmlinux-5.15"), "kernel 5.15")
	kernel618 := write(t, filepath.Join(root, "vmlinux-6.18"), "kernel 6.18")
	hash515, err := digest.ComputeForFile(kernel515, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	hash618, err := digest.ComputeForFile(kernel618, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	cfg := &ExecutorConfig{
		JailerRoot: root, InitrdImagePath: write(t, filepath.Join(root, "initrd.cpio"), "initrd"),
		GuestKernelImagePath: kernel515, GuestKernelVersion: hash515.GetHash(),
		GuestKernelImagePath5_15: kernel515, GuestKernelVersion5_15: hash515.GetHash(),
		GuestKernelImagePath6_18: kernel618, GuestKernelVersion6_18: hash618.GetHash(),
	}
	prepareSnapshot := func(t *testing.T, c *FirecrackerContainer) {
		t.Helper()
		for _, name := range []string{containerFSName, scratchFSName, "memory.snap", "vmstate.snap"} {
			write(t, filepath.Join(c.getChroot(), name), name)
		}
		// Use the same local chunked-cache path as ordinary snapshots.
		store := func(name string) *copy_on_write.COWStore {
			cow, err := copy_on_write.ConvertFileToCOW(t.Context(), env, filepath.Join(c.getChroot(), name), 4096, t.TempDir(), "", false, snaputil.ConvertToCOWConcurrency)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, cow.Close()) })
			return cow
		}
		c.scratchStore = store(scratchFSName)
		c.memoryStore = store("memory.snap")
	}
	keys := map[string]string{}
	for _, version := range []string{"5.15", "6.18"} {
		t.Run(version, func(t *testing.T) {
			task := &repb.ExecutionTask{ExecuteRequest: &repb.ExecuteRequest{DigestFunction: repb.DigestFunction_SHA256}}
			c, err := NewContainer(t.Context(), env, task, ContainerOpts{
				VMConfiguration: &fcpb.VMConfiguration{NumCpus: 1, MemSizeMb: 128},
				ExecutorConfig:  cfg, GuestKernelVersion: version,
			})
			require.NoError(t, err)
			wantPath, wantDigest := kernel515, hash515.GetHash()
			if version == "6.18" {
				wantPath, wantDigest = kernel618, hash618.GetHash()
			}
			require.Equal(t, wantDigest, c.vmConfig.GuestKernelVersion)
			keys[version] = c.SnapshotKeySet().GetBranchKey().GetConfigurationHash()
			// Later task experiments cannot change the kernel already in the key.
			task.Experiments = []string{"upgrade-fc-guest-kernel"}
			boot, err := c.getConfig(t.Context(), "rootfs", "containerfs", "scratchfs", "workspacefs")
			require.NoError(t, err)
			require.Equal(t, wantPath, boot.KernelImagePath)
			prepareSnapshot(t, c)
			require.NoError(t, c.saveSnapshot(t.Context(), &snapshotDetails{
				memSnapshotName: "memory.snap", vmStateSnapshotName: "vmstate.snap", saveLocalSnapshot: true,
			}))
			manifest, err := c.loader.GetLocalManifest(t.Context(), c.SnapshotKeySet().GetWriteKey(), false)
			require.NoError(t, err)
			require.Equal(t, wantDigest, manifest.GetVmConfiguration().GetGuestKernelVersion())
			var savedKernel *repb.FileNode
			for _, file := range manifest.GetFiles() {
				if file.Name == filepath.Base(wantPath) {
					savedKernel = file
				}
				if file.Name != filepath.Base(wantPath) {
					require.NotContains(t, file.Name, "vmlinux")
				}
			}
			require.NotNil(t, savedKernel, "snapshot must contain the selected kernel")
			restored := filepath.Join(t.TempDir(), "kernel")
			require.True(t, fc.FastLinkFile(t.Context(), savedKernel, restored))
			restoredDigest, err := digest.ComputeForFile(restored, repb.DigestFunction_SHA256)
			require.NoError(t, err)
			require.Equal(t, wantDigest, restoredDigest.GetHash())
			require.NoError(t, c.validateSnapshotKernel(manifest.GetVmConfiguration()))
			other := manifest.GetVmConfiguration().CloneVT()
			other.GuestKernelVersion = "other-build"
			require.True(t, status.IsFailedPreconditionError(c.validateSnapshotKernel(other)))
			// A missing snapshot config must fail before any VM setup starts.
			require.True(t, status.IsFailedPreconditionError(c.loadSnapshot(t.Context(), &snaploader.Snapshot{})))

			// An implicit override may restore a kernel other than this
			// executor's default. Its next snapshot must keep those same bytes.
			receiverTask := &repb.ExecutionTask{ExecuteRequest: &repb.ExecuteRequest{DigestFunction: repb.DigestFunction_SHA256}}
			receiver, err := NewContainer(t.Context(), env, receiverTask, ContainerOpts{
				VMConfiguration:     &fcpb.VMConfiguration{NumCpus: 1, MemSizeMb: 128},
				ExecutorConfig:      cfg,
				OverrideSnapshotKey: c.SnapshotKeySet().GetWriteKey(),
			})
			require.NoError(t, err)
			require.Equal(t, keys[version], receiver.SnapshotKeySet().GetWriteKey().GetConfigurationHash())
			require.NoError(t, receiver.newID(t.Context()))
			name, err := receiver.snapshotKernelImageName(manifest.GetVmConfiguration(), manifest.GetFiles())
			require.NoError(t, err)
			unpackedKernel := filepath.Join(receiver.getChroot(), name)
			require.NoError(t, os.MkdirAll(receiver.getChroot(), 0755))
			if wantDigest != receiver.vmConfig.GuestKernelVersion {
				write(t, unpackedKernel, "wrong kernel")
				require.True(t, status.IsFailedPreconditionError(receiver.adoptSnapshotKernel(manifest.GetVmConfiguration(), name)))
				require.Equal(t, hash515.GetHash(), receiver.vmConfig.GuestKernelVersion)
				require.NoError(t, os.Remove(unpackedKernel))
			}
			require.True(t, fc.FastLinkFile(t.Context(), savedKernel, unpackedKernel))
			require.NoError(t, receiver.adoptSnapshotKernel(manifest.GetVmConfiguration(), name))
			require.Equal(t, wantDigest, receiver.vmConfig.GuestKernelVersion)
			require.Equal(t, unpackedKernel, receiver.guestKernelImagePath)
			prepareSnapshot(t, receiver)
			require.NoError(t, receiver.saveSnapshot(t.Context(), &snapshotDetails{
				memSnapshotName: "memory.snap", vmStateSnapshotName: "vmstate.snap", saveLocalSnapshot: true,
			}))
			resaved, err := receiver.loader.GetLocalManifest(t.Context(), receiver.SnapshotKeySet().GetWriteKey(), false)
			require.NoError(t, err)
			require.Equal(t, wantDigest, resaved.GetVmConfiguration().GetGuestKernelVersion())
			foundKernel := false
			for _, file := range resaved.GetFiles() {
				if file.Name == name {
					foundKernel = true
					path := filepath.Join(t.TempDir(), "resaved-kernel")
					require.True(t, fc.FastLinkFile(t.Context(), file, path))
					d, err := digest.ComputeForFile(path, repb.DigestFunction_SHA256)
					require.NoError(t, err)
					require.Equal(t, wantDigest, d.GetHash())
				}
			}
			require.True(t, foundKernel, "new snapshot must contain the restored kernel")
		})
	}
	require.NotEqual(t, keys["5.15"], keys["6.18"], "kernel choice must separate snapshot configuration keys")
}

func TestSnapshotKernelArtifact(t *testing.T) {
	c := &FirecrackerContainer{
		executorConfig: &ExecutorConfig{
			GuestKernelImagePath: "/custom/kernel", GuestKernelImagePath5_15: "/kernels/vmlinux", GuestKernelImagePath6_18: "/kernels/vmlinux_6.18",
		},
		vmConfig: &fcpb.VMConfiguration{GuestKernelVersion: "current"},
	}
	config := &fcpb.VMConfiguration{GuestKernelVersion: "snapshot"}
	for _, tc := range []struct {
		name  string
		files []string
		want  string
	}{
		{name: "custom default", files: []string{"kernel", "initrd.cpio"}, want: "kernel"},
		{name: "old default", files: []string{"vmlinux"}, want: "vmlinux"},
		{name: "new kernel", files: []string{"vmlinux_6.18"}, want: "vmlinux_6.18"},
		{name: "missing"},
		{name: "multiple", files: []string{"vmlinux", "vmlinux_6.18"}},
		{name: "duplicate", files: []string{"vmlinux", "vmlinux"}},
		{name: "path traversal", files: []string{"../vmlinux"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var files []*repb.FileNode
			for _, name := range tc.files {
				files = append(files, &repb.FileNode{Name: name})
			}
			name, err := c.snapshotKernelImageName(config, files)
			if tc.want == "" {
				require.True(t, status.IsFailedPreconditionError(err), "%v", err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, name)
			}
		})
	}
}

func TestSnapshotKernelRejectsOldRolloutMismatch(t *testing.T) {
	root := t.TempDir()
	current := filepath.Join(root, "vmlinux_6.18")
	require.NoError(t, os.WriteFile(current, []byte("kernel 6.18"), 0644))
	d, err := digest.ComputeForFile(current, repb.DigestFunction_SHA256)
	require.NoError(t, err)
	c := &FirecrackerContainer{
		executorConfig: &ExecutorConfig{
			JailerRoot: root, GuestKernelImagePath: filepath.Join(root, "vmlinux"), GuestKernelImagePath6_18: current,
		},
		guestKernelImagePath: current,
		vmConfig:             &fcpb.VMConfiguration{GuestKernelVersion: d.GetHash()},
	}
	// Old rollout snapshots claimed 6.18 but cached the default 5.15 kernel.
	require.NoError(t, os.MkdirAll(c.getChroot(), 0755))
	require.NoError(t, os.WriteFile(filepath.Join(c.getChroot(), "vmlinux"), []byte("kernel 5.15"), 0644))
	name, err := c.snapshotKernelImageName(c.vmConfig, []*repb.FileNode{{Name: "vmlinux"}})
	require.NoError(t, err)
	require.True(t, status.IsFailedPreconditionError(c.adoptSnapshotKernel(c.vmConfig, name)))
	require.Equal(t, current, c.guestKernelImagePath)
	require.Equal(t, d.GetHash(), c.vmConfig.GuestKernelVersion)
}
