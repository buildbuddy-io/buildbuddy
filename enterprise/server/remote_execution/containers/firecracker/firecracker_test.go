package firecracker_test

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/copy_on_write"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaputil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/vbd"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testcontainer"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/testing/protocmp"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	busyboxImage = "mirror.gcr.io/library/busybox:latest"
	// Alternate image to use if getting rate-limited by docker hub
	// busyboxImage = "gcr.io/google-containers/busybox:latest"

	ubuntuImage              = "mirror.gcr.io/library/ubuntu:20.04"
	imageWithDockerInstalled = "gcr.io/flame-public/executor-docker-default:enterprise-v1.6.0"

	// Minimum memory needed for a firecracker VM. This may need to be increased
	// if the size of initrd.cpio increases.
	minMemSizeMB = 200

	diskCacheSize = 10_000_000_000  // 10GB
	fileCacheSize = 100_000_000_000 // 100GB
)

var (
	testExecutorRoot = flag.String("test_executor_root", "/tmp/test-executor-root", "If set, use this as the executor root data dir. Helps avoid excessive image pulling when re-running tests.")
	// TODO(bduffany): make the bazel test a benchmark, and run it for both
	// NBD and non-NBD.
	testBazelBuild = flag.Bool("test_bazel_build", false, "Whether to test a bazel build.")
	filecacheDir   = flag.String("persistent_filecache_dir", "", "Filecache directory to be used across test runs.")

	skipDockerTests = flag.Bool("skip_docker_tests", false, "Whether to skip docker-in-firecracker tests")
)

var (
	cleaned = map[testing.TB]bool{}
)

func init() {
	// Set umask to match the executor process.
	syscall.Umask(0)

	// Configure resource limits to ensure we allocate enough memory for the
	// shared LRU in copy_on_write.
	if err := resources.Configure(true /*=snapshotSharingEnabled*/); err != nil {
		log.Fatalf("Failed to configure resources: %s", err)
	}

	// Some tests need iptables which is in /usr/sbin.
	err := os.Setenv("PATH", os.Getenv("PATH")+":/usr/sbin")
	if err != nil {
		log.Fatal(err.Error())
	}
}

func TestGuestAPIVersion(t *testing.T) {
	// When changing either `goinit/main.go` or `vmexec.go`, the
	// "guest API hash" below will change, as a very rough way to detect
	// breaking changes in the API exported by the guest.
	//
	// When this happens, first identify whether you've made a compatible or
	// incompatible change to the guest API.
	//
	// Examples of compatible changes:
	// - Adding comments
	// - Small cleanups/refactoring which do not change behavior
	// - Adding a new flag to goinit, which firecracker.go does not yet set
	//   at the guest command line
	// - Adding a new proto field to vmexec.proto that is not yet used
	// - Exposing *optional* new metadata from the Exec API
	//
	// Examples of incompatible changes:
	// - Passing a new flag to goinit which older versions don't support
	// - Relying on new vmexec server behavior
	//
	// When this happens, the test below will fail, and you can do one of two
	// things:
	//
	// (1) If you made an incompatible change to the guest API (or you fixed a
	//     bug etc. that you really want fixed for all actions ASAP), update the
	//     hash below and bump the expected version.
	//     Then update GuestAPIVersion in firecracker.go to match.
	// (2) If you made a compatible change, just update the hash but do not
	//     bump the expected version.
	//
	// Note that if you go with option 1, ALL VM snapshots will be invalidated
	// which will negatively affect customer experience. Be careful!
	const (
		expectedHash    = "0a4d754cb1b71a32c7f1bea13ac4a69c2e4f045ccf08e44ff08157dc0bdd9a35"
		expectedVersion = "8"
	)
	assert.Equal(t, expectedHash, firecracker.GuestAPIHash)
	assert.Equal(t, expectedVersion, firecracker.GuestAPIVersion)
	if t.Failed() {
		t.Log("Possible breaking change in VM guest API detected. Please see the instructions in firecracker_test.go > TestGuestAPIVersion")
	}
}

// cleanExecutorRoot cleans all entries in the test root dir *except* for cached
// ext4 images. Converting docker images to ext4 images takes a long time and
// it would slow down testing to build these images from scratch every time.
// So we instead keep the same directory around and clean it between tests.
//
// See README.md for more details on the filesystem layout.
func cleanExecutorRoot(t *testing.T, path string) {
	if os.Getuid() == 0 {
		// Clean up VBD mounts that might've been left around from previous
		// tests that were interrupted. Otherwise we won't be able to clean up
		// old firecracker workspaces.
		err := vbd.CleanStaleMounts()
		require.NoError(t, err)
	}

	err := os.MkdirAll(path, 0755)
	require.NoError(t, err)
	entries, err := os.ReadDir(path)
	require.NoError(t, err)
	for _, entry := range entries {
		// The "/executor" subdir contains the cached images.
		// Delete all other content.
		if entry.Name() == "executor" {
			continue
		}
		err := os.RemoveAll(filepath.Join(path, entry.Name()))
		require.NoError(t, err)
	}
}

type envOpts struct {
	cacheRootDir     string
	cacheSize        int64
	filecacheRootDir string
}

func getTestEnv(ctx context.Context, t *testing.T, opts envOpts) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)

	// Use a permissive image cache authenticator to avoid registry requests.
	env.SetImageCacheAuthenticator(testcontainer.PermissiveImageCacheAuthenticator())

	// Use temp file for skopeo auth file.
	// See https://github.com/containers/skopeo/issues/1240
	tmp := testfs.MakeTempDir(t)
	err := os.Setenv("REGISTRY_AUTH_FILE", filepath.Join(tmp, "auth.json"))
	require.NoError(t, err)

	// Clean up any lingering networking changes from previous test runs.
	// TODO: make the executor more robust when an IP is already in use,
	// and remove this.
	err = networking.DeleteNetNamespaces(ctx)
	require.NoError(t, err)

	testRootDir := opts.cacheRootDir
	if testRootDir == "" {
		testRootDir = testfs.MakeTempDir(t)
	}
	cacheSize := opts.cacheSize
	if cacheSize == 0 {
		cacheSize = diskCacheSize
	}
	dc, err := disk_cache.NewDiskCache(env, &disk_cache.Options{RootDirectory: testRootDir}, cacheSize)
	if err != nil {
		t.Error(err)
	}
	env.SetCache(dc)
	casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		t.Error(err)
	}
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		t.Error(err)
	}
	actionCacheServer, err := action_cache_server.NewActionCacheServer(env)
	if err != nil {
		t.Error(err)
	}
	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(env)
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	repb.RegisterActionCacheServer(grpcServer, actionCacheServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	conn, err := testenv.LocalGRPCConn(ctx, env)
	if err != nil {
		t.Error(err)
	}

	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetActionCacheClient(repb.NewActionCacheClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))

	fcDir := opts.filecacheRootDir
	if *filecacheDir != "" {
		fcDir = *filecacheDir
	} else if fcDir == "" {
		fcDir = testRootDir
	}
	fc, err := filecache.NewFileCache(fcDir, fileCacheSize, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc)

	return env
}

func tempJailerRoot(t *testing.T) string {
	// When running this test on the bare executor pool, ensure the jailer root
	// is under /buildbuddy so that it's on the same device as the executor data
	// dir (with action workspaces and filecache).
	if testfs.Exists(t, "/buildbuddy", "") {
		*testExecutorRoot = "/buildbuddy/test-executor-root"
	}

	if *testExecutorRoot != "" {
		// Clean the root dir before using it, but only once per test. (We don't
		// want to accidentally delete something mid-test that's still in use).
		if !cleaned[t] {
			cleaned[t] = true
			cleanExecutorRoot(t, *testExecutorRoot)
		}
		return *testExecutorRoot
	}

	// NOTE: JailerRoot needs to be < 38 chars long, so can't just use
	// testfs.MakeTempDir(t).
	return testfs.MakeTempSymlink(t, "/tmp", "buildbuddy-*-jailer", testfs.MakeTempDir(t))
}

func getExecutorConfig(t *testing.T) *firecracker.ExecutorConfig {
	cfg, err := firecracker.GetExecutorConfig(context.Background(), tempJailerRoot(t))
	require.NoError(t, err)
	return cfg
}

func TestFirecrackerRunSimple(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	path := filepath.Join(workDir, "world.txt")
	if err := os.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `printf "$GREETING $(cat world.txt)" && printf "foo" >&2`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	expectedResult := &interfaces.CommandResult{
		ExitCode: 0,
		Stdout:   []byte("Hello world"),
		Stderr:   []byte("foo"),
	}

	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	assertCommandResult(t, expectedResult, res)
}

func TestFirecrackerLifecycle(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	path := filepath.Join(workDir, "world.txt")
	if err := os.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `printf "$GREETING $(cat world.txt)" && printf "foo" >&2`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	expectedResult := &interfaces.CommandResult{
		ExitCode: 0,
		Stdout:   []byte("Hello world"),
		Stderr:   []byte("foo"),
	}

	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	cached, err := c.IsImageCached(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !cached {
		if err := c.PullImage(ctx, oci.Credentials{}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Create(ctx, opts.ActionWorkingDirectory); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		log.Debugf("Cleaning up...")
		if err := c.Remove(ctx); err != nil {
			t.Fatal(err)
		}
	})
	res := c.Exec(ctx, cmd, nil)
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	assertCommandResult(t, expectedResult, res)
}

func TestFirecrackerSnapshotAndResume(t *testing.T) {
	// Test for both small and large memory sizes
	for _, memorySize := range []int64{minMemSizeMB, 4000} {
		ctx := context.Background()
		env := getTestEnv(ctx, t, envOpts{})
		env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
		rootDir := testfs.MakeTempDir(t)
		workDir := testfs.MakeDirAll(t, rootDir, "work")

		cfg := getExecutorConfig(t)
		opts := firecracker.ContainerOpts{
			ContainerImage:         busyboxImage,
			ActionWorkingDirectory: workDir,
			VMConfiguration: &fcpb.VMConfiguration{
				NumCpus:            1,
				MemSizeMb:          memorySize,
				EnableNetworking:   false,
				ScratchDiskSizeMb:  100,
				KernelVersion:      cfg.KernelVersion,
				FirecrackerVersion: cfg.FirecrackerVersion,
				GuestApiVersion:    cfg.GuestAPIVersion,
			},
			ExecutorConfig: cfg,
		}
		task := &repb.ExecutionTask{
			Command: &repb.Command{
				// Note: platform must match in order to share snapshots
				Platform: &repb.Platform{Properties: []*repb.Platform_Property{
					{Name: "recycle-runner", Value: "true"},
				}},
				Arguments: []string{"./buildbuddy_ci_runner"},
			},
		}

		c, err := firecracker.NewContainer(ctx, env, task, opts)
		if err != nil {
			t.Fatal(err)
		}

		if err := container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage); err != nil {
			t.Fatalf("unable to pull image: %s", err)
		}

		if err := c.Create(ctx, opts.ActionWorkingDirectory); err != nil {
			t.Fatalf("unable to Create container: %s", err)
		}
		t.Cleanup(func() {
			if err := c.Remove(ctx); err != nil {
				t.Fatal(err)
			}
		})

		cmd := &repb.Command{
			// Run a script that increments /workspace/count (on workspacefs) and
			// /root/count (on scratchfs), or writes 0 if the file doesn't exist.
			// This will let us test whether the scratchfs is sticking around across
			// runs, and whether workspacefs is being correctly reset across runs.
			Arguments: []string{"sh", "-c", `
			for dir in /workspace /root; do
				(
					cd "$dir"
					if [[ -e ./count ]]; then
						count=$(cat ./count)
						count=$((count+1))
						printf "$count" > ./count
					else
						printf 0 > ./count
					fi
					echo "$PWD/count: $(cat count)"
				)
			done
		`},
		}

		res := c.Exec(ctx, cmd, nil /*=stdio*/)
		require.NoError(t, res.Error)

		assert.Equal(t, "/workspace/count: 0\n/root/count: 0\n", string(res.Stdout))

		// Try pause, unpause, exec several times.
		for i := 1; i <= 3; i++ {
			if err := c.Pause(ctx); err != nil {
				t.Fatalf("unable to pause container: %s", err)
			}

			countBefore := rand.Intn(100)
			err := os.WriteFile(filepath.Join(workDir, "count"), []byte(fmt.Sprint(countBefore)), 0644)
			require.NoError(t, err)

			if err := c.Unpause(ctx); err != nil {
				t.Fatalf("unable to unpause container: %s", err)
			}

			res := c.Exec(ctx, cmd, nil /*=stdio*/)
			require.NoError(t, res.Error)

			assert.Equal(t, fmt.Sprintf("/workspace/count: %d\n/root/count: %d\n", countBefore+1, i), string(res.Stdout))
		}
	}
}

func TestFirecracker_LocalSnapshotSharing(t *testing.T) {
	if !*snaputil.EnableLocalSnapshotSharing {
		t.Skip("Snapshot sharing is not enabled")
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	rootDir := testfs.MakeTempDir(t)
	cfg := getExecutorConfig(t)

	var containersToCleanup []*firecracker.FirecrackerContainer
	t.Cleanup(func() {
		for _, vm := range containersToCleanup {
			err := vm.Remove(ctx)
			assert.NoError(t, err)
		}
	})

	workDir := testfs.MakeDirAll(t, rootDir, "work")
	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         minMemSizeMB, // small to make snapshotting faster.
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: cfg,
	}
	task := &repb.ExecutionTask{
		Command: &repb.Command{
			// Note: platform must match in order to share snapshots
			Platform: &repb.Platform{Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
			}},
		},
	}
	baseVM, err := firecracker.NewContainer(ctx, env, task, opts)
	require.NoError(t, err)
	containersToCleanup = append(containersToCleanup, baseVM)
	err = container.PullImageIfNecessary(ctx, env, baseVM, oci.Credentials{}, opts.ContainerImage)
	require.NoError(t, err)
	err = baseVM.Create(ctx, opts.ActionWorkingDirectory)
	require.NoError(t, err)

	// Create a snapshot. Data written to this snapshot should persist
	// when other VMs reuse the snapshot
	cmd := appendToLog("Base")
	res := baseVM.Exec(ctx, cmd, nil /*=stdio*/)
	require.NoError(t, res.Error)
	require.Equal(t, "Base\n", string(res.Stdout))
	err = baseVM.Pause(ctx)
	require.NoError(t, err)

	containers := make([]*firecracker.FirecrackerContainer, 0, 4)
	// Load the same base snapshot from multiple VMs - there should be no
	// corruption or data transfer from snapshot sharing
	for i := 0; i < 4; i++ {
		workDir = testfs.MakeDirAll(t, rootDir, fmt.Sprintf("work-%d", i))
		opts = firecracker.ContainerOpts{
			ContainerImage:         busyboxImage,
			ActionWorkingDirectory: workDir,
			VMConfiguration: &fcpb.VMConfiguration{
				NumCpus:           1,
				MemSizeMb:         minMemSizeMB, // small to make snapshotting faster.
				EnableNetworking:  false,
				ScratchDiskSizeMb: 100,
			},
			ExecutorConfig: cfg,
		}
		forkedVM, err := firecracker.NewContainer(ctx, env, task, opts)
		require.NoError(t, err)
		containers = append(containers, forkedVM)
		containersToCleanup = append(containersToCleanup, forkedVM)

		// The new VM should reuse the Base VM's snapshot, whether we call
		// Create() or Unpause()
		if i%2 == 0 {
			err = forkedVM.Unpause(ctx)
			require.NoError(t, err)
		} else {
			err = forkedVM.Create(ctx, workDir)
			require.NoError(t, err)
		}

		// Write VM-specific data to the log
		cmd = appendToLog(fmt.Sprintf("Fork-%d", i))
		res = forkedVM.Exec(ctx, cmd, nil /*=stdio*/)
		require.NoError(t, res.Error)
		// The log should contain data written to the original snapshot
		// and the current VM, but not from any of the other VMs sharing
		// the same original snapshot
		require.Equal(t, fmt.Sprintf("Base\nFork-%d\n", i), string(res.Stdout))
	}

	// We want to test having multiple VMs start from the same snapshot. Pausing
	// overwrites any pre-existing snapshot, so to ensure all the forked VMs
	// start from the same base snapshot, don't pause them until after all
	// forked VMs have started from the base snapshot
	//
	// Pause multiple VMs simultaneously to test no race conditions / corruption
	// when writing sharable snapshots
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			c := containers[i]
			// Each new VM shouldn't have trouble saving snapshots themselves
			err := c.Pause(ctx)
			require.NoError(t, err)
		}()
	}
	wg.Wait()

	// Test that a new VM uses the newest snapshot

	// Create a snapshot from Fork-3
	c := containers[3]
	err = c.Pause(ctx)
	require.NoError(t, err)

	// A new VM should use the snapshot from Fork-3
	workDir = testfs.MakeDirAll(t, rootDir, "work-last")
	opts = firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         minMemSizeMB, // small to make snapshotting faster.
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: cfg,
	}
	c, err = firecracker.NewContainer(ctx, env, task, opts)
	require.NoError(t, err)
	containersToCleanup = append(containersToCleanup, c)

	// This VM should load the snapshot saved by the last forked VM to Pause
	err = c.Create(ctx, workDir)
	require.NoError(t, err)
	cmd = appendToLog("Last")
	res = c.Exec(ctx, cmd, nil /*=stdio*/)
	require.NoError(t, res.Error)
	require.Equal(t, "Base\nFork-3\nLast\n", string(res.Stdout))

	err = c.Pause(ctx)
	require.NoError(t, err)
}

func TestFirecracker_RemoteSnapshotSharing(t *testing.T) {
	if !*snaputil.EnableRemoteSnapshotSharing {
		t.Skip("Snapshot sharing is not enabled")
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	cfg := getExecutorConfig(t)

	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	filecacheRoot := testfs.MakeDirAll(t, cfg.JailerRoot, "filecache")
	fc, err := filecache.NewFileCache(filecacheRoot, fileCacheSize, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc)

	var containersToCleanup []*firecracker.FirecrackerContainer
	t.Cleanup(func() {
		for _, vm := range containersToCleanup {
			err := vm.Remove(ctx)
			assert.NoError(t, err)
		}
	})

	workDir := testfs.MakeDirAll(t, rootDir, "work")
	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:            1,
			MemSizeMb:          minMemSizeMB, // small to make snapshotting faster.
			EnableNetworking:   false,
			ScratchDiskSizeMb:  100,
			KernelVersion:      cfg.KernelVersion,
			FirecrackerVersion: cfg.FirecrackerVersion,
			GuestApiVersion:    cfg.GuestAPIVersion,
		},
		ExecutorConfig: cfg,
	}
	task := &repb.ExecutionTask{
		Command: &repb.Command{
			// Note: platform must match in order to share snapshots
			Platform: &repb.Platform{Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
			}},
			Arguments: []string{"./buildbuddy_ci_runner"},
		},
	}
	baseVM, err := firecracker.NewContainer(ctx, env, task, opts)
	require.NoError(t, err)
	containersToCleanup = append(containersToCleanup, baseVM)
	err = container.PullImageIfNecessary(ctx, env, baseVM, oci.Credentials{}, opts.ContainerImage)
	require.NoError(t, err)
	err = baseVM.Create(ctx, opts.ActionWorkingDirectory)
	require.NoError(t, err)
	baseSnapshotId := baseVM.SnapshotID()

	// Create a snapshot. Data written to this snapshot should persist
	// when other VMs reuse the snapshot
	cmd := appendToLog("Base")
	res := baseVM.Exec(ctx, cmd, nil /*=stdio*/)
	require.NoError(t, res.Error)
	require.Equal(t, "Base\n", string(res.Stdout))
	require.NotEmpty(t, res.VMMetadata.GetSnapshotId())
	err = baseVM.Pause(ctx)
	require.NoError(t, err)

	// Start a VM from the snapshot. Artifacts should be stored locally in the filecache
	workDirForkLocalFetch := testfs.MakeDirAll(t, rootDir, "work-fork-local-fetch")
	opts = firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDirForkLocalFetch,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         minMemSizeMB, // small to make snapshotting faster.
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: cfg,
	}
	forkedVM, err := firecracker.NewContainer(ctx, env, task, opts)
	require.NoError(t, err)
	containersToCleanup = append(containersToCleanup, forkedVM)
	err = forkedVM.Unpause(ctx)
	require.NoError(t, err)
	cmd = appendToLog("Fork local fetch")
	res = forkedVM.Exec(ctx, cmd, nil /*=stdio*/)
	require.NoError(t, res.Error)
	// The log should contain data written to the original snapshot
	// and the current VM
	require.Equal(t, "Base\nFork local fetch\n", string(res.Stdout))
	require.NotEmpty(t, res.VMMetadata.GetSnapshotId())
	err = forkedVM.Pause(ctx)
	require.NoError(t, err)

	// Clear the local filecache. Vms should still be able to unpause the snapshot
	// by pulling artifacts from the remote cache
	err = os.RemoveAll(filecacheRoot)
	require.NoError(t, err)
	filecacheRoot2 := testfs.MakeDirAll(t, cfg.JailerRoot, "filecache2")
	fc2, err := filecache.NewFileCache(filecacheRoot2, fileCacheSize, false)
	require.NoError(t, err)
	fc2.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc2)

	// Start a VM from the snapshot.
	workDirForkRemoteFetch := testfs.MakeDirAll(t, rootDir, "work-fork-remote-fetch")
	opts = firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDirForkRemoteFetch,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         minMemSizeMB, // small to make snapshotting faster.
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: cfg,
	}
	forkedVM2, err := firecracker.NewContainer(ctx, env, task, opts)
	require.NoError(t, err)
	containersToCleanup = append(containersToCleanup, forkedVM2)
	err = forkedVM2.Unpause(ctx)
	require.NoError(t, err)
	cmd = appendToLog("Fork remote fetch")
	res = forkedVM2.Exec(ctx, cmd, nil /*=stdio*/)
	require.NoError(t, res.Error)
	// The log should contain data written to the most recent snapshot
	require.Equal(t, "Base\nFork local fetch\nFork remote fetch\n", string(res.Stdout))
	require.NotEmpty(t, res.VMMetadata.GetSnapshotId())

	// Should still be able to start from the original snapshot if we use
	// a snapshot key containing the original VM's snapshot ID
	workDirForkOriginalSnapshot := testfs.MakeDirAll(t, rootDir, "work-fork-og-snapshot")
	originalSnapshotKey := baseVM.SnapshotKeySet().GetBranchKey()
	originalSnapshotKey.SnapshotId = baseSnapshotId
	opts = firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDirForkOriginalSnapshot,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         minMemSizeMB, // small to make snapshotting faster.
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: cfg,
		SavedState:     &rnpb.FirecrackerState{SnapshotKey: originalSnapshotKey},
	}
	ogFork, err := firecracker.NewContainer(ctx, env, task, opts)
	require.NoError(t, err)
	containersToCleanup = append(containersToCleanup, ogFork)
	err = ogFork.Unpause(ctx)
	require.NoError(t, err)
	cmd = appendToLog("Fork from original vm")
	res = ogFork.Exec(ctx, cmd, nil /*=stdio*/)
	require.NoError(t, res.Error)
	// The log should contain data written to the original snapshot
	// and the current VM, but not from any of the other VMs, including the master
	// snapshot
	require.Equal(t, "Base\nFork from original vm\n", string(res.Stdout))
	require.NotEmpty(t, res.VMMetadata.GetSnapshotId())
}

func TestFirecracker_RemoteSnapshotSharing_RemoteInstanceName(t *testing.T) {
	if !*snaputil.EnableRemoteSnapshotSharing {
		t.Skip("Snapshot sharing is not enabled")
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	cfg := getExecutorConfig(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	// Set up a task with remote snapshot sharing enabled.
	task := &repb.ExecutionTask{
		ExecuteRequest: &repb.ExecuteRequest{
			InstanceName: "",
		},
		Command: &repb.Command{
			// Enable remote snapshotting
			Arguments: []string{"./buildbuddy_ci_runner"},
			Platform: &repb.Platform{Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
			}},
		},
	}
	workdir := testfs.MakeTempDir(t)
	opts := firecracker.ContainerOpts{
		ExecutorConfig: cfg,
		// The image name here matters - the issue which originally prompted
		// this test didn't reproduce on busybox, likely because the image is
		// smaller and disk chunks were more likely to be dirtied.
		ContainerImage: platform.Ubuntu20_04WorkflowsImage,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         minMemSizeMB,
			ScratchDiskSizeMb: 100,
		},
		ActionWorkingDirectory: workdir,
	}

	// This command just increments the current value in a counter file
	// "/root/attempts" (if missing, defaults to 0), then prints the new value.
	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
cd /root
ATTEMPT_NUMBER=$(cat ./attempts 2>/dev/null || echo 0)
ATTEMPT_NUMBER=$(( ATTEMPT_NUMBER + 1 ))
printf '%s' $ATTEMPT_NUMBER | tee ./attempts
`}}

	run := func(instanceName string, expectedLogs string) {
		task.ExecuteRequest.InstanceName = instanceName
		c, err := firecracker.NewContainer(ctx, env, task, opts)
		require.NoError(t, err)
		container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
		err = c.Create(ctx, workdir)
		require.NoError(t, err)
		res := c.Exec(ctx, cmd, nil)
		// Make sure we pause before doing any other assertions,
		// since this also cleans up the VM.
		{
			err = c.Pause(ctx)
			require.NoError(t, err)
		}
		require.Empty(t, string(res.Stderr))
		require.Equal(t, 0, res.ExitCode)
		require.NoError(t, res.Error)
		assert.Equal(t, expectedLogs, string(res.Stdout))
	}

	run("", "1")  // Should start clean
	run("A", "1") // New instance name "A"; should start clean
	run("A", "2") // Should resume from previous, and increment the counter
}

func TestFirecracker_LocalSnapshotSharing_ContainerImageChunksExpiredFromCache(t *testing.T) {
	if !*snaputil.EnableRemoteSnapshotSharing {
		t.Skip("Snapshot sharing is not enabled")
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	cfg := getExecutorConfig(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	// Set up a task with only local snapshotting enabled.
	task := &repb.ExecutionTask{
		Command: &repb.Command{
			Platform: &repb.Platform{Properties: []*repb.Platform_Property{
				{Name: "recycle-runner", Value: "true"},
			}},
		},
	}
	workdir := testfs.MakeTempDir(t)
	opts := firecracker.ContainerOpts{
		ExecutorConfig: cfg,
		ContainerImage: platform.Ubuntu20_04WorkflowsImage,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         minMemSizeMB,
			ScratchDiskSizeMb: 100,
		},
		ActionWorkingDirectory: workdir,
	}

	// This command just increments the current value in a counter file
	// "/root/attempts" (if missing, defaults to 0), then prints the new value.
	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
cd /root
ATTEMPT_NUMBER=$(cat ./attempts 2>/dev/null || echo 0)
ATTEMPT_NUMBER=$(( ATTEMPT_NUMBER + 1 ))
printf '%s' $ATTEMPT_NUMBER | tee ./attempts
`}}

	run := func(expectedLogs string) {
		c, err := firecracker.NewContainer(ctx, env, task, opts)
		require.NoError(t, err)
		container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
		err = c.Create(ctx, workdir)
		require.NoError(t, err)
		res := c.Exec(ctx, cmd, nil)
		// Make sure we pause before doing any other assertions,
		// since this also cleans up the VM.
		{
			err = c.Pause(ctx)
			require.NoError(t, err)
		}
		require.Empty(t, string(res.Stderr))
		require.Equal(t, 0, res.ExitCode)
		require.NoError(t, res.Error)
		assert.Equal(t, expectedLogs, string(res.Stdout))
	}

	run("1") // Should start clean

	// Evict all artifacts from filecache, which should expire the base image.
	fcDir := testfs.MakeTempDir(t)
	fc, err := filecache.NewFileCache(fcDir, fileCacheSize, false)
	require.NoError(t, err)
	fc.WaitForDirectoryScanToComplete()
	env.SetFileCache(fc)

	run("1") // Should start clean
	run("2") // Should resume from previous, and increment the counter
}

func TestFirecrackerSnapshotVersioning(t *testing.T) {
	if !*snaputil.EnableLocalSnapshotSharing {
		t.SkipNow()
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	task := &repb.ExecutionTask{
		Command: &repb.Command{Platform: &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "recycle-runner", Value: "true"},
		}}},
	}

	opts := firecracker.ContainerOpts{
		ExecutorConfig:         getExecutorConfig(t),
		ActionWorkingDirectory: workDir,
		VMConfiguration:        &fcpb.VMConfiguration{},
	}

	// Two containers with same executorConfig: snapshot keys should be equal.
	c1, err := firecracker.NewContainer(ctx, env, task, opts)
	require.NoError(t, err)
	c2, err := firecracker.NewContainer(ctx, env, task, opts)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(c1.SnapshotKeySet(), c2.SnapshotKeySet(), protocmp.Transform()))

	// Change guest API version; snapshot keys should not be equal.
	opts.ExecutorConfig.GuestAPIVersion = "something-else"
	c3, err := firecracker.NewContainer(ctx, env, task, opts)
	require.NoError(t, err)
	require.NotEmpty(t, cmp.Diff(c1.SnapshotKeySet(), c3.SnapshotKeySet(), protocmp.Transform()))
}

func TestFirecrackerComplexFileMapping(t *testing.T) {
	numFiles := 100
	fileSizeBytes := int64(1_000_000)
	scratchTestFileSizeBytes := int64(50_000_000)
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})

	rootDir := testfs.MakeTempDir(t)
	subDirs := []string{"a", "b", "c", "d", "e"}
	files := make([]string, 0, numFiles)

	for i := 0; i < numFiles; i++ {
		rand.Shuffle(len(subDirs), func(i, j int) {
			subDirs[i], subDirs[j] = subDirs[j], subDirs[i]
		})
		pathDirs := append([]string{rootDir}, subDirs[:rand.Intn(len(subDirs)-1)+1]...)
		parentDir := filepath.Join(pathDirs...)
		if err := disk.EnsureDirectoryExists(parentDir); err != nil {
			t.Fatal(err)
		}
		r, buf := testdigest.RandomCASResourceBuf(t, fileSizeBytes)
		fullPath := filepath.Join(parentDir, r.GetDigest().GetHash()+".txt")
		if err := os.WriteFile(fullPath, buf, 0660); err != nil {
			t.Fatal(err)
		}
	}

	workspaceDirSize, err := disk.DirSize(rootDir)
	require.NoError(t, err)

	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			find -name '*.txt' -exec cp {} {}.out \;
			</dev/zero head -c ` + fmt.Sprint(scratchTestFileSizeBytes) + ` > ~/scratch_file.txt
			# Sleep a bit to ensure we get a good disk usage sample.
			sleep 1
		`},
	}
	expectedResult := &interfaces.CommandResult{
		ExitCode: 0,
		Stdout:   nil,
		Stderr:   nil,
	}
	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: rootDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         minMemSizeMB,
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	if res.Error != nil {
		t.Fatalf("error: %s", res.Error)
	}

	// Check that the result has the expected disk usage stats.
	assert.True(t, res.UsageStats != nil)
	var workspaceFSU, rootFSU *repb.UsageStats_FileSystemUsage
	for _, fsu := range res.UsageStats.PeakFileSystemUsage {
		if fsu.Target == "/workspace" {
			workspaceFSU = fsu
		} else if fsu.Target == "/" {
			rootFSU = fsu
		}
	}

	const workspaceDiskSlackSpaceBytes = 2_000_000_000
	if workspaceFSU == nil {
		assert.Fail(t, "/workspace disk usage was not reported")
	} else {
		expectedWorkspaceDev := "/dev/vdc"
		if *firecracker.EnableRootfs {
			expectedWorkspaceDev = "/dev/vdb"
		}

		assert.Equal(t, "ext4", workspaceFSU.GetFstype())
		assert.True(t, workspaceFSU.GetSource() == expectedWorkspaceDev,
			"unexpected workspace mount source %s", workspaceFSU.GetSource(),
		)
		assert.InDelta(
			t, workspaceFSU.GetUsedBytes(),
			// Expected size is twice the input size, since we duplicate the inputs.
			2*workspaceDirSize,
			10_000_000,
			"used workspace disk size")
		expectedTotalSize := ext4.MinDiskImageSizeBytes + workspaceDirSize + workspaceDiskSlackSpaceBytes
		assert.InDelta(
			t, expectedTotalSize, workspaceFSU.GetTotalBytes(),
			60_000_000,
			"total workspace disk size")
	}
	if rootFSU == nil {
		assert.Fail(t, "root (/) disk usage was not reported")
	} else if *firecracker.EnableRootfs {
		assert.Equal(t, "ext4", rootFSU.GetFstype())
		assert.Equal(t, "/dev/vda", rootFSU.GetSource())
	} else {
		assert.Equal(t, "overlay", rootFSU.GetFstype())
		assert.Equal(t, "overlayfs:/scratch/bbvmroot", rootFSU.GetSource())
		const approxInitialScratchDiskSizeBytes = 38e6
		assert.InDelta(
			t, approxInitialScratchDiskSizeBytes+scratchTestFileSizeBytes,
			rootFSU.GetUsedBytes(),
			20_000_000,
			"used scratch disk size")
		assert.InDelta(
			t, 1_000_000*opts.VMConfiguration.ScratchDiskSizeMb+ext4.MinDiskImageSizeBytes+approxInitialScratchDiskSizeBytes,
			rootFSU.GetTotalBytes(),
			20_000_000,
			"total scratch disk size")
	}

	assertCommandResult(t, expectedResult, res)

	for _, fullPath := range files {
		if exists, err := disk.FileExists(ctx, fullPath); err != nil || !exists {
			t.Fatalf("File %q not found in workspace.", fullPath)
		}
		if exists, err := disk.FileExists(ctx, fullPath+".out"); err != nil || !exists {
			t.Fatalf("File %q not found in workspace.", fullPath)
		}
	}
}

func TestFirecrackerRunWithNetwork(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	// Make sure the container can send packets to something external of the VM
	googleDNS := "8.8.8.8"
	cmd := &repb.Command{Arguments: []string{"ping", "-c1", googleDNS}}

	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			EnableNetworking:  true,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	assert.Equal(t, 0, res.ExitCode)
	assert.Contains(t, string(res.Stdout), "64 bytes from "+googleDNS)
}

func TestFirecrackerRun_ReapOrphanedZombieProcess(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	// Write a helper script that prints process info for a pid.
	// The ":1" disables whitespace padding in the printed column values.
	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"procinfo": `#!/usr/bin/env sh
			exec ps -p "$1" -o pid:1,ppid:1,state:1,comm:1 --no-headers
		`,
	})
	testfs.MakeExecutable(t, workDir, "procinfo")

	// Run a shell subprocess that spawns a "sleep 1" child process in the
	// background, then exits immediately.
	// The sleep process should be orphaned once the parent shell exits,
	// then reparented to pid 1 (init).
	// Once the sleep process exits, it should be reaped by the init process.

	cmd := &repb.Command{
		Arguments: []string{"bash", "-e", "-c", `
			sh -c '
				sleep 0.1 &
				printf "%s" "$!" > sleep.pid

				echo "Before reparent:"
				./procinfo "$(cat sleep.pid)"
			' &
			printf "%s" "$!" > sh.pid
			wait

			echo "After reparent:"
			./procinfo "$(cat sleep.pid)"

			sleep 0.2
			echo "After exit:"
			# Note: procinfo is expected to fail here.
			./procinfo "$(cat sleep.pid)" || true
		`},
	}

	opts := firecracker.ContainerOpts{
		// Use an ubuntu image since busybox doesn't support the `ps` options
		// we need in the procinfo helper script.
		ContainerImage:         ubuntuImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	assert.Empty(t, string(res.Stderr))
	require.Equal(t, 0, res.ExitCode)

	initPID := 1
	shPID := testfs.ReadFileAsString(t, opts.ActionWorkingDirectory, "sh.pid")
	sleepPID := testfs.ReadFileAsString(t, opts.ActionWorkingDirectory, "sleep.pid")

	// Note, state codes are documented here:
	// https://man7.org/linux/man-pages/man1/ps.1.html#PROCESS_STATE_CODES

	expectedOutput := "Before reparent:\n" +
		// Just after starting, the sleep process should be in state "S"
		// (sleeping) and still parented to the sh process that spawned it.
		fmt.Sprintf("%s %s S sleep\n", sleepPID, shPID) +
		"After reparent:\n" +
		// After the sh process exits it should have been reparented to pid 1
		// (init) and still in sleeping state.
		fmt.Sprintf("%s %d S sleep\n", sleepPID, initPID) +
		"After exit:\n" +
		// ps output should be empty after the sleep proces exits.
		// If it shows state "Z" ("zombie"), it wasn't properly reaped.
		""

	assert.Equal(t, expectedOutput, string(res.Stdout))
}

func TestFirecrackerNonRoot(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	ws, err := workspace.New(env, rootDir, &workspace.Opts{NonrootWritable: true})
	require.NoError(t, err)
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			# print out the uid/gid
			id

			# make sure the workspace root dir is writable
			touch foo || exit 1
			# make sure output directories are writable too
			touch outputs/bar || exit 1
			touch nested/outputs/baz || exit 1
		`},
		OutputDirectories: []string{"outputs", "nested/outputs"},
	}
	ws.SetTask(ctx, &repb.ExecutionTask{Command: cmd})
	err = ws.CreateOutputDirs()
	require.NoError(t, err)

	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		User:                   "nobody",
		ActionWorkingDirectory: ws.Path(),
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         1000,
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	require.NoError(t, res.Error)
	require.Empty(t, string(res.Stderr))
	require.Equal(t, 0, res.ExitCode)
	require.Regexp(t, regexp.MustCompile(`uid=[0-9]+\(nobody\) gid=[0-9]+\(nobody\)`), string(res.Stdout))
}

func TestFirecrackerRunNOPWithZeroDisk(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	cmd := &repb.Command{Arguments: []string{"pwd"}}
	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:          1,
			MemSizeMb:        2500,
			EnableNetworking: false,
			// Request 0 disk; implementation should ensure the disk is at least as big
			// as is required to run a NOP command. Otherwise, users might have to
			// keep on top of our min disk requirements which is not really feasible.
			ScratchDiskSizeMb: 0,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, "", string(res.Stderr))
	assert.Equal(t, "/workspace\n", string(res.Stdout))
}

func TestFirecrackerRunWithDockerOverUDS(t *testing.T) {
	if *skipDockerTests {
		t.Skip()
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			set -e

			# Discard pull output to make the output deterministic
			docker pull ` + busyboxImage + ` &>/dev/null

			# Try running a few commands
			docker run --rm ` + busyboxImage + ` echo Hello
			docker run --rm ` + busyboxImage + ` echo world

			# Check what storage driver docker is using
			docker info 2>/dev/null | grep 'Storage Driver'
		`},
	}

	opts := firecracker.ContainerOpts{
		ContainerImage:         imageWithDockerInstalled,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			EnableNetworking:  true,
			InitDockerd:       true,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	assert.Equal(t, 0, res.ExitCode)
	expectedStorageDriver := "vfs"
	if *firecracker.EnableRootfs {
		expectedStorageDriver = "overlay2"
	}
	assert.Equal(t, "Hello\nworld\n Storage Driver: "+expectedStorageDriver+"\n", string(res.Stdout), "stdout should contain pwd output")
	assert.Equal(t, "", string(res.Stderr), "stderr should be empty")
}

func TestFirecrackerRunWithDockerOverTCP(t *testing.T) {
	if *skipDockerTests {
		t.Skip()
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			set -e
			# Discard pull output to make the output deterministic
			docker -H tcp://127.0.0.1:2375 pull ` + busyboxImage + ` &>/dev/null

			# Try running a few commands
			docker -H tcp://127.0.0.1:2375 run --rm ` + busyboxImage + ` echo Hello
			docker -H tcp://127.0.0.1:2375 run --rm ` + busyboxImage + ` echo world
		`},
	}

	opts := firecracker.ContainerOpts{
		ContainerImage:         imageWithDockerInstalled,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			EnableNetworking:  true,
			InitDockerd:       true,
			ScratchDiskSizeMb: 100,
			EnableDockerdTcp:  true,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, "Hello\nworld\n", string(res.Stdout), "stdout should contain pwd output")
	assert.Equal(t, "", string(res.Stderr), "stderr should be empty")
}

func TestFirecrackerRunWithDockerOverTCPDisabled(t *testing.T) {
	if *skipDockerTests {
		t.Skip()
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			set -e
			# Discard pull output to make the output deterministic
			docker -H tcp://127.0.0.1:2375 pull ` + busyboxImage + ` &>/dev/null
		`},
	}

	opts := firecracker.ContainerOpts{
		ContainerImage:         imageWithDockerInstalled,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			EnableNetworking:  true,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	assert.NotEqual(t, 0, res.ExitCode)
}

func TestFirecrackerExecWithRecycledWorkspaceWithNewContents(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"test1.sh": "echo Hello; echo world > /root/should_be_persisted.txt",
	})

	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			ScratchDiskSizeMb: 2000,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)
	err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
	require.NoError(t, err)
	err = c.Create(ctx, opts.ActionWorkingDirectory)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(ctx)
		assert.NoError(t, err)
	})
	res := c.Exec(ctx, &repb.Command{Arguments: []string{"sh", "test1.sh"}}, nil /*=stdio*/)
	require.NoError(t, res.Error)
	require.Equal(t, "", string(res.Stderr))
	require.Equal(t, "Hello\n", string(res.Stdout))

	err = c.Pause(ctx)
	require.NoError(t, err)

	// While we're paused, write new workspace contents.
	// Then unpause and execute a command that requires the new contents.
	err = os.Remove(filepath.Join(workDir, "test1.sh"))
	require.NoError(t, err)
	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"test2.sh": "cat /root/should_be_persisted.txt",
	})

	err = c.Unpause(ctx)
	require.NoError(t, err)

	res = c.Exec(ctx, &repb.Command{Arguments: []string{"sh", "test2.sh"}}, nil /*=stdio*/)

	require.NoError(t, res.Error)
	require.Equal(t, "", string(res.Stderr))
	require.Equal(t, "world\n", string(res.Stdout))

	err = c.Pause(ctx)
	require.NoError(t, err)

	// Try resuming again, to test resuming from a snapshot of a VM which was
	// itself loaded from snapshot.
	err = os.Remove(filepath.Join(workDir, "test2.sh"))
	require.NoError(t, err)
	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"test3.sh": "echo world",
	})

	err = c.Unpause(ctx)
	require.NoError(t, err)

	res = c.Exec(ctx, &repb.Command{Arguments: []string{"sh", "test3.sh"}}, nil /*=stdio*/)

	require.NoError(t, res.Error)
	require.Equal(t, "", string(res.Stderr))
	require.Equal(t, "world\n", string(res.Stdout))

	err = c.Pause(ctx)
	require.NoError(t, err)
}

func TestFirecrackerExecWithRecycledWorkspaceWithDocker(t *testing.T) {
	if *skipDockerTests {
		t.Skip()
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"test1.sh": "echo Hello; echo world > /root/should_be_persisted.txt",
	})

	opts := firecracker.ContainerOpts{
		ContainerImage:         imageWithDockerInstalled,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			ScratchDiskSizeMb: 4000, // 4 GB
			EnableNetworking:  true,
			InitDockerd:       true,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)
	err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
	require.NoError(t, err)
	err = c.Create(ctx, opts.ActionWorkingDirectory)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(ctx)
		assert.NoError(t, err)
	})

	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			set -e
			# Discard pull output to make the output deterministic
			docker pull ` + ubuntuImage + ` 2>&1 >/dev/null

			# Try running a few commands
			docker run --rm ` + ubuntuImage + ` echo Hello
			docker run --rm ` + ubuntuImage + ` echo world

			# Write some output to the workspace to be preserved
			touch preserves.txt
		`},
		OutputFiles: []string{"preserves.txt"},
	}
	res := c.Exec(ctx, cmd, nil /*=stdio*/)
	require.NoError(t, res.Error)
	require.Equal(t, "", string(res.Stderr))
	require.Equal(t, "Hello\nworld\n", string(res.Stdout))

	err = c.Pause(ctx)
	require.NoError(t, err)

	// Check that we copied the output file to the host workspace.
	_, err = os.Stat(filepath.Join(workDir, "preserves.txt"))
	require.NoError(t, err)

	// While we're paused, write new workspace contents.
	// Then unpause and execute a command that requires the new contents.
	err = os.Remove(filepath.Join(workDir, "test1.sh"))
	require.NoError(t, err)

	testfs.WriteAllFileContents(t, workDir, map[string]string{
		"test2.sh": `
		set -e

		# Make sure we can read the output from the previous command
		stat preserves.txt >/dev/null

		# Make sure we can run docker images from cache
		docker run --rm ` + ubuntuImage + ` echo world
		`,
	})

	start := time.Now()
	err = c.Unpause(ctx)
	require.NoError(t, err)

	res = c.Exec(ctx, &repb.Command{Arguments: []string{"sh", "test2.sh"}}, nil /*=stdio*/)

	log.Debugf("Resumed VM and executed docker-in-firecracker command in %s", time.Since(start))

	require.NoError(t, res.Error)
	require.Equal(t, "", string(res.Stderr))
	require.Equal(t, "world\n", string(res.Stdout))
	require.Equal(t, 0, res.ExitCode)

	err = c.Pause(ctx)
	require.NoError(t, err)
}

func TestFirecrackerExecWithDockerFromSnapshot(t *testing.T) {
	if *skipDockerTests {
		t.Skip()
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	opts := firecracker.ContainerOpts{
		ContainerImage:         imageWithDockerInstalled,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			InitDockerd:       true,
			EnableNetworking:  true,
			ScratchDiskSizeMb: 1000,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	if err := container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage); err != nil {
		t.Fatalf("unable to pull image: %s", err)
	}

	if err := c.Create(ctx, opts.ActionWorkingDirectory); err != nil {
		t.Fatalf("unable to Create container: %s", err)
	}
	t.Cleanup(func() {
		if err := c.Remove(ctx); err != nil {
			t.Fatal(err)
		}
	})

	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			set -e
			# Discard pull output to make the output deterministic
			docker pull ` + busyboxImage + ` &>/dev/null

			docker run --rm ` + busyboxImage + ` echo Hello
		`},
	}

	res := c.Exec(ctx, cmd, nil /*=stdio*/)

	require.NoError(t, res.Error)
	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, "Hello\n", string(res.Stdout), "stdout should contain expected output")
	assert.Equal(t, "", string(res.Stderr), "stderr should be empty")

	if err := c.Pause(ctx); err != nil {
		t.Fatalf("unable to pause container: %s", err)
	}
	if err := c.Unpause(ctx); err != nil {
		t.Fatalf("unable to unpause container: %s", err)
	}

	cmd = &repb.Command{
		Arguments: []string{"bash", "-c", `
		  # Note: Image should be cached from previous command.
			docker run --rm ` + busyboxImage + ` echo world
		`},
	}

	res = c.Exec(ctx, cmd, nil /*=stdio*/)

	require.NoError(t, res.Error)
	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, "world\n", string(res.Stdout), "stdout should contain expected output")
	assert.Equal(t, "", string(res.Stderr), "stderr should be empty")
}

// NOTE: These Timeout tests are somewhat slow and may be a bit flaky because
// they rely on timing of events that is difficult to coordinate with the VM. If
// we ever change this to run on CI, we probably want to move this to a manual
// test, or figure out some way to determine that the VM has started the
// `sleep` command.
func TestFirecrackerRun_Timeout_DebugOutputIsAvailable(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)

	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		echo stdout >&1
		echo stderr >&2
		echo output > output.txt
		sleep infinity
	`}}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})

	require.True(
		t, status.IsDeadlineExceededError(res.Error),
		"expected DeadlineExceeded, but got: %s", res.Error)
	assert.Equal(
		t, "stdout\n", string(res.Stdout),
		"should get partial stdout if the exec times out")
	assert.Equal(
		t, "stderr\n", string(res.Stderr),
		"should get partial stderr if the exec times out")
	out := testfs.ReadFileAsString(t, workDir, "output.txt")
	assert.Equal(
		t, "output\n", out,
		"should get partial output files even if the exec times out")
}

func TestFirecrackerExec_Timeout_DebugOutputIsAvailable(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)
	err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
	require.NoError(t, err)
	err = c.Create(ctx, opts.ActionWorkingDirectory)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(context.Background())
		require.NoError(t, err)
	})

	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		echo output > output.txt
		echo stdout >&1
		# Wait a little bit for stdout to be flushed.
		sleep 1

		# Send a message on stderr to let the test know we're done.
		echo stderr >&2

		sleep infinity
	`}}

	stderrReader, stderrWriter := io.Pipe()
	// Use an exec context that is canceled as soon as we get a message on
	// stderr, simulating a timeout.
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()
		expectedStderr := "stderr\n"
		b := make([]byte, len(expectedStderr))
		_, err := io.ReadFull(stderrReader, b)
		require.NoError(t, err)
		require.Equal(t, expectedStderr, string(b))
	}()

	res := c.Exec(ctx, cmd, &interfaces.Stdio{
		// Write stderr to the pipe but buffer stdout in the result as usual.
		Stderr: stderrWriter,
	})

	require.True(
		t, status.IsCanceledError(res.Error),
		"expected Canceled, but got: %s", res.Error)
	assert.Equal(
		t, "stdout\n", string(res.Stdout),
		"should get partial stdout if the exec times out")
	out := testfs.ReadFileAsString(t, workDir, "output.txt")
	assert.Equal(
		t, "output\n", out,
		"should get partial output files even if the exec times out")
}

func TestFirecrackerLargeResult(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)
	const stdoutSize = 10_000_000
	cmd := &repb.Command{Arguments: []string{"sh", "-c", fmt.Sprintf(`yes | head -c %d`, stdoutSize)}}
	res := c.Run(ctx, cmd, workDir, oci.Credentials{})

	require.NoError(t, res.Error)
	assert.Equal(t, string(res.Stderr), "")
	assert.Len(t, res.Stdout, stdoutSize)
}

func TestFirecrackerWithExecutorRestart(t *testing.T) {
	ctx := context.Background()

	// Make sure we use the same filecache and disk root across restarts
	testRoot := tempJailerRoot(t)
	err := os.RemoveAll(filepath.Join(testRoot, "_runner_pool_state.bin"))
	require.NoError(t, err)
	filecacheRoot := testfs.MakeDirAll(t, testRoot, "filecache")
	diskCacheRoot := testfs.MakeTempDir(t)

	var (
		env    *testenv.TestEnv
		ta     *testauth.TestAuthenticator
		pool   interfaces.RunnerPool
		ctxUS1 context.Context
	)
	setup := func() {
		var err error
		env = getTestEnv(ctx, t, envOpts{filecacheRootDir: filecacheRoot, cacheRootDir: diskCacheRoot})
		flags.Set(t, "executor.enable_firecracker", true)
		// Jailer root dir needs to be < 38 chars
		flags.Set(t, "executor.root_directory", testRoot)
		ta = testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1"))
		env.SetAuthenticator(ta)
		pool, err = runner.NewPool(env, &runner.PoolOptions{})
		require.NoError(t, err)
		ctxUS1, err = ta.WithAuthenticatedUser(ctx, "US1")
		require.NoError(t, err)
	}

	setup()

	smd := &scpb.SchedulingMetadata{
		TaskSize: &scpb.TaskSize{
			EstimatedMemoryBytes: 1_000_000_000,
			EstimatedMilliCpu:    1_000,
		},
	}
	commonProps := []*repb.Platform_Property{
		{Name: "recycle-runner", Value: "true"},
		{Name: "workload-isolation-type", Value: "firecracker"},
		{Name: "container-image", Value: "docker://" + busyboxImage},
		// TODO: Test setting preserve-workspace=true when resuming from
		// persisted state. This doesn't matter a whole lot for workflows in
		// particular, because we don't use the workspace and instead override
		// the working dir to /root/workspace
	}

	task1 := &repb.ScheduledTask{
		ExecutionTask: &repb.ExecutionTask{
			Command: &repb.Command{
				Arguments: []string{"sh", "-c", "printf foo > /root/KEEP"},
				Platform:  &repb.Platform{Properties: commonProps},
			},
		},
		SchedulingMetadata: smd,
	}

	r, err := pool.Get(ctxUS1, task1)
	require.NoError(t, err)

	res := r.Run(ctxUS1)
	require.NoError(t, res.Error)

	finishedCleanly := true
	pool.TryRecycle(ctxUS1, r, finishedCleanly)

	// Simulate executor shutdown.
	err = pool.Shutdown(ctx)
	require.NoError(t, err)

	// Now re-initialize everything, simulating an executor restart. The pool
	// should restore the saved snapshot state. Do this a couple of times for
	// good measure.
	for i := 0; i < 3; i++ {
		setup()

		task2 := &repb.ScheduledTask{
			ExecutionTask: &repb.ExecutionTask{
				Command: &repb.Command{
					Arguments: []string{"sh", "-c", "cat /root/KEEP 2>&1"},
					Platform:  &repb.Platform{Properties: commonProps},
				},
			},
			SchedulingMetadata: smd,
		}

		r, err = pool.Get(ctxUS1, task2)
		require.NoError(t, err)

		log.Infof("Running task...")

		res = r.Run(ctxUS1)
		require.NoError(t, res.Error)

		// Should be able to read /root/KEEP from the previous run.
		require.Equal(t, "foo", string(res.Stdout))

		finishedCleanly := true
		pool.TryRecycle(ctxUS1, r, finishedCleanly)

		err = pool.Shutdown(ctx)
		require.NoError(t, err)
	}
}

func TestMergeDiffSnapshot(t *testing.T) {
	for _, cow := range []bool{false, true} {
		t.Run(fmt.Sprintf("COW=%v", cow), func(t *testing.T) {
			testMergeDiffSnapshot(t, cow)
		})
	}
}

func testMergeDiffSnapshot(t *testing.T, cow bool) {
	tmp := testfs.MakeTempDir(t)
	for i := 0; i < 100; i++ {
		copy_on_write.ResetMmmapedBytesMetricForTest()

		ctx := context.Background()
		snapSize := 1e6 + rand.Int63n(1e6)
		// Make a buffer to keep track of our expected bytes.
		b := make([]byte, snapSize)
		// Create a base snapshot with some random data pages.
		// Regions which are not written are holes, in order to test that
		// we handle holes in the base snapshot properly.
		basePath := filepath.Join(tmp, fmt.Sprintf("%d.base", i))
		{
			bf, err := os.Create(basePath)
			require.NoError(t, err)
			err = bf.Truncate(snapSize)
			require.NoError(t, err)
			writeRandomPages(t, bf, rand.Intn(100), b)
			err = bf.Close()
			require.NoError(t, err)
		}

		// Sanity check: base snapshot file contents should match our expected
		// buffer contents.
		baseBytes, err := os.ReadFile(basePath)
		require.NoError(t, err)
		if !bytes.Equal(baseBytes, b) {
			assert.FailNowf(t, "Base file bytes are not equal to expected bytes", "")
		}

		// Write some random diffs to the diff snapshot.
		// Regions which are not written are holes, in order to test that
		// we handle holes in the diff snapshot properly.
		diffPath := filepath.Join(tmp, fmt.Sprintf("%d.diff", i))
		{
			df, err := os.Create(diffPath)
			require.NoError(t, err)
			err = df.Truncate(snapSize)
			require.NoError(t, err)
			writeRandomPages(t, df, rand.Intn(100), b)
			err = df.Close()
			require.NoError(t, err)
		}
		var store *copy_on_write.COWStore
		cowDirName := fmt.Sprintf("cow-%d", i)
		if cow {
			env := getTestEnv(ctx, t, envOpts{})
			dataDir := testfs.MakeDirAll(t, tmp, cowDirName)
			c, err := copy_on_write.ConvertFileToCOW(ctx, env, basePath, 4096*16, dataDir, "", false /*=remoteEnabled*/)
			require.NoError(t, err)
			store = c
		}
		err = firecracker.MergeDiffSnapshot(ctx, basePath, store, diffPath, 4 /*=concurrency*/, 4096 /*=bufSize*/)
		require.NoError(t, err)

		var merged []byte
		if cow {
			// All bytes should have been unmapped afterwards, even though
			// we haven't closed the COWStore yet.
			mmappedBytes := testmetrics.GaugeValueForLabels(
				t, metrics.COWSnapshotMemoryMappedBytes,
				prometheus.Labels{metrics.FileName: cowDirName})
			require.Equal(t, float64(0), mmappedBytes)

			r, err := interfaces.StoreReader(store)
			require.NoError(t, err)
			merged, err = io.ReadAll(r)
			require.NoError(t, err)
			err = store.Close()
			require.NoError(t, err)
		} else {
			merged, err = os.ReadFile(basePath)
			require.NoError(t, err)
		}
		if !bytes.Equal(merged, b) {
			require.FailNowf(t, "Merged bytes not equal to expected bytes", "")
		}
	}
}

// Writes n random data pages to the given file. The data pages written are
// copied into expectedBuf at the same offsets.
func writeRandomPages(t *testing.T, f *os.File, n int, expectedBuf []byte) {
	const pageSize = 4096
	for i := 0; i < n; i++ {
		offset := rand.Intn(len(expectedBuf))
		// Round offset down to page-level resolution
		offset = (offset / pageSize) * pageSize
		// Write 1 page if possible, or the remainder of the file if we're at
		// the end
		length := min(pageSize, len(expectedBuf)-offset)
		for i := offset; i < offset+length; i++ {
			expectedBuf[i] = byte(rand.Intn(128))
		}
		_, err := f.WriteAt(expectedBuf[offset:offset+length], int64(offset))
		require.NoError(t, err)
	}
}

func TestFirecrackerExecScriptLoadedFromDisk(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	// Write an NOP script and exec it directly, to test a fork/exec that
	// depends on the NBD in order to complete.
	err := os.WriteFile(filepath.Join(workDir, "script.sh"), []byte("#!/usr/bin/env bash"), 0777)
	require.NoError(t, err)
	cmd := &repb.Command{Arguments: []string{"./script.sh"}}
	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			// Important: NumCpus is 1 here to test that we can serve the NBD
			// request for the script, and execute the script, while only having
			// a single CPU core available (Go defaults GOMAXPROCS to this
			// value).
			NumCpus:           1,
			MemSizeMb:         500,
			EnableNetworking:  true,
			ScratchDiskSizeMb: 200,
		},
		ExecutorConfig: getExecutorConfig(t),
	}

	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)

	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	require.NoError(t, res.Error)
}

func TestFirecrackerHealthChecking(t *testing.T) {
	// Set health check durations to be short so that this test doesn't take a
	// long time.
	flags.Set(t, "executor.firecracker_health_check_interval", 1*time.Second)
	flags.Set(t, "executor.firecracker_health_check_timeout", 2*time.Second)

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	workDir := testfs.MakeTempDir(t)
	opts := firecracker.ContainerOpts{
		ContainerImage:         imageWithDockerInstalled,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         250,
			ScratchDiskSizeMb: 50,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)
	err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
	require.NoError(t, err)
	err = c.Create(ctx, workDir)
	require.NoError(t, err)
	t.Cleanup(func() {
		err := c.Remove(ctx)
		require.NoError(t, err)
	})

	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `
			set -e
			# Wait a little bit so that we can collect stats
			sleep 0.5
			# Freeze vmexec server (SIGSTOP)
			ps aux | grep '\--vmexec' | grep -v grep | awk '{print $2}' | xargs kill -STOP
		`},
	}
	res := c.Exec(ctx, cmd, nil /*=stdio*/)
	require.True(t, status.IsUnavailableError(res.Error), "expected Unavailable err, got %s", res.Error)
	require.GreaterOrEqual(t, res.UsageStats.GetPeakMemoryBytes(), int64(0))
}

func TestFirecrackerStressIO(t *testing.T) {
	// TODO: make these configurable via flags

	// High-level orchestration options
	const (
		// Total number of runs
		runs = 10
		// Max number of VMs to run concurrently
		concurrency = 1
	)
	// Per-exec options
	const (
		// Number of files to write per run.
		// After writing all files, they will all be read back.
		// Between read/write phases, the FS is synced and the page cache is
		// dropped.
		// If set to 0, each task does nothing (basically: sh -c '').
		// If set to < 0, skips Exec entirely and just does pause/unpause.
		ops = 100
		// File size of each file written, in bytes.
		fileSize = 10 * 1024
		// Whether to execute each run inside a docker container in the VM.
		// Requires dockerd.
		dockerize = false
	)
	// VM lifecycle options
	const (
		// Max number of times a single VM can be used before it is removed
		maxRunsPerVM = 1
	)
	// VM configuration
	const (
		cpus       = 4
		memoryMB   = 800
		scratchMB  = 800
		dockerd    = false
		networking = false
	)

	if dockerize && (!dockerd || !networking) {
		require.FailNow(t, "dockerize option requires dockerd and networking")
	}

	ctx := context.Background()
	te := getTestEnv(ctx, t, envOpts{})
	cfg := getExecutorConfig(t)

	// Create a little test setup that's like a simpler version of the runner
	// pool.
	type VM struct {
		Instance *firecracker.FirecrackerContainer
		Runs     int
	}
	pool := make(chan *VM, runs)
	get := func() (*VM, error) {
		select {
		case vm := <-pool:
			if err := vm.Instance.Unpause(ctx); err != nil {
				_ = vm.Instance.Remove(context.Background())
				return nil, err
			}
			return vm, nil
		default:
		}
		workDir := testfs.MakeTempDir(t)
		opts := firecracker.ContainerOpts{
			ContainerImage:         imageWithDockerInstalled,
			ActionWorkingDirectory: workDir,
			ExecutorConfig:         cfg,
			VMConfiguration: &fcpb.VMConfiguration{
				NumCpus:           cpus,
				MemSizeMb:         memoryMB,
				ScratchDiskSizeMb: scratchMB,
				InitDockerd:       dockerd,
				EnableNetworking:  networking,
			},
		}
		c, err := firecracker.NewContainer(ctx, te, &repb.ExecutionTask{}, opts)
		if err != nil {
			return nil, err
		}
		if err := c.PullImage(ctx, oci.Credentials{}); err != nil {
			return nil, err
		}
		if err := c.Create(ctx, opts.ActionWorkingDirectory); err != nil {
			return nil, err
		}
		return &VM{Instance: c}, nil
	}
	pause := func(vm *VM) error {
		if err := vm.Instance.Pause(ctx); err != nil {
			_ = vm.Instance.Remove(context.Background())
			assert.FailNowf(t, "pause failed", "%s", err)
			return err
		}
		vm.Runs++
		if vm.Runs < maxRunsPerVM {
			pool <- vm
		}
		return nil
	}

	script := `
		set -e

		OPS=` + fmt.Sprint(ops) + `
		if [ "$OPS" -eq 0 ]; then exit 0; fi

		SIZE=` + fmt.Sprint(fileSize) + `
		RUN_NUMBER=$(cat /root/run_number 2>/dev/null || echo 0)
		echo "Writing $OPS files..."
		for i in $(seq $OPS); do
			DIR=/root/$RUN_NUMBER
			if [ $(( i % 2 )) -eq 0 ]; then
				DIR=/workspace/$RUN_NUMBER
			fi
			mkdir -p "$DIR"
			yes | head -c "$SIZE" > "${DIR}/${i}".txt
		done
		# Flush to disk, then clear caches to read back from disk
		sync
		echo 3 > /proc/sys/vm/drop_caches
		echo "Reading back $OPS files..."
		for i in $(seq $OPS); do
			DIR=/root/$RUN_NUMBER
			if [ $(( i % 2 )) -eq 0 ]; then
				DIR=/workspace/$RUN_NUMBER
			fi
			cat "${DIR}/${i}".txt > /dev/null
		done
		echo $(( RUN_NUMBER + 1 )) > /root/run_number
	`
	cmd := &repb.Command{Arguments: []string{"/bin/sh", "-c", script}}
	if dockerize {
		cmd.Arguments = append([]string{
			"docker", "run", "--rm",
			"--privileged",
			"--net=host",
			"--volume=/root:/root",
			"--volume=/workspace:/workspace",
			busyboxImage,
		}, cmd.Arguments...)
	}
	eg, ctx := errgroup.WithContext(context.Background())
	eg.SetLimit(concurrency)
	for i := 1; i <= runs; i++ {
		i := i
		eg.Go(func() (err error) {
			if t.Failed() {
				return nil
			}
			defer func() { assert.NoError(t, err) }()

			vm, err := get()
			if err != nil {
				return err
			}
			defer pause(vm)

			log.Infof("Run %d of %d, VM exec %d of %d", i, runs, vm.Runs+1, maxRunsPerVM)

			if ops < 0 {
				return nil
			}

			res := vm.Instance.Exec(ctx, cmd, nil)
			if res.Error != nil {
				return res.Error
			}
			if res.ExitCode != 0 {
				return fmt.Errorf("exit code %d; stderr: %s", res.ExitCode, string(res.Stderr))
			}
			return nil
		})
		if t.Failed() {
			break
		}
	}
	err := eg.Wait()
	assert.NoError(t, err)
}

func TestBazelBuild(t *testing.T) {
	if !*testBazelBuild {
		t.Skip()
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t, envOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	cmd := &repb.Command{Arguments: []string{"bash", "-c", `
		cd ~
		git clone https://github.com/bazelbuild/rules_foreign_cc
		cd rules_foreign_cc
		bazelisk test //...
	`}}
	opts := firecracker.ContainerOpts{
		ContainerImage:         platform.Ubuntu20_04WorkflowsImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           6,
			MemSizeMb:         8000,
			EnableNetworking:  true,
			ScratchDiskSizeMb: 20_000,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, env, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	require.NoError(t, res.Error)
}

func tree(label string) {
	fmt.Println("jailer root", label, ":")
	b, err := exec.Command("tree", "-A", "-C", "--inodes", "/tmp/buildbuddy-test-jailer-root", "-I", "executor").CombinedOutput()
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(string(b))
}

// Returns a task that appends the message to a logfile, then prints the logfile so far
func appendToLog(message string) *repb.Command {
	return &repb.Command{
		Arguments: []string{"sh", "-c", `
				# Write to the scratchfs, which should be persisted in the snapshot
				cd /root
				# Clear the memory page cache to ensure the file is read from disk
				# NBD has unlocked immutable disk snapshots, which is what we
				# want to test here
				echo 3 > /proc/sys/vm/drop_caches
				echo ` + message + ` >> ./log
				cat ./log
			`},
	}
}

// Compares a CommandResult against an expected one, ignoring non-deterministic
// fields.
func assertCommandResult(t testing.TB, expected *interfaces.CommandResult, actual *interfaces.CommandResult) {
	{
		// Shallow copy
		a := *actual
		actual = &a
	}
	actual.UsageStats = nil
	actual.AuxiliaryLogs = nil
	actual.VMMetadata = nil
	assert.Equal(t, expected, actual)
}
