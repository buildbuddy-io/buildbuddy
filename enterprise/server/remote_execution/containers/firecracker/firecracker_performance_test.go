package firecracker_test

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/snaploader"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/stretchr/testify/require"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

var testManualBenchmark = flag.Bool("test_manual_benchmark", false, "Whether to run manual benchmarking tests.")
var testCacheRoot = flag.String("test_cache_root", "", "If set, use this as the cache root dir for the manual benchmark test. This is useful if you want to use a cached snapshot to speed up re-running tests.")
var testSingleBuild = flag.Bool("test_single_build", false, "Whether to only run one test build. This can be useful because the whole test takes a long time to run.")

// To enable tracing, start a jaeger instance with:
// `docker run -d --name jaeger -p 16686:16686 -p 14268:14268 jaegertracing/all-in-one:1.23`
// View traces at http://localhost:16686
// To port-forward the UI from a GCP VM, you can run:
// `gcloud compute ssh gcp_vm_name -- -NL 16686:localhost:16686`
var enableTracing = flag.Bool("manual_benchmark_enable_tracing", false, "Whether to enable tracing with jaeger.")

// Prints performance data about various Firecracker commands
// Run with:
// ./enterprise/server/remote_execution/containers/firecracker/test.sh -- -test.run=TestFirecracker_RemoteSnapshotSharing_ManualBenchmarking -test_manual_benchmark
//
// Because the initial build takes a long time and the default cache dir is cleaned
// up at the end of every test, you can pass a more permanent cache
// directory with -test_cache_root. After running the test once, the cache will
// be populated with a snapshot, and in future runs of the test you can ignore the
// results related to clean builds
// ./enterprise/server/remote_execution/containers/firecracker/test.sh -- -test.run=TestFirecracker_RemoteSnapshotSharing_ManualBenchmarking -test_manual_benchmark -test_cache_root=/tmp/disk_dir -test_single_build
func TestFirecracker_RemoteSnapshotSharing_ManualBenchmarking(t *testing.T) {
	if !*testManualBenchmark {
		t.Skip()
	}

	// Silence the logs so output is easier to read
	flags.Set(t, "app.log_level", "fatal")
	log.Configure()

	rand.Seed(time.Now().UnixNano())

	var containersToCleanup []*firecracker.FirecrackerContainer
	t.Cleanup(func() {
		for _, vm := range containersToCleanup {
			_ = vm.Remove(context.Background())
		}
	})

	var (
		env  *testenv.TestEnv
		ctx  context.Context
		c    *firecracker.FirecrackerContainer
		task *repb.ExecutionTask
		opts firecracker.ContainerOpts
		cfg  *firecracker.ExecutorConfig
	)

	setup := func(createContainer bool) {
		var err error
		env = testenv.GetTestEnv(t)
		flags.Set(t, "executor.enable_local_snapshot_sharing", true)
		flags.Set(t, "executor.firecracker_enable_vbd", true)
		flags.Set(t, "executor.firecracker_enable_uffd", true)

		ctx = context.Background()
		// Set large cache size (100GB) to ensure artifacts aren't evicted
		env = getTestEnv(ctx, t, envOpts{cacheSize: 100_000_000_000, cacheRootDir: *testCacheRoot, filecacheRootDir: *testCacheRoot})
		cfg = getExecutorConfig(t)
		env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

		if *enableTracing {
			flags.Set(t, "app.trace_fraction", 1)
		}
		flags.Set(t, "app.trace_jaeger_collector", "http://localhost:14268/api/traces")
		err = tracing.Configure(env)
		require.NoError(t, err)

		task = &repb.ExecutionTask{
			Command: &repb.Command{
				// Note: platform must match in order to share snapshots
				Platform: &repb.Platform{Properties: []*repb.Platform_Property{
					{Name: "recycle-runner", Value: "true"},
					{Name: platform.WorkflowIDPropertyName, Value: "workflow"},
				}},
			},
		}

		if createContainer {
			rootDir := testfs.MakeTempDir(t)
			workDir := testfs.MakeDirAll(t, rootDir, "work")
			opts = firecracker.ContainerOpts{
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

			c, err = firecracker.NewContainer(ctx, env, task, opts)
			require.NoError(t, err)
			containersToCleanup = append(containersToCleanup, c)
			err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
			require.NoError(t, err)
			err = c.Create(ctx, opts.ActionWorkingDirectory)
			require.NoError(t, err)
		}
	}

	// Test various scenarios of running bazel builds with clean/recycled
	// runners
	for _, enableRemote := range []bool{true, false} {
		flags.Set(t, "executor.enable_remote_snapshot_sharing", enableRemote)
		setup(false)

		rootDir := testfs.MakeTempDir(t)
		workDir := testfs.MakeDirAll(t, rootDir, "work")
		cmd := &repb.Command{Arguments: []string{"bash", "-c", `
				 cd ~
				 if [ -d buildbuddy ]; then
					echo "Directory exists."
				 else
					git clone https://github.com/buildbuddy-io/buildbuddy --filter=blob:none
				 fi
				 cd buildbuddy
				 # See https://github.com/bazelbuild/bazelisk/issues/220
				 echo "USE_BAZEL_VERSION=6.4.0rc1" > .bazeliskrc
				 bazelisk build //enterprise/server/...
			`}}
		opts = firecracker.ContainerOpts{
			ContainerImage:         platform.Ubuntu20_04WorkflowsImage,
			ActionWorkingDirectory: workDir,
			VMConfiguration: &fcpb.VMConfiguration{
				NumCpus:           6,
				MemSizeMb:         10000,
				EnableNetworking:  true,
				ScratchDiskSizeMb: 20_000,
			},
			ExecutorConfig: cfg,
		}
		// firecracker.NewContainer() mutates the ContainerOpts set on the container
		// The opts are used to generate the snapshot key, so update the same fields
		// here, so manually constructed snapshot keys in this test match keys constructed
		// by the container
		opts.VMConfiguration.KernelVersion = cfg.KernelVersion
		opts.VMConfiguration.FirecrackerVersion = cfg.FirecrackerVersion
		opts.VMConfiguration.GuestApiVersion = cfg.GuestAPIVersion

		// Run bazel on a clean runner. Local and remote cache are empty
		start := time.Now()
		ctx, span := tracing.StartSpan(ctx)
		c, err := firecracker.NewContainer(ctx, env, task, opts)
		require.NoError(t, err)
		containersToCleanup = append(containersToCleanup, c)
		err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
		require.NoError(t, err)
		err = c.Create(ctx, opts.ActionWorkingDirectory)
		require.NoError(t, err)
		res := c.Exec(ctx, cmd, nil)
		require.NoError(t, res.Error)
		require.Contains(t, string(res.Stderr), "Build completed successfully")
		fmt.Printf("(remote_snapshot_sharing=%v) Bazel build on a clean runner took %s.\n", enableRemote, time.Since(start))
		err = c.Pause(ctx)
		require.NoError(t, err)
		span.End()

		if *testSingleBuild {
			t.Fatal("Exiting early because test_single_build flag was set")
		}

		// Run a bazel test on a recycled runner - 100% locally cached
		start = time.Now()
		workDir = testfs.MakeDirAll(t, rootDir, "work-fork-locally-cached")
		opts.ActionWorkingDirectory = workDir
		c, err = firecracker.NewContainer(ctx, env, task, opts)
		require.NoError(t, err)
		containersToCleanup = append(containersToCleanup, c)
		err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
		require.NoError(t, err)
		err = c.Unpause(ctx)
		require.NoError(t, err)
		res = c.Exec(ctx, cmd, nil)
		require.NoError(t, res.Error)
		require.Contains(t, string(res.Stderr), "Build completed successfully")
		require.Contains(t, string(res.Stdout), "Directory exists")
		fmt.Printf("(remote_snapshot_sharing=%v) Bazel build on a recycled runner (100%% locally cached) took %s.\n", enableRemote, time.Since(start))

		// Evict 30% of artifacts from local cache.
		// If only local snapshot sharing is enabled, will be forced to run
		// on a clean runner.
		// If remote is enabled, will fetch missing artifacts from remote cache.
		loader, err := snaploader.New(env)
		require.NoError(t, err)
		configHash, err := digest.ComputeForMessage(opts.VMConfiguration, repb.DigestFunction_SHA256)
		require.NoError(t, err)
		keySet, err := snaploader.SnapshotKeySet(task, configHash.GetHash(), "")
		require.NoError(t, err)
		snapMetadata, err := loader.GetSnapshot(ctx, keySet, enableRemote)
		require.NoError(t, err)
		for _, f := range snapMetadata.GetFiles() {
			if rand.Intn(100) < 30 {
				deleted := env.GetFileCache().DeleteFile(ctx, f)
				require.True(t, deleted)
			}
		}
		for _, f := range snapMetadata.GetChunkedFiles() {
			for _, c := range f.GetChunks() {
				if rand.Intn(100) < 30 {
					_ = env.GetFileCache().DeleteFile(ctx, &repb.FileNode{Digest: c.Digest})
				}
			}
		}

		start = time.Now()
		workDir = testfs.MakeDirAll(t, rootDir, "work-fork-30-locally-cached")
		opts.ActionWorkingDirectory = workDir
		c, err = firecracker.NewContainer(ctx, env, task, opts)
		require.NoError(t, err)
		containersToCleanup = append(containersToCleanup, c)
		err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
		require.NoError(t, err)
		err = c.Create(ctx, workDir)
		require.NoError(t, err)
		res = c.Exec(ctx, cmd, nil)
		require.NoError(t, res.Error)
		require.Contains(t, string(res.Stderr), "Build completed successfully")
		if enableRemote {
			require.Contains(t, string(res.Stdout), "Directory exists")
			fmt.Printf("(remote_snapshot_sharing=%v) Bazel build (30%% locally cached) took %s. 70%% artifacts fetched remotely.\n", enableRemote, time.Since(start))
		} else {
			fmt.Printf("(remote_snapshot_sharing=%v) Bazel build (30%% locally cached) took %s. Could not start from snapshot, had to prepare clean runner.\n", enableRemote, time.Since(start))
		}

		// Evict all artifacts from filecache.
		// If only local snapshot sharing is enabled, will be forced to run
		// on a clean runner.
		// If remote is enabled, will fetch all artifacts from remote cache
		fcDir := testfs.MakeTempDir(t)
		fc, err := filecache.NewFileCache(fcDir, fileCacheSize, false)
		require.NoError(t, err)
		fc.WaitForDirectoryScanToComplete()
		env.SetFileCache(fc)

		start = time.Now()
		workDir = testfs.MakeDirAll(t, rootDir, "work-fork-remotely-cached")
		opts.ActionWorkingDirectory = workDir
		c, err = firecracker.NewContainer(ctx, env, task, opts)
		require.NoError(t, err)
		containersToCleanup = append(containersToCleanup, c)
		err = container.PullImageIfNecessary(ctx, env, c, oci.Credentials{}, opts.ContainerImage)
		require.NoError(t, err)
		err = c.Create(ctx, workDir)
		require.NoError(t, err)
		res = c.Exec(ctx, cmd, nil)
		require.NoError(t, res.Error)
		require.Contains(t, string(res.Stderr), "Build completed successfully")
		if enableRemote {
			require.Contains(t, string(res.Stdout), "Directory exists")
			fmt.Printf("(remote_snapshot_sharing=%v) Bazel build (0%% locally cached) took %s. 100%% artifacts fetched remotely.\n", enableRemote, time.Since(start))
		} else {
			fmt.Printf("(remote_snapshot_sharing=%v) Bazel build (0%% locally cached) took %s. Could not start from snapshot, had to prepare clean runner.\n", enableRemote, time.Since(start))
		}
		fmt.Println()
	}

	fmt.Printf("\n\n======= More Detailed Breakdowns =======\n\n")

	// Pausing a new VM
	for _, enableRemote := range []bool{true, false} {
		flags.Set(t, "executor.enable_remote_snapshot_sharing", enableRemote)
		setup(true)

		start := time.Now()
		err := c.Pause(ctx)
		require.NoError(t, err)
		fmt.Printf("(remote_snapshot_sharing=%v) Pausing a new VM took %s.\n", enableRemote, time.Since(start))
	}

	// Pausing a VM that had started from a snapshot.
	// No changes to the VM other than running for a bit.
	for _, enableRemote := range []bool{true, false} {
		flags.Set(t, "executor.enable_remote_snapshot_sharing", enableRemote)
		setup(true)

		// Create a snapshot
		err := c.Pause(ctx)
		require.NoError(t, err)

		// Load the snapshot
		err = c.Unpause(ctx)
		require.NoError(t, err)

		start := time.Now()
		err = c.Pause(ctx)
		require.NoError(t, err)
		fmt.Printf("(remote_snapshot_sharing=%v) Pausing a VM that had started from a snapshot and had no non-metadata changes took %s.\n", enableRemote, time.Since(start))
	}

	// Pausing a VM that had started from a snapshot.
	// Execute a command in the VM before saving the new snapshot.
	for _, enableRemote := range []bool{true, false} {
		flags.Set(t, "executor.enable_remote_snapshot_sharing", enableRemote)
		setup(true)

		// Create a snapshot
		err := c.Pause(ctx)
		require.NoError(t, err)

		// Load the snapshot and exec a command in the VM
		err = c.Unpause(ctx)
		require.NoError(t, err)
		cmd := appendToLog("Executing")
		res := c.Exec(ctx, cmd, nil /*=stdio*/)
		require.NoError(t, res.Error)

		start := time.Now()
		err = c.Pause(ctx)
		require.NoError(t, err)
		fmt.Printf("(remote_snapshot_sharing=%v) Pausing a VM that had started from a snapshot and had execution-related changes took %s.\n", enableRemote, time.Since(start))
	}

	// Loading a snapshot for a VM.
	for _, enableRemote := range []bool{true, false} {
		flags.Set(t, "executor.enable_remote_snapshot_sharing", enableRemote)
		setup(true)

		// Create a snapshot
		err := c.Pause(ctx)
		require.NoError(t, err)

		start := time.Now()
		err = c.Unpause(ctx)
		require.NoError(t, err)
		fmt.Printf("(remote_snapshot_sharing=%v) Unpausing a VM that is fully cached in filecache took %s.\n", enableRemote, time.Since(start))
	}

	// Loading a snapshot for a VM where ~30% of artifacts were evicted from filecache
	// and must be fetched remotely.
	{
		flags.Set(t, "executor.enable_remote_snapshot_sharing", true)
		setup(true)

		// Create a snapshot
		err := c.Pause(ctx)
		require.NoError(t, err)

		// Delete 30% of artifacts from the filecache
		loader, err := snaploader.New(env)
		require.NoError(t, err)
		configHash, err := digest.ComputeForMessage(opts.VMConfiguration, repb.DigestFunction_SHA256)
		require.NoError(t, err)
		keySet, err := snaploader.SnapshotKeySet(task, configHash.GetHash(), "")
		require.NoError(t, err)
		snapMetadata, err := loader.GetSnapshot(ctx, keySet, true)
		require.NoError(t, err)
		for _, f := range snapMetadata.GetFiles() {
			if rand.Intn(100) < 30 {
				deleted := env.GetFileCache().DeleteFile(ctx, f)
				require.True(t, deleted)
			}
		}
		for _, f := range snapMetadata.GetChunkedFiles() {
			for _, c := range f.GetChunks() {
				if rand.Intn(100) < 30 {
					_ = env.GetFileCache().DeleteFile(ctx, &repb.FileNode{Digest: c.Digest})
				}
			}
		}
		start := time.Now()
		err = c.Unpause(ctx)
		require.NoError(t, err)

		fmt.Printf("(remote_snapshot_sharing=true) Unpausing a VM where ~30%% of artifacts were evicted from filecache and must be fetched remotely. Took %s.\n", time.Since(start))
	}

	// Loading a snapshot for a VM where all artifacts were evicted from filecache
	// and must be fetched remotely.
	{
		flags.Set(t, "executor.enable_remote_snapshot_sharing", true)
		setup(true)

		// Create a snapshot
		err := c.Pause(ctx)
		require.NoError(t, err)

		// Clear the filecache
		err = os.RemoveAll(filepath.Join(cfg.JailerRoot, "filecache"))
		require.NoError(t, err)
		filecacheRoot2 := testfs.MakeDirAll(t, cfg.JailerRoot, "filecache2")
		fc2, err := filecache.NewFileCache(filecacheRoot2, fileCacheSize, false)
		require.NoError(t, err)
		fc2.WaitForDirectoryScanToComplete()
		env.SetFileCache(fc2)

		start := time.Now()
		err = c.Unpause(ctx)
		require.NoError(t, err)

		fmt.Printf("(remote_snapshot_sharing=true) Unpausing a VM where all artifacts were evicted from filecache and must be fetched remotely. Took %s.\n", time.Since(start))
	}
}
