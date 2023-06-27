package firecracker_test

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/runner"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ext4"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/buildbuddy-io/buildbuddy/server/util/fileresolver"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bundle "github.com/buildbuddy-io/buildbuddy/enterprise"
	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	busyboxImage = "docker.io/library/busybox:latest"
	// Alternate image to use if getting rate-limited by docker hub
	// busyboxImage = "gcr.io/google-containers/busybox:latest"

	ubuntuImage              = "marketplace.gcr.io/google/ubuntu2004"
	imageWithDockerInstalled = "gcr.io/flame-public/executor-docker-default:enterprise-v1.6.0"

	// Minimum memory needed for a firecracker VM. This may need to be increased
	// if the size of initrd.cpio increases.
	minMemSizeMB = 200

	diskCacheSize = 10_000_000_000  // 10GB
	fileCacheSize = 100_000_000_000 // 100GB
)

var (
	testExecutorRoot = flag.String("test_executor_root", "/tmp/test-executor-root", "If set, use this as the executor root data dir. Helps avoid excessive image pulling when re-running tests.")

	skipDockerTests = flag.Bool("skip_docker_tests", false, "Whether to skip docker-in-firecracker tests")
)

func init() {
	// Set umask to match the executor process.
	syscall.Umask(0)
}

// cleanExecutorRoot cleans all entries in the test root dir *except* for cached
// ext4 images. Converting docker images to ext4 images takes a long time and
// it would slow down testing to build these images from scratch every time.
// So we instead keep the same directory around and clean it between tests.
//
// See README.md for more details on the filesystem layout.
func cleanExecutorRoot(t *testing.T, path string) {
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

func getTestEnv(ctx context.Context, t *testing.T) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)

	b, err := bundle.Get()
	require.NoError(t, err)
	env.SetFileResolver(fileresolver.New(b, "enterprise"))

	testRootDir := testfs.MakeTempDir(t)
	dc, err := disk_cache.NewDiskCache(env, &disk_cache.Options{RootDirectory: testRootDir}, diskCacheSize)
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
	grpcServer, runFunc := env.LocalGRPCServer()
	repb.RegisterContentAddressableStorageServer(grpcServer, casServer)
	repb.RegisterActionCacheServer(grpcServer, actionCacheServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	go runFunc()

	conn, err := env.LocalGRPCConn(ctx)
	if err != nil {
		t.Error(err)
	}

	env.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetActionCacheClient(repb.NewActionCacheClient(conn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))

	fc, err := filecache.NewFileCache(testRootDir, fileCacheSize)
	require.NoError(t, err)
	env.SetFileCache(fc)

	// Some tests need iptables which is in /usr/sbin.
	err = os.Setenv("PATH", os.Getenv("PATH")+":/usr/sbin")
	require.NoError(t, err)

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
		cleanExecutorRoot(t, *testExecutorRoot)
		return *testExecutorRoot
	}

	// NOTE: JailerRoot needs to be < 38 chars long, so can't just use
	// testfs.MakeTempDir(t).
	return testfs.MakeTempSymlink(t, "/tmp", "buildbuddy-*-jailer", testfs.MakeTempDir(t))
}

func TestFirecrackerRunSimple(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	res.UsageStats = nil
	assert.Equal(t, expectedResult, res)
}

func TestFirecrackerLifecycle(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	cached, err := c.IsImageCached(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !cached {
		if err := c.PullImage(ctx, container.PullCredentials{}); err != nil {
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
	res.UsageStats = nil
	assert.Equal(t, expectedResult, res)
}

func TestFirecrackerSnapshotAndResume(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	path := filepath.Join(workDir, "world.txt")
	if err := os.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         minMemSizeMB, // small to make snapshotting faster.
			EnableNetworking:  false,
			ScratchDiskSizeMb: 100,
		},
		JailerRoot: tempJailerRoot(t),
	}
	c, err := firecracker.NewContainer(ctx, env, cacheAuth, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	if err := container.PullImageIfNecessary(ctx, env, cacheAuth, c, container.PullCredentials{}, opts.ContainerImage); err != nil {
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

func TestFirecrackerComplexFileMapping(t *testing.T) {
	numFiles := 100
	fileSizeBytes := int64(1_000_000)
	scratchTestFileSizeBytes := int64(50_000_000)
	ctx := context.Background()
	env := getTestEnv(ctx, t)

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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	if res.Error != nil {
		t.Fatalf("error: %s", res.Error)
	}

	// Check that the result has the expected disk usage stats.
	assert.True(t, res.UsageStats != nil)
	var workspaceFSU, scratchFSU *repb.UsageStats_FileSystemUsage
	for _, fsu := range res.UsageStats.PeakFileSystemUsage {
		if fsu.Target == "/workspace" {
			workspaceFSU = fsu
		} else if fsu.Target == "/" {
			scratchFSU = fsu
		}
	}

	const workspaceDiskSlackSpaceBytes = 2_000_000_000
	if workspaceFSU == nil {
		assert.Fail(t, "/workspace disk usage was not reported")
	} else {
		assert.Equal(t, "ext4", workspaceFSU.GetFstype())
		assert.True(t,
			workspaceFSU.GetSource() == "/dev/vdc" ||
				workspaceFSU.GetSource() == "/dev/nbd1",
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
	if scratchFSU == nil {
		assert.Fail(t, "scratch (/) disk usage was not reported")
	} else {
		assert.Equal(t, "overlay", scratchFSU.GetFstype())
		assert.Equal(t, "overlayfs:/scratch/bbvmroot", scratchFSU.GetSource())
		const approxInitialScratchDiskSizeBytes = 40e6
		assert.InDelta(
			t, approxInitialScratchDiskSizeBytes+scratchTestFileSizeBytes,
			scratchFSU.GetUsedBytes(),
			20_000_000,
			"used scratch disk size")
		assert.InDelta(
			t, 1_000_000*opts.VMConfiguration.ScratchDiskSizeMb+ext4.MinDiskImageSizeBytes+approxInitialScratchDiskSizeBytes,
			scratchFSU.GetTotalBytes(),
			20_000_000,
			"total scratch disk size")
	}

	res.UsageStats = nil

	assert.Equal(t, expectedResult, res)

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
	env := getTestEnv(ctx, t)
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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	assert.Equal(t, 0, res.ExitCode)
	assert.Contains(t, string(res.Stdout), "64 bytes from "+googleDNS)
}

func TestFirecrackerRun_ReapOrphanedZombieProcess(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
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
	env := getTestEnv(ctx, t)
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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
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
	env := getTestEnv(ctx, t)
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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	require.NoError(t, res.Error)
	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, "", string(res.Stderr))
	assert.Equal(t, "/workspace\n", string(res.Stdout))
}

func TestFirecrackerRunWithDocker(t *testing.T) {
	if *skipDockerTests {
		t.Skip()
	}

	ctx := context.Background()
	env := getTestEnv(ctx, t)
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	path := filepath.Join(workDir, "world.txt")
	if err := os.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			set -e
			# Discard pull output to make the output deterministic
			docker pull ` + busyboxImage + ` &>/dev/null

			# Try running a few commands
			docker run --rm ` + busyboxImage + ` echo Hello
			docker run --rm ` + busyboxImage + ` echo world
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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, "Hello\nworld\n", string(res.Stdout), "stdout should contain pwd output")
	assert.Equal(t, "", string(res.Stderr), "stderr should be empty")
}

func TestFirecrackerExecWithRecycledWorkspaceWithNewContents(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
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
		JailerRoot: tempJailerRoot(t),
	}
	c, err := firecracker.NewContainer(ctx, env, cacheAuth, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)
	err = container.PullImageIfNecessary(ctx, env, cacheAuth, c, container.PullCredentials{}, opts.ContainerImage)
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
	env := getTestEnv(ctx, t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
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
		JailerRoot: tempJailerRoot(t),
	}
	c, err := firecracker.NewContainer(ctx, env, cacheAuth, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)
	err = container.PullImageIfNecessary(ctx, env, cacheAuth, c, container.PullCredentials{}, opts.ContainerImage)
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
			docker pull ubuntu:20.04 &>/dev/null

			# Try running a few commands
			docker run --rm ubuntu:20.04 echo Hello
			docker run --rm ubuntu:20.04 echo world

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
		docker run --rm ubuntu:20.04 echo world
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
	env := getTestEnv(ctx, t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	cacheAuth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
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
		JailerRoot: tempJailerRoot(t),
	}
	c, err := firecracker.NewContainer(ctx, env, cacheAuth, &repb.ExecutionTask{}, opts)
	if err != nil {
		t.Fatal(err)
	}

	if err := container.PullImageIfNecessary(ctx, env, cacheAuth, c, container.PullCredentials{}, opts.ContainerImage); err != nil {
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
	env := getTestEnv(ctx, t)
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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)

	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		echo stdout >&1
		echo stderr >&2
		echo output > output.txt
		sleep infinity
	`}}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})

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
	env := getTestEnv(ctx, t)
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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)
	err = container.PullImageIfNecessary(ctx, env, auth, c, container.PullCredentials{}, opts.ContainerImage)
	require.NoError(t, err)
	err = c.Create(ctx, opts.ActionWorkingDirectory)
	require.NoError(t, err)
	t.Cleanup(func() {
		err = c.Remove(context.Background())
		require.NoError(t, err)
	})

	cmd := &repb.Command{Arguments: []string{"sh", "-c", `
		echo stdout >&1
		echo stderr >&2
		echo output > output.txt
		sleep infinity
	`}}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	res := c.Exec(ctx, cmd, nil /*=stdio*/)

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

func TestFirecrackerLargeResult(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
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
		JailerRoot: tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(ctx, env, auth, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)
	const stdoutSize = 10_000_000
	cmd := &repb.Command{Arguments: []string{"sh", "-c", fmt.Sprintf(`yes | head -c %d`, stdoutSize)}}
	res := c.Run(ctx, cmd, workDir, container.PullCredentials{})

	require.NoError(t, res.Error)
	assert.Equal(t, string(res.Stderr), "")
	assert.Len(t, res.Stdout, stdoutSize)
}

func TestFirecrackerWithExecutorRestart(t *testing.T) {
	ctx := context.Background()

	testRoot := tempJailerRoot(t)
	err := os.RemoveAll(filepath.Join(testRoot, "_runner_pool_state.bin"))
	require.NoError(t, err)
	filecacheRoot := testfs.MakeDirAll(t, testRoot, "filecache")

	var (
		env    *testenv.TestEnv
		ta     *testauth.TestAuthenticator
		fc     interfaces.FileCache
		pool   interfaces.RunnerPool
		ctxUS1 context.Context
	)
	setup := func() {
		var err error
		env = testenv.GetTestEnv(t)
		flags.Set(t, "executor.enable_firecracker", true)
		// Jailer root dir needs to be < 38 chars
		flags.Set(t, "executor.root_directory", testRoot)
		b, err := bundle.Get()
		require.NoError(t, err)
		env.SetFileResolver(fileresolver.New(b, "enterprise"))
		ta = testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1"))
		env.SetAuthenticator(ta)
		fc, err = filecache.NewFileCache(filecacheRoot, fileCacheSize)
		require.NoError(t, err)
		fc.WaitForDirectoryScanToComplete()
		env.SetFileCache(fc)
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

func tree(label string) {
	fmt.Println("jailer root", label, ":")
	b, err := exec.Command("tree", "-A", "-C", "--inodes", "/tmp/buildbuddy-test-jailer-root", "-I", "executor").CombinedOutput()
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(string(b))
}
