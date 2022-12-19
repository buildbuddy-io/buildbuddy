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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/workspace"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bundle "github.com/buildbuddy-io/buildbuddy/enterprise"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	busyboxImage = "docker.io/library/busybox:latest"
	// Alternate image to use if getting rate-limited by docker hub
	// busyboxImage = "gcr.io/google-containers/busybox:latest"

	imageWithDockerInstalled = "gcr.io/flame-public/executor-docker-default:enterprise-v1.6.0"

	// Minimum memory needed for a firecracker VM. This may need to be increased
	// if the size of initrd.cpio increases.
	minMemSizeMB = 200

	diskCacheSize = 10_000_000_000  // 10GB
	fileCacheSize = 100_000_000_000 // 100GB
)

var (
	skipDockerTests = flag.Bool("skip_docker_tests", false, "Whether to skip docker-in-firecracker tests")
)

func init() {
	// Set umask to match the executor process.
	syscall.Umask(0)
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
	return env
}

func tempJailerRoot(t *testing.T) string {
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
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       false,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}
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
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       false,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
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
		NumCPUs:                1,
		MemSizeMB:              minMemSizeMB, // small to make snapshotting faster.
		EnableNetworking:       false,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
	}
	c, err := firecracker.NewContainer(env, cacheAuth, opts)
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

	if err := c.Pause(ctx); err != nil {
		t.Fatalf("unable to pause container: %s", err)
	}
	if err := c.Unpause(ctx); err != nil {
		t.Fatalf("unable to unpause container: %s", err)
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

	res := c.Exec(ctx, cmd, nil /*=stdio*/)
	if res.Error != nil {
		t.Fatalf("error: %s", res.Error)
	}
	assert.Equal(t, expectedResult, res)
}

func TestFirecrackerFileMapping(t *testing.T) {
	numFiles := 100
	fileSizeBytes := int64(1000)
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
		d, buf := testdigest.NewRandomDigestBuf(t, fileSizeBytes)
		fullPath := filepath.Join(parentDir, d.GetHash()+".txt")
		if err := os.WriteFile(fullPath, buf, 0660); err != nil {
			t.Fatal(err)
		}
	}

	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `find -name '*.txt' -exec cp {} {}.out \;`},
	}
	expectedResult := &interfaces.CommandResult{
		ExitCode: 0,
		Stdout:   nil,
		Stderr:   nil,
	}
	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: rootDir,
		NumCPUs:                1,
		MemSizeMB:              minMemSizeMB,
		EnableNetworking:       false,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	if res.Error != nil {
		t.Fatalf("error: %s", res.Error)
	}

	// Check that the result has usage stats, but don't perform equality checks
	// on them.
	assert.True(t, res.UsageStats != nil)
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

func TestFirecrackerRunStartFromSnapshot(t *testing.T) {
	// TODO: Re-enable after fixing TODOs in Run()
	t.Skip()

	ctx := context.Background()
	env := getTestEnv(ctx, t)
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work1")

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
		NumCPUs:                1,
		MemSizeMB:              minMemSizeMB,
		EnableNetworking:       false,
		AllowSnapshotStart:     true,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
		DebugMode:              true,
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Run will handle the full lifecycle: no need to call Remove() here.
	firstRunStart := time.Now()
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	firstRunDuration := time.Since(firstRunStart)
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	assert.Equal(t, expectedResult, res)

	// Now do the same thing again, but with a twist. The attached
	// files and command run will be different. This command would
	// fail if the new workspace were not properly attached.
	workDir = testfs.MakeDirAll(t, rootDir, "work2")
	opts.ActionWorkingDirectory = workDir

	path = filepath.Join(workDir, "mars.txt")
	if err := os.WriteFile(path, []byte("mars"), 0660); err != nil {
		t.Fatal(err)
	}
	cmd = &repb.Command{
		Arguments: []string{"sh", "-c", `printf "$GREETING from $(cat mars.txt)" && printf "bar" >&2`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	expectedResult = &interfaces.CommandResult{
		ExitCode: 0,
		Stdout:   []byte("Hello from mars"),
		Stderr:   []byte("bar"),
	}

	// This should resume the previous snapshot.
	c, err = firecracker.NewContainer(env, auth, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	secondRunStart := time.Now()
	res = c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	secondRunDuration := time.Since(secondRunStart)
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	assert.Equal(t, expectedResult, res)

	// This should be significantly faster because it's started from a
	// snapshot.
	// TODO(tylerw): debug this.
	assert.Less(t, secondRunDuration, firstRunDuration)
}

func TestFirecrackerRunWithNetwork(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	path := filepath.Join(workDir, "world.txt")
	if err := os.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	// Make sure the container can at least send packets via the default route.
	defaultRouteIP, err := networking.FindDefaultRouteIP(ctx)
	require.NoError(t, err, "failed to find default route IP")
	cmd := &repb.Command{Arguments: []string{"ping", "-c1", defaultRouteIP}}

	opts := firecracker.ContainerOpts{
		ContainerImage:         busyboxImage,
		ActionWorkingDirectory: workDir,
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       true,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}

	assert.Equal(t, 0, res.ExitCode)
	assert.Contains(t, string(res.Stdout), "64 bytes from "+defaultRouteIP)
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
		NumCPUs:                1,
		MemSizeMB:              1000,
		EnableNetworking:       false,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
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
	require.Regexp(t, regexp.MustCompile("uid=[0-9]+\\(nobody\\) gid=[0-9]+\\(nobody\\)"), string(res.Stdout))
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
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       false,
		JailerRoot:             tempJailerRoot(t),
		// Request 0 disk; implementation should ensure the disk is at least as big
		// as is required to run a NOP command. Otherwise, users might have to
		// keep on top of our min disk requirements which is not really feasible.
		ScratchDiskSizeMB: 0,
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
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
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       true,
		InitDockerd:            true,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
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
		NumCPUs:                1,
		MemSizeMB:              2500,
		ScratchDiskSizeMB:      2000,
		JailerRoot:             tempJailerRoot(t),
	}
	c, err := firecracker.NewContainer(env, cacheAuth, opts)
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
		NumCPUs:                1,
		MemSizeMB:              2500,
		ScratchDiskSizeMB:      4000, // 4 GB
		JailerRoot:             tempJailerRoot(t),
		EnableNetworking:       true,
		InitDockerd:            true,
	}
	c, err := firecracker.NewContainer(env, cacheAuth, opts)
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
		NumCPUs:                1,
		MemSizeMB:              2500,
		InitDockerd:            true,
		EnableNetworking:       true,
		ScratchDiskSizeMB:      1000,
		JailerRoot:             tempJailerRoot(t),
	}
	c, err := firecracker.NewContainer(env, cacheAuth, opts)
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
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       false,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
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
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       false,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
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
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       false,
		ScratchDiskSizeMB:      100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, auth, opts)
	require.NoError(t, err)
	const stdoutSize = 10_000_000
	cmd := &repb.Command{Arguments: []string{"sh", "-c", fmt.Sprintf(`yes | head -c %d`, stdoutSize)}}
	res := c.Run(ctx, cmd, workDir, container.PullCredentials{})

	require.NoError(t, res.Error)
	assert.Equal(t, string(res.Stderr), "")
	assert.Len(t, res.Stdout, stdoutSize)
}

func tree(label string) {
	fmt.Println("jailer root", label, ":")
	b, err := exec.Command("tree", "-A", "-C", "--inodes", "/tmp/buildbuddy-test-jailer-root", "-I", "executor").CombinedOutput()
	if err != nil {
		fmt.Println("error:", err)
	}
	fmt.Println(string(b))
}
