package firecracker_test

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/networking"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/config"
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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	bundle "github.com/buildbuddy-io/buildbuddy/enterprise"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	imageWithDockerInstalled = "gcr.io/flame-public/executor-docker-default:enterprise-v1.6.0"
)

func getTestEnv(ctx context.Context, t *testing.T) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)

	b, err := bundle.Get()
	require.NoError(t, err)
	env.SetFileResolver(fileresolver.New(b, "enterprise"))

	diskCacheSize := 10_000_000_000 // 10GB
	testRootDir := testfs.MakeTempDir(t)
	dc, err := disk_cache.NewDiskCache(env, &config.DiskConfig{RootDirectory: testRootDir}, int64(diskCacheSize))
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

	fc, err := filecache.NewFileCache(testRootDir, int64(diskCacheSize))
	require.NoError(t, err)
	env.SetFileCache(fc)
	return env
}

func tempJailerRoot(t *testing.T) string {
	// NOTE: JailerRoot needs to be < 38 chars long, so can't just use
	// testfs.MakeTempDir(t).
	root := "/tmp/buildbuddy-test-jailer-root"
	err := os.MkdirAll(root, 0755)
	require.NoError(t, err)
	return root
}

func TestFirecrackerRunSimple(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	path := filepath.Join(workDir, "world.txt")
	if err := ioutil.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `printf "$GREETING $(cat world.txt)" && printf "foo" >&2`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	expectedResult := &interfaces.CommandResult{
		ExitCode:           0,
		Stdout:             []byte("Hello world"),
		Stderr:             []byte("foo"),
		CommandDebugString: "(firecracker) [sh -c printf \"$GREETING $(cat world.txt)\" && printf \"foo\" >&2]",
	}

	opts := firecracker.ContainerOpts{
		ContainerImage:         "docker.io/library/busybox",
		ActionWorkingDirectory: workDir,
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       false,
		DiskSlackSpaceMB:       100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, nil, auth, opts)
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
	if err := ioutil.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `printf "$GREETING $(cat world.txt)" && printf "foo" >&2`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	expectedResult := &interfaces.CommandResult{
		ExitCode:           0,
		Stdout:             []byte("Hello world"),
		Stderr:             []byte("foo"),
		CommandDebugString: "(firecracker) [sh -c printf \"$GREETING $(cat world.txt)\" && printf \"foo\" >&2]",
	}

	opts := firecracker.ContainerOpts{
		ContainerImage:         "docker.io/library/busybox",
		ActionWorkingDirectory: workDir,
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       false,
		DiskSlackSpaceMB:       100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, nil, auth, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := c.Remove(ctx); err != nil {
			t.Fatal(err)
		}
	}()

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
	res := c.Exec(ctx, cmd, nil, nil)
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
	if err := ioutil.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	opts := firecracker.ContainerOpts{
		ContainerImage:         "docker.io/library/busybox",
		ActionWorkingDirectory: workDir,
		NumCPUs:                1,
		MemSizeMB:              200, // small to make snapshotting faster.
		EnableNetworking:       false,
		DiskSlackSpaceMB:       100,
		JailerRoot:             tempJailerRoot(t),
	}
	c, err := firecracker.NewContainer(env, nil, cacheAuth, opts)
	if err != nil {
		t.Fatal(err)
	}

	if err := container.PullImageIfNecessary(ctx, env, cacheAuth, c, container.PullCredentials{}, opts.ContainerImage); err != nil {
		t.Fatalf("unable to pull image: %s", err)
	}

	if err := c.Create(ctx, opts.ActionWorkingDirectory); err != nil {
		t.Fatalf("unable to Create container: %s", err)
	}

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
		ExitCode:           0,
		Stdout:             []byte("Hello world"),
		Stderr:             []byte("foo"),
		CommandDebugString: "(firecracker) [sh -c printf \"$GREETING $(cat world.txt)\" && printf \"foo\" >&2]",
	}

	res := c.Exec(ctx, cmd, nil /*=reader*/, nil /*=writer*/)
	if res.Error != nil {
		t.Fatalf("error: %s", res.Error)
	}
	assert.Equal(t, expectedResult, res)

	if err := c.Remove(ctx); err != nil {
		t.Fatal(err)
	}
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
		if err := ioutil.WriteFile(fullPath, buf, 0660); err != nil {
			t.Fatal(err)
		}
	}

	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `find -name '*.txt' -exec cp {} {}.out \;`},
	}
	expectedResult := &interfaces.CommandResult{
		ExitCode:           0,
		Stdout:             nil,
		Stderr:             nil,
		CommandDebugString: `(firecracker) [sh -c find -name '*.txt' -exec cp {} {}.out \;]`,
	}
	opts := firecracker.ContainerOpts{
		ContainerImage:         "docker.io/library/busybox",
		ActionWorkingDirectory: rootDir,
		NumCPUs:                1,
		MemSizeMB:              200,
		EnableNetworking:       false,
		DiskSlackSpaceMB:       100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, nil, auth, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, container.PullCredentials{})
	if res.Error != nil {
		t.Fatalf("error: %s", res.Error)
	}
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
	ctx := context.Background()
	env := getTestEnv(ctx, t)
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work1")

	path := filepath.Join(workDir, "world.txt")
	if err := ioutil.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `printf "$GREETING $(cat world.txt)" && printf "foo" >&2`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	expectedResult := &interfaces.CommandResult{
		ExitCode:           0,
		Stdout:             []byte("Hello world"),
		Stderr:             []byte("foo"),
		CommandDebugString: "(firecracker) [sh -c printf \"$GREETING $(cat world.txt)\" && printf \"foo\" >&2]",
	}

	opts := firecracker.ContainerOpts{
		ContainerImage:         "docker.io/library/busybox",
		ActionWorkingDirectory: workDir,
		NumCPUs:                1,
		MemSizeMB:              200,
		EnableNetworking:       false,
		AllowSnapshotStart:     true,
		DiskSlackSpaceMB:       100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, nil, auth, opts)
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
	if err := ioutil.WriteFile(path, []byte("mars"), 0660); err != nil {
		t.Fatal(err)
	}
	cmd = &repb.Command{
		Arguments: []string{"sh", "-c", `printf "$GREETING from $(cat mars.txt)" && printf "bar" >&2`},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "GREETING", Value: "Hello"},
		},
	}
	expectedResult = &interfaces.CommandResult{
		ExitCode:           0,
		Stdout:             []byte("Hello from mars"),
		Stderr:             []byte("bar"),
		CommandDebugString: "(firecracker) [sh -c printf \"$GREETING from $(cat mars.txt)\" && printf \"bar\" >&2]",
	}

	// This should resume the previous snapshot.
	c, err = firecracker.NewContainer(env, nil, auth, opts)
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
	if err := ioutil.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	// Make sure the container can at least send packets via the default route.
	defaultRouteIP, err := networking.FindDefaultRouteIP(ctx)
	require.NoError(t, err, "failed to find default route IP")
	cmd := &repb.Command{Arguments: []string{"ping", "-c1", defaultRouteIP}}

	opts := firecracker.ContainerOpts{
		ContainerImage:         "docker.io/library/busybox",
		ActionWorkingDirectory: workDir,
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       true,
		DiskSlackSpaceMB:       100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, nil, auth, opts)
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

func TestFirecrackerRunWithDocker(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")

	path := filepath.Join(workDir, "world.txt")
	if err := ioutil.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			set -e
			# Discard pull output to make the output deterministic
			docker pull busybox &>/dev/null

			# Try running a few commands
			docker run --rm busybox echo Hello
			docker run --rm busybox echo world
		`},
	}

	opts := firecracker.ContainerOpts{
		ContainerImage:         imageWithDockerInstalled,
		ActionWorkingDirectory: workDir,
		NumCPUs:                1,
		MemSizeMB:              2500,
		EnableNetworking:       true,
		InitDockerd:            true,
		DebugMode:              true,
		DiskSlackSpaceMB:       100,
		JailerRoot:             tempJailerRoot(t),
	}
	auth := container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{})
	c, err := firecracker.NewContainer(env, nil, auth, opts)
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

func TestFirecrackerExecWithDockerFromSnapshot(t *testing.T) {
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
		DiskSlackSpaceMB:       100,
		JailerRoot:             tempJailerRoot(t),
		DebugMode:              true,
	}
	c, err := firecracker.NewContainer(env, nil, cacheAuth, opts)
	if err != nil {
		t.Fatal(err)
	}

	if err := container.PullImageIfNecessary(ctx, env, cacheAuth, c, container.PullCredentials{}, opts.ContainerImage); err != nil {
		t.Fatalf("unable to pull image: %s", err)
	}

	if err := c.Create(ctx, opts.ActionWorkingDirectory); err != nil {
		t.Fatalf("unable to Create container: %s", err)
	}

	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			set -e
			# Discard pull output to make the output deterministic
			docker pull busybox &>/dev/null

			docker run --rm busybox echo Hello
		`},
	}

	res := c.Exec(ctx, cmd, nil /*=reader*/, nil /*=writer*/)

	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, "Hello\n", string(res.Stdout), "stdout should contain pwd output")
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
			docker run --rm busybox echo world
		`},
	}

	res = c.Exec(ctx, cmd, nil /*=reader*/, nil /*=writer*/)

	assert.Equal(t, 0, res.ExitCode)
	assert.Equal(t, "world\n", string(res.Stdout), "stdout should contain pwd output")
	assert.Equal(t, "", string(res.Stderr), "stderr should be empty")

	if err := c.Remove(ctx); err != nil {
		t.Fatal(err)
	}
}
