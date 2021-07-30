package firecracker_test

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/server/backends/disk_cache"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/stretchr/testify/assert"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func makeDir(t *testing.T, prefix string) string {
	dir, err := ioutil.TempDir(prefix, "d-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

func getTestEnv(ctx context.Context, t *testing.T) *testenv.TestEnv {
	env := testenv.GetTestEnv(t)
	diskCacheSize := 10_000_000_000 // 10GB
	testRootDir := makeDir(t, "/tmp")
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
	return env
}

func TestFirecrackerRun(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
	rootDir := makeDir(t, "/tmp")
	workDir := makeDir(t, rootDir)

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
	}
	c, err := firecracker.NewContainer(env, opts)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory)
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	assert.Equal(t, expectedResult, res)
}

func TestFirecrackerSnapshotAndResume(t *testing.T) {
	ctx := context.Background()
	env := getTestEnv(ctx, t)
	rootDir := makeDir(t, "/tmp")
	workDir := makeDir(t, rootDir)

	path := filepath.Join(workDir, "world.txt")
	if err := ioutil.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	opts := firecracker.ContainerOpts{
		ContainerImage:         "docker.io/library/busybox",
		ActionWorkingDirectory: workDir,
		NumCPUs:                1,
		MemSizeMB:              100,
		EnableNetworking:       false,
	}
	c, err := firecracker.NewContainer(env, opts)
	if err != nil {
		t.Fatal(err)
	}

	if err := c.PullImageIfNecessary(ctx); err != nil {
		t.Fatalf("unable to PullImageIfNecessary: %s", err)
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

	rootDir := makeDir(t, "/tmp")
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
		MemSizeMB:              100,
		EnableNetworking:       false,
	}
	c, err := firecracker.NewContainer(env, opts)
	if err != nil {
		t.Fatal(err)
	}
	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory)
	if res.Error != nil {
		t.Fatalf("error: %s", res.Error)
	}
	assert.Equal(t, expectedResult, res)

	for _, fullPath := range files {
		if exists, err := disk.FileExists(fullPath); err != nil || !exists {
			t.Fatalf("File %q not found in workspace.", fullPath)
		}
		if exists, err := disk.FileExists(fullPath + ".out"); err != nil || !exists {
			t.Fatalf("File %q not found in workspace.", fullPath)
		}
	}
}
