package firecracker_test

import (
	"context"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testdigest"
	"github.com/buildbuddy-io/buildbuddy/server/util/disk"
	"github.com/stretchr/testify/assert"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	_ "github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
)

func makeRootDir(t *testing.T) string {
	rootDir, err := ioutil.TempDir("/tmp", "buildbuddy_docker_test_*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		os.RemoveAll(rootDir)
	})
	return rootDir
}

func makeWorkDir(t *testing.T, rootDir string) string {
	workDir, err := ioutil.TempDir(rootDir, "ws_*")
	if err != nil {
		t.Fatal(err)
	}
	return workDir
}

func TestFirecrackerRun(t *testing.T) {
	rootDir := makeRootDir(t)
	workDir := makeWorkDir(t, rootDir)

	path := filepath.Join(workDir, "world.txt")
	if err := ioutil.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
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
	c, err := firecracker.NewContainer(ctx, "docker.io/library/busybox", rootDir)
	if err != nil {
		t.Fatal(err)
	}

	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, workDir)
	if res.Error != nil {
		t.Fatal(res.Error)
	}
	assert.Equal(t, expectedResult, res)
}

func TestFirecrackerSnapshotAndResume(t *testing.T) {
	rootDir := makeRootDir(t)
	workDir := makeWorkDir(t, rootDir)

	path := filepath.Join(workDir, "world.txt")
	if err := ioutil.WriteFile(path, []byte("world"), 0660); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	c, err := firecracker.NewContainer(ctx, "docker.io/library/busybox", workDir)
	if err != nil {
		t.Fatal(err)
	}

	if err := c.PullImageIfNecessary(ctx); err != nil {
		t.Fatalf("unable to PullImageIfNecessary: %s", err)
	}

	if err := c.Create(ctx, workDir); err != nil {
		t.Fatalf("unable to Create container: %s", err)
	}

	if err := c.Pause(ctx); err != nil {
		t.Fatalf("unable to pause container: %s", err)
	}
	if err := c.Unpause(ctx); err != nil {
		t.Fatalf("unable to unpause container: %s", err)
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

	rootDir := makeRootDir(t)
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
	c, err := firecracker.NewContainer(ctx, "docker.io/library/busybox", rootDir)
	if err != nil {
		t.Fatal(err)
	}
	// Run will handle the full lifecycle: no need to call Remove() here.
	res := c.Run(ctx, cmd, rootDir)
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
