package firecracker_test

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/stretchr/testify/assert"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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
	c := firecracker.NewContainer(ctx, "docker.io/library/busybox", rootDir)
	res := c.Run(ctx, cmd, workDir)
	assert.Equal(t, expectedResult, res)
}
