package image_verification_test

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/firecracker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/test/integration/remote_execution/rbetest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"

	fcpb "github.com/buildbuddy-io/buildbuddy/proto/firecracker"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

// Set via x_defs in BUILD file.
var (
	executorImageRlocationpath string
)

var (
	testExecutorRoot = flag.String("test_executor_root", "/tmp/test-executor-root", "If set, use this as the executor root data dir. Helps avoid excessive image pulling when re-running tests.")
)

func executorRootDir(t *testing.T) string {
	// When running this test on the bare executor pool, ensure the jailer root
	// is under /buildbuddy so that it's on the same device as the executor data
	// dir (with action workspaces and filecache).
	// Using a fixed directory also means that separate runs can share the image
	// cache instead of each having to pull their own images.
	if testfs.Exists(t, "/buildbuddy", "") {
		*testExecutorRoot = "/buildbuddy/test-executor-root"
	}

	if *testExecutorRoot != "" {
		return *testExecutorRoot
	}

	// NOTE: JailerRoot needs to be < 38 chars long, so can't just use
	// testfs.MakeTempDir(t).
	return testfs.MakeTempSymlink(t, "/tmp", "buildbuddy-*-jailer", testfs.MakeTempDir(t))
}

func getExecutorConfig(t *testing.T) *firecracker.ExecutorConfig {
	root := executorRootDir(t)
	buildRoot := filepath.Join(root, "build")
	cacheRoot := filepath.Join(root, "cache")

	err := os.MkdirAll(cacheRoot, 0755)
	require.NoError(t, err)

	cfg, err := firecracker.GetExecutorConfig(context.Background(), buildRoot, cacheRoot)
	require.NoError(t, err)
	return cfg
}

func TestRun(t *testing.T) {
	rbeEnv := rbetest.NewRBETestEnv()
	bbServer := rbeEnv.AddBuildBuddyServer()

	ctx := context.Background()

	cmd := &repb.Command{
		Arguments: []string{"bash", "-c", `
			set -e

			docker image import ` + executorImageRlocationpath + ` executor:latest  &>/dev/null

			docker run --rm executor:latest
		`},
	}

	opts := firecracker.ContainerOpts{
		ContainerImage:         executorImageRlocationpath,
		ActionWorkingDirectory: "/",
		VMConfiguration: &fcpb.VMConfiguration{
			NumCpus:           1,
			MemSizeMb:         2500,
			EnableNetworking:  true,
			InitDockerd:       true,
			ScratchDiskSizeMb: 100,
		},
		ExecutorConfig: getExecutorConfig(t),
	}
	c, err := firecracker.NewContainer(ctx, rbeEnv, &repb.ExecutionTask{}, opts)
	require.NoError(t, err)

	res := c.Run(ctx, cmd, opts.ActionWorkingDirectory, oci.Credentials{})
	if res.Error != nil {
		t.Fatal(res.Error)
	}

}
