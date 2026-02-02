package image_verification_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/docker"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	dockerclient "github.com/docker/docker/client"
)

// Set via x_defs in BUILD file.
var (
	executorImageRlocationpath string
)

func TestExecutor(t *testing.T) {
	socket := "/var/run/docker.sock"
	dc, err := dockerclient.NewClientWithOpts(
		dockerclient.WithHost(fmt.Sprintf("unix://%s", socket)),
		dockerclient.WithAPIVersionNegotiation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	rootDir := testfs.MakeTempDir(t)
	cfg := &docker.DockerOptions{Socket: socket, InheritUserIDs: true}
	ctx := context.Background()
	cmd := &repb.Command{
		Arguments: []string{"/tini", "--", "/app/enterprise/server/cmd/executor/executor", "--executor.app_target=grpc://bb-server:1985"},
	}
	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1")))
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	c := docker.NewDockerContainer(env, dc, serveExecutorImage(t), rootDir, cfg)
	resultChannel := make(chan *interfaces.CommandResult)
	go func() {
		resultChannel <- c.Run(ctx, cmd, rootDir, oci.Credentials{})
	}()
	time.Sleep(time.Second * 10)
	c.Remove(ctx)
	result := <-resultChannel
	t.Logf("result:\nstdout:%s\nstderr:%s\n", result.Stdout, result.Stderr)
	t.FailNow()
}

func serveExecutorImage(t *testing.T) string {
	registry := testregistry.Run(t, testregistry.Opts{})
	image := testregistry.ImageFromRlocationpath(t, executorImageRlocationpath)
	imageName := "bb-executor"
	registry.Push(t, image, imageName, nil)
	return registry.ImageAddress(imageName)
}
