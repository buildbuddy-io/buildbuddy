package applecontainer_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/applecontainer"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestRun(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	ctx := context.Background()
	cmd := &repb.Command{
		Arguments: []string{"sh", "-c", `echo "Hello"`},
	}

	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	env.SetCommandRunner(&commandutil.CommandRunner{})

	provider, err := applecontainer.NewProvider(env, rootDir)
	require.NoError(t, err)

	ctr, err := provider.New(ctx, &container.Init{Props: &platform.Properties{ContainerImage: "docker.io/library/busybox:latest"}})
	require.NoError(t, err)

	res := ctr.Run(ctx, cmd, workDir, oci.Credentials{})
	require.Zero(t, res.ExitCode)
	require.Equal(t, "Hello\n", string(res.Stdout))
	require.NoError(t, res.Error)
}

func TestExecAfterCreate(t *testing.T) {
	rootDir := testfs.MakeTempDir(t)
	workDir := testfs.MakeDirAll(t, rootDir, "work")
	ctx := context.Background()
	cmd := &repb.Command{Arguments: []string{"sh", "-c", `cat <<'EOF'
HelloFromExec
EOF
`}}

	env := testenv.GetTestEnv(t)
	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))
	env.SetImageCacheAuthenticator(container.NewImageCacheAuthenticator(container.ImageCacheAuthenticatorOpts{}))
	env.SetCommandRunner(&commandutil.CommandRunner{})

	provider, err := applecontainer.NewProvider(env, rootDir)
	require.NoError(t, err)

	ctr, err := provider.New(ctx, &container.Init{Props: &platform.Properties{ContainerImage: "docker.io/library/busybox:latest"}})
	require.NoError(t, err)

	require.NoError(t, container.PullImageIfNecessary(ctx, env, ctr, oci.Credentials{}, "docker.io/library/busybox:latest"))
	require.NoError(t, ctr.Create(ctx, workDir))

	res := ctr.Exec(ctx, cmd, &interfaces.Stdio{})
	require.Zero(t, res.ExitCode)
	require.Equal(t, "HelloFromExec\n", string(res.Stdout))
	require.NoError(t, res.Error)

	require.NoError(t, ctr.Remove(ctx))
}
