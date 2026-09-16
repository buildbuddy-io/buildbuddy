//go:build linux && !android

package runner

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/containers/bare"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/filecache"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestRunnerVFSDownloadError(t *testing.T) {
	flags.Set(t, "executor.enable_vfs", true)
	for _, testCase := range []struct {
		name         string
		script       string
		wantVFSError bool
	}{
		{name: "command_fails", script: "cat input.txt", wantVFSError: true},
		{name: "command_ignores_error", script: "cat input.txt || true", wantVFSError: true},
		{name: "ordinary_missing_file", script: "cat nonexistent.txt", wantVFSError: false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			authenticator := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
			env.SetAuthenticator(authenticator)
			ctx, err := authenticator.WithAuthenticatedUser(t.Context(), "US1")
			require.NoError(t, err)
			_, runServer, lis := testenv.RegisterLocalGRPCServer(t, env)
			testcache.Setup(t, env, lis)
			go runServer()
			fc, err := filecache.NewFileCache(testfs.MakeTempDir(t), 1_000_000, false)
			require.NoError(t, err)
			fc.WaitForDirectoryScanToComplete()
			env.SetFileCache(fc)
			pool, err := NewPool(env, testfs.MakeTempDir(t), &PoolOptions{ContainerProvider: &bare.Provider{}})
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, pool.Shutdown(env.GetServerContext())) })
			task := &repb.ScheduledTask{ExecutionTask: &repb.ExecutionTask{Command: &repb.Command{
				Arguments: []string{"sh", "-c", testCase.script},
				Platform: &repb.Platform{Properties: []*repb.Platform_Property{
					{Name: "recycle-runner", Value: "true"},
					{Name: "enable-vfs", Value: "true"},
					{Name: "vfs-prefetch-mode", Value: "none"},
				}},
			}}}
			runner, err := pool.Get(ctx, task)
			require.NoError(t, err)
			r := runner.(*taskRunner)
			t.Cleanup(func() { require.NoError(t, r.RemoveWithTimeout(ctx)) })
			require.NoError(t, r.PrepareForTask(ctx))

			// The input exists in the action's tree but has been evicted from
			// CAS. Files absent from the tree should remain ordinary ENOENTs.
			d, err := digest.Compute(strings.NewReader("evicted input"), repb.DigestFunction_SHA256)
			require.NoError(t, err)
			err = r.Workspace.DownloadInputs(ctx, &container.FileSystemLayout{
				DigestFunction: repb.DigestFunction_SHA256,
				Inputs: &repb.Tree{Root: &repb.Directory{Files: []*repb.FileNode{
					{Name: "input.txt", Digest: d},
				}}},
			})
			require.NoError(t, err)
			res := r.Run(ctx, &repb.IOStats{})
			if testCase.wantVFSError {
				require.True(t, status.IsFailedPreconditionError(res.Error), "%v", res.Error)
				require.Equal(t, commandutil.NoExitCode, res.ExitCode)
				require.Contains(t, string(res.Stderr), "Input/output error")
			} else {
				require.NoError(t, res.Error)
				require.NotZero(t, res.ExitCode)
			}
		})
	}
}
