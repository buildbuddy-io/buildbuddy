package commandutil_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
)

func TestRun_NormalExit_NoError(t *testing.T) {
	ctx := context.Background()

	{
		res := runSh(ctx, "exit 0")

		assert.NoError(t, res.Error)
		assert.Equal(t, 0, res.ExitCode)
	}
	{
		res := runSh(ctx, "exit 1")

		assert.NoError(t, res.Error)
		assert.Equal(t, 1, res.ExitCode)
	}
	{
		res := runSh(ctx, "exit 137")

		assert.NoError(t, res.Error)
		assert.Equal(t, 137, res.ExitCode)
	}
}

func TestRun_Abort_NoError(t *testing.T) {
	ctx := context.Background()

	res := runSh(ctx, "kill -ABRT $$")

	assert.NoError(t, res.Error)
	assert.Equal(t, 134, res.ExitCode) // 134 = SignaledExitCodeOffset(=128) + SIGABRT(=6)
}

func TestRun_OtherKillSignals_ErrorResult(t *testing.T) {
	ctx := context.Background()

	{
		res := runSh(ctx, "kill -INT $$")

		assert.Equal(t, commandutil.ErrKilled, res.Error)
		assert.Equal(t, -1, res.ExitCode)
	}
	{
		res := runSh(ctx, "kill -TERM $$")

		assert.Equal(t, commandutil.ErrKilled, res.Error)
		assert.Equal(t, -1, res.ExitCode)
	}
	{
		res := runSh(ctx, "kill -KILL $$")

		assert.Equal(t, commandutil.ErrKilled, res.Error)
		assert.Equal(t, -1, res.ExitCode)
	}
}

func TestRun_CommandNotFound_ErrorResult(t *testing.T) {
	ctx := context.Background()

	{
		cmd := &repb.Command{Arguments: []string{"./command_not_found_in_working_dir"}}
		res := commandutil.Run(ctx, cmd, ".", nil /*=stdin*/, nil /*=stdout*/)

		assert.Error(t, res.Error)
		assert.True(t, status.IsFailedPreconditionError(res.Error), "expecting FAILED_PRECONDITION when executable file is missing")
		assert.Equal(t, -2, res.ExitCode)
	}
	{
		cmd := &repb.Command{Arguments: []string{"command_not_found_in_PATH"}}
		res := commandutil.Run(ctx, cmd, ".", nil /*=stdin*/, nil /*=stdout*/)

		assert.Error(t, res.Error)
		assert.True(t, status.IsFailedPreconditionError(res.Error), "expecting FAILED_PRECONDITION when executable is not found in $PATH")
		assert.Equal(t, -2, res.ExitCode)
	}
}

func TestRun_CommandNotExecutable_ErrorResult(t *testing.T) {
	ctx := context.Background()
	wd := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, wd, map[string]string{"non_executable_file": ""})

	cmd := &repb.Command{Arguments: []string{"./non_executable_file"}}
	res := commandutil.Run(ctx, cmd, wd, nil /*=stdin*/, nil /*=stdout*/)

	assert.Error(t, res.Error)
	assert.True(t, status.IsFailedPreconditionError(res.Error), "expecting FAILED_PRECONDITION when command is not executable")
	assert.Equal(t, -2, res.ExitCode)
}

func runSh(ctx context.Context, script string) *interfaces.CommandResult {
	cmd := &repb.Command{Arguments: []string{"sh", "-c", script}}
	return commandutil.Run(ctx, cmd, ".", nil /*=stdin*/, nil /*=stdout*/)
}
