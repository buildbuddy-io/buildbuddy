package bbrc

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	register_cli_commands "github.com/buildbuddy-io/buildbuddy/cli/cli_command/register"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/stretchr/testify/require"
)

func TestResolveArgs_SubcommandInheritanceAndQuotedPhase(t *testing.T) {
	register_cli_commands.Register()
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `
common --common
execution --execution
"execution get" --get
common:debug --common-debug
execution:debug --execution-debug
"execution get":debug --get-debug
`,
	})

	command := cli_command.GetCommand("execution")
	require.NotNil(t, command)

	args, err := resolveArgs(command, []string{"--bb_config=debug", "get", "--output=markdown", "abc123"}, nil, ws, "")
	require.NoError(t, err)
	require.Equal(t, []string{
		"get",
		"--common",
		"--execution",
		"--get",
		"--common-debug",
		"--execution-debug",
		"--get-debug",
		"--output=markdown",
		"abc123",
	}, args)
}

func TestResolveArgs_StartupConfigSelection(t *testing.T) {
	register_cli_commands.Register()
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `
fix --diff=false
fix:ci --diff
`,
	})

	command := cli_command.GetCommand("fix")
	require.NotNil(t, command)

	args, err := resolveArgs(command, nil, []string{"ci"}, ws, "")
	require.NoError(t, err)
	require.Equal(t, []string{"--diff=false", "--diff"}, args)
}

func TestResolveArgs_RemoteBoundaryLeavesBazelArgsUntouched(t *testing.T) {
	register_cli_commands.Register()
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `
remote --remote_runner=from-rc
remote:ci --os=linux
`,
	})

	command := cli_command.GetCommand("remote")
	require.NotNil(t, command)

	args, err := resolveArgs(command, []string{
		"--bb_config=ci",
		"build",
		"//:target",
		"--bb_config=untouched",
		"--os=untouched",
	}, nil, ws, "")
	require.NoError(t, err)
	require.Equal(t, []string{
		"--remote_runner=from-rc",
		"--os=linux",
		"build",
		"//:target",
		"--bb_config=untouched",
		"--os=untouched",
	}, args)
}

func TestResolveArgs_ImportsAndCircularConfigs(t *testing.T) {
	register_cli_commands.Register()
	ws := testfs.MakeTempDir(t)
	testfs.WriteAllFileContents(t, ws, map[string]string{
		".bbrc": `
import %workspace%/shared.bbrc
ask:loop --bb_config=loop
`,
		"shared.bbrc": `
common --verbose
ask:from-import --openai
`,
	})

	command := cli_command.GetCommand("ask")
	require.NotNil(t, command)

	args, err := resolveArgs(command, []string{"--bb_config=from-import"}, nil, ws, "")
	require.NoError(t, err)
	require.Equal(t, []string{"--verbose", "--openai"}, args)

	_, err = resolveArgs(command, []string{"--bb_config=loop"}, nil, ws, "")
	require.ErrorContains(t, err, "circular --bb_config reference detected")
}
