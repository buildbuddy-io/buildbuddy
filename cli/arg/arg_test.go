package arg

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

func TestNewBazelArgs(t *testing.T) {
	setupWorkspace(t, `
build --bes_backend=grpc://default-bes
build:ci --remote_cache=grpc://ci-cache
`)

	args, err := NewBazelArgs([]string{
		"build",
		"--config=ci",
		"-c", "opt",
		"//foo",
	})
	require.NoError(t, err)

	// Config and bazelrc flags should be expanded.
	require.Equal(t, "grpc://default-bes", args.Get("bes_backend"))
	require.Equal(t, "grpc://ci-cache", args.Get("remote_cache"))
	require.NotContains(t, args.Resolved(), "--config=ci")
	require.Contains(t, args.Resolved(), "--ignore_all_rc_files")

	// Flags should be canonicalized (-c expanded to --compilation_mode).
	require.Equal(t, "opt", args.Get("compilation_mode"))

	require.Equal(t, []string{"//foo"}, args.GetTargets())
	require.Equal(t, "build", args.GetCommand())
}

func TestAppend_Config(t *testing.T) {
	setupWorkspace(t, `
build:ci --remote_cache=grpc://ci-cache
`)
	args, err := NewBazelArgs([]string{"build", "//foo"})
	require.NoError(t, err)
	err = args.Append("--config=ci")
	require.NoError(t, err)
	err = args.Append("--configuration=a")
	require.NoError(t, err)

	// Config flag should be expanded.
	require.Equal(t, "grpc://ci-cache", args.Get("remote_cache"))
	require.NotContains(t, args.Resolved(), "--config=ci")

	// Make sure that we don't try to expand flags that start with config but don't match exactly.
	require.Equal(t, "a", args.Get("configuration"))
}

func TestAppend_ExecutableArgs(t *testing.T) {
	setupWorkspace(t, ``)

	args, err := NewBazelArgs([]string{"run", ":target", "--", "--flag=value"})
	require.NoError(t, err)

	err = args.Append("--build_metadata=ROLE=CI")
	require.NoError(t, err)

	require.Equal(t, "ROLE=CI", args.Get("build_metadata"))
	require.Equal(t, []string{":target"}, args.GetTargets())

	bazelArgs, execArgs := SplitExecutableArgs(args.Resolved())
	require.NotContains(t, execArgs, "--build_metadata=ROLE=CI")
	require.Contains(t, bazelArgs, "--build_metadata=ROLE=CI")
}

func TestAppendStartupOption(t *testing.T) {
	args, err := NewBazelArgsNoResolve([]string{"--bazelrc=/tmp/bazelrc", "build", "//foo"})
	require.NoError(t, err)

	err = args.AppendStartupOption("output_base", "/tmp/output")
	require.NoError(t, err)

	require.Equal(t, []string{
		"--bazelrc=/tmp/bazelrc",
		"--output_base=/tmp/output",
		"build",
		"//foo",
	}, args.Forwarded())
}

func TestPrepend(t *testing.T) {
	setupWorkspace(t, `
build:ci --remote_cache=grpc://ci-cache
`)
	args, err := NewBazelArgs([]string{"--output_base=/tmp/output", "build", "--flag=initial", "//foo"})
	require.NoError(t, err)

	err = args.Prepend("--build_metadata=ROLE=CI")
	require.NoError(t, err)

	require.Equal(t, []string{
		"--output_base=/tmp/output",
		"--ignore_all_rc_files",
		"build",
		"--build_metadata=ROLE=CI",
		"--flag=initial",
		"//foo",
	}, args.Resolved())

	err = args.Prepend("--config=ci")
	require.NoError(t, err)

	require.Equal(t, []string{
		"--output_base=/tmp/output",
		"--ignore_all_rc_files",
		"build",
		// Config flag should be expanded.
		"--remote_cache=grpc://ci-cache",
		"--build_metadata=ROLE=CI",
		"--flag=initial",
		"//foo",
	}, args.Resolved())
}

func TestPop(t *testing.T) {
	setupWorkspace(t, `
build --bes_backend=grpc://default-bes
`)
	args, err := NewBazelArgs([]string{"build", "--flag=val", "//foo"})
	require.NoError(t, err)

	value, err := args.Pop("flag")
	require.NoError(t, err)

	require.Equal(t, "val", value)
	require.Equal(t, "grpc://default-bes", args.Get("bes_backend"))
	require.NotContains(t, args.Resolved(), "--flag=val")
}

func TestPop_DoesNotRemoveFlagsFromConfigs(t *testing.T) {
	// --build_metadata is only set via config file. Pop should not be able to remove it.
	setupWorkspace(t, `build:ci --build_metadata=COMMIT_SHA=abc`)

	args, err := NewBazelArgs([]string{"build", "--config=ci", "//foo"})
	require.NoError(t, err)

	value, err := args.Pop("build_metadata")
	require.Error(t, err)
	require.Empty(t, value)

	require.Contains(t, args.Resolved(), "--build_metadata=COMMIT_SHA=abc")
}

func TestPop_SkipsConfigResolvedDuplicates(t *testing.T) {
	// --config=ci is placed after --build_metadata on the CLI, so the config
	// expansion appends --build_metadata=COMMIT_SHA=abc at the end of Resolved.
	// Pop should return the CLI-set value and leave the config-resolved one intact.
	setupWorkspace(t, `build:ci --build_metadata=COMMIT_SHA=abc`)

	args, err := NewBazelArgs([]string{"build", "--build_metadata=ROLE=CI", "--config=ci", "//foo"})
	require.NoError(t, err)

	value, err := args.Pop("build_metadata")
	require.NoError(t, err)

	require.Equal(t, "ROLE=CI", value)
	require.NotContains(t, args.Resolved(), "--build_metadata=ROLE=CI")
	require.Contains(t, args.Resolved(), "--build_metadata=COMMIT_SHA=abc")
}

func TestStripBBBoolFlag(t *testing.T) {
	tests := []struct {
		name              string
		args              []string
		flagToStrip       string
		expectedStripped  bool
		expectedForwarded []string
		expectedResolved  []string
	}{
		{
			name:              "flag set",
			args:              []string{"run", "--stream_run_logs", ":target"},
			flagToStrip:       "stream_run_logs",
			expectedStripped:  true,
			expectedForwarded: []string{"run", ":target"},
			expectedResolved:  []string{"--ignore_all_rc_files", "run", ":target"},
		},
		{
			name: "flag set multiple times",
			args: []string{
				"run",
				"--stream_run_logs",
				"--stream_run_logs",
				":target",
			},
			flagToStrip:       "stream_run_logs",
			expectedStripped:  true,
			expectedForwarded: []string{"run", ":target"},
			expectedResolved:  []string{"--ignore_all_rc_files", "run", ":target"},
		},
		{
			name: "flag set then unset",
			args: []string{
				"run",
				"--stream_run_logs",
				"--nostream_run_logs",
				":target",
			},
			flagToStrip:       "stream_run_logs",
			expectedStripped:  false,
			expectedForwarded: []string{"run", ":target"},
			expectedResolved:  []string{"--ignore_all_rc_files", "run", ":target"},
		},
		{
			name:              "flag not set",
			args:              []string{"run", ":target"},
			flagToStrip:       "stream_run_logs",
			expectedStripped:  false,
			expectedForwarded: []string{"run", ":target"},
			expectedResolved:  []string{"--ignore_all_rc_files", "run", ":target"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setupWorkspace(t, "")
			args, err := NewBazelArgs(test.args)
			require.NoError(t, err)
			stripped, err := args.StripBBBoolFlag(test.flagToStrip)
			require.NoError(t, err)
			require.Equal(t, test.expectedStripped, stripped)
			require.NotContains(t, args.Forwarded(), test.flagToStrip)
			require.NotContains(t, args.Resolved(), test.flagToStrip)

			require.Equal(t, test.expectedForwarded, args.Forwarded())
			require.Equal(t, test.expectedResolved, args.Resolved())
			require.Equal(t, []string{":target"}, args.GetTargets())
		})
	}
}

func TestStripBBFlag(t *testing.T) {
	tests := []struct {
		name              string
		args              []string
		flagToStrip       string
		expectedStripped  string
		expectedForwarded []string
		expectedResolved  []string
	}{
		{
			name:              "flag set",
			args:              []string{"run", "--on_stream_run_logs_failure=fail", ":target"},
			flagToStrip:       "on_stream_run_logs_failure",
			expectedStripped:  "fail",
			expectedForwarded: []string{"run", ":target"},
			expectedResolved:  []string{"--ignore_all_rc_files", "run", ":target"},
		},
		{
			name: "flag set multiple times",
			args: []string{
				"run",
				"--on_stream_run_logs_failure=ignore",
				"--on_stream_run_logs_failure=fail",
				":target",
			},
			flagToStrip:       "on_stream_run_logs_failure",
			expectedStripped:  "fail",
			expectedForwarded: []string{"run", ":target"},
			expectedResolved:  []string{"--ignore_all_rc_files", "run", ":target"},
		},
		{
			name:              "flag set with separate value",
			args:              []string{"run", "--on_stream_run_logs_failure", "fail", ":target"},
			flagToStrip:       "on_stream_run_logs_failure",
			expectedStripped:  "fail",
			expectedForwarded: []string{"run", ":target"},
			expectedResolved:  []string{"--ignore_all_rc_files", "run", ":target"},
		},
		{
			name:              "flag not set",
			args:              []string{"run", ":target"},
			flagToStrip:       "on_stream_run_logs_failure",
			expectedStripped:  "",
			expectedForwarded: []string{"run", ":target"},
			expectedResolved:  []string{"--ignore_all_rc_files", "run", ":target"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setupWorkspace(t, "")
			args, err := NewBazelArgs(test.args)
			require.NoError(t, err)
			stripped, err := args.StripBBFlag(test.flagToStrip)
			require.NoError(t, err)
			require.Equal(t, test.expectedStripped, stripped)
			require.NotContains(t, args.Forwarded(), test.flagToStrip)
			require.NotContains(t, args.Resolved(), test.flagToStrip)

			require.Equal(t, test.expectedForwarded, args.Forwarded())
			require.Equal(t, test.expectedResolved, args.Resolved())
			require.Equal(t, []string{":target"}, args.GetTargets())
		})
	}
}

func TestStripBBStartupOptions(t *testing.T) {
	setupWorkspace(t, "")

	args, err := NewBazelArgs([]string{"--watch", "--watcher_flags=--from_rc", "--watcher_flags=--from_cli", "test", "//foo"})
	require.NoError(t, err)

	removed := args.StripBBStartupOptions("watch", "watcher_flags")
	var removedArgs []string
	for _, opt := range removed {
		removedArgs = append(removedArgs, opt.Option.Normalized().Format()...)
	}

	require.Equal(t, []string{
		"--watch",
		"--watcher_flags=--from_rc",
		"--watcher_flags=--from_cli",
	}, removedArgs)
	require.Equal(t, []string{"test", "//foo"}, args.Forwarded())
	require.Equal(t, []string{"--ignore_all_rc_files", "test", "//foo"}, args.Resolved())
}

func TestFindLast(t *testing.T) {
	for _, tc := range []struct {
		Name          string
		Args          []string
		ExpectedValue string
		ExpectedIndex int
		SetWithSpace  bool
	}{
		{
			"Value is set with =",
			[]string{"--foo=bar"},
			"bar", 0, false,
		},
		{
			"Value is set with space",
			[]string{"--foo", "bar"},
			"bar", 0, true,
		},
		{
			"Arg set multiple times",
			[]string{"--foo=bar", "--foo=baz"},
			"baz", 1, false,
		},
		{
			"Arg set multiple times with space",
			[]string{"--foo=bar", "--foo", "baz"},
			"baz", 1, true,
		},
		{
			"Preceding args",
			[]string{"--fee=fum", "--foo=bar", "--foo", "baz"},
			"baz", 2, true,
		},
	} {
		val, idx, length := FindLast(tc.Args, "foo")
		require.Equal(t, tc.ExpectedValue, val, tc.Name)
		require.Equal(t, tc.ExpectedIndex, idx, tc.Name)
		expectedLength := 1
		if tc.SetWithSpace {
			expectedLength = 2
		}
		require.Equal(t, expectedLength, length, tc.Name)
	}
}

func TestGetMulti(t *testing.T) {
	args := []string{"--build_metadata=COMMIT_SHA=abc123", "--foo", "--build_metadata=ROLE=CI"}
	values := GetMulti(args, "build_metadata")
	assert.Equal(t, []string{"COMMIT_SHA=abc123", "ROLE=CI"}, values)
}

func TestGetTargets(t *testing.T) {
	args := []string{"build", "//foo"}
	targets := GetTargets(args)
	assert.Equal(t, []string{"//foo"}, targets)

	args = []string{"build", "//foo", ":bar", "baz"}
	targets = GetTargets(args)
	assert.Equal(t, []string{"//foo", ":bar", "baz"}, targets)

	args = []string{"build", "--opt=val", "foo", "bar", "--anotheropt=anotherval"}
	targets = GetTargets(args)
	assert.Equal(t, []string{"foo", "bar"}, targets)

	args = []string{"run", "--opt=val", "foo", "bar", "--", "baz"}
	targets = GetTargets(args)
	assert.Equal(t, []string{"foo", "bar"}, targets)

	// Don't include subtractive patterns.
	args = []string{"build", "--opt=val", "--", "foo", "bar", "-baz"}
	targets = GetTargets(args)
	assert.Equal(t, []string{"foo", "bar"}, targets)

	args = []string{"build"}
	targets = GetTargets(args)
	assert.Equal(t, []string{}, targets)

	args = []string{}
	targets = GetTargets(args)
	assert.Equal(t, []string{}, targets)
}

func TestGetCommand(t *testing.T) {
	args := []string{}
	command, index := GetCommandAndIndex(args)
	assert.Equal(t, "", command)
	assert.Equal(t, -1, index)

	args = []string{"build"}
	command, index = GetCommandAndIndex(args)
	assert.Equal(t, "build", command)
	assert.Equal(t, 0, index)

	args = []string{"--ouput_base=notcommand", "build", "--foo", "bar"}
	command, index = GetCommandAndIndex(args)
	assert.Equal(t, "build", command)
	assert.Equal(t, 1, index)
}

func setupWorkspace(t *testing.T, bazelrc string) {
	ws := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(ws, "WORKSPACE"), nil, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(ws, ".bazelrc"), []byte(bazelrc), 0644))
	workspace.SetForTest(t, ws)
}
