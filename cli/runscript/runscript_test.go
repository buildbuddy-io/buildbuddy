package runscript

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/buildbuddy-io/buildbuddy/cli/workspace"
	"github.com/stretchr/testify/require"
)

func init() {
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

func TestConfigure(t *testing.T) {
	for _, test := range []struct {
		name       string
		bazelrc    string
		args       []string
		wantScript bool
	}{
		{name: "run", args: []string{"run", "//x"}, wantScript: true},
		{name: "norun", args: []string{"run", "--norun", "//x"}},
		{name: "run=false", args: []string{"run", "--run=false", "//x"}},
		{name: "run=0", args: []string{"run", "--run=0", "//x"}},
		{name: "norun in bazelrc", bazelrc: "run --norun", args: []string{"run", "//x"}},
		{name: "norun then run", args: []string{"run", "--norun", "--run", "//x"}, wantScript: true},
		{name: "norun after --", args: []string{"run", "//x", "--", "--norun"}, wantScript: true},
		{name: "script_path set", args: []string{"run", "--script_path=/tmp/s", "//x"}},
		{name: "build", args: []string{"build", "//x"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			setupWorkspace(t, test.bazelrc)
			args, err := arg.NewBazelArgs(test.args)
			require.NoError(t, err)

			newArgs, scriptPath, err := Configure(args)
			require.NoError(t, err)
			if scriptPath != "" {
				t.Cleanup(func() { os.Remove(scriptPath) })
			}

			if test.wantScript {
				require.NotEmpty(t, scriptPath)
				require.Equal(t, scriptPath, newArgs.Get("script_path"))
			} else {
				require.Empty(t, scriptPath)
			}
		})
	}
}

func setupWorkspace(t *testing.T, bazelrc string) {
	ws := t.TempDir()
	t.Setenv("HOME", t.TempDir())
	require.NoError(t, os.WriteFile(filepath.Join(ws, "WORKSPACE"), nil, 0644))
	require.NoError(t, os.WriteFile(filepath.Join(ws, ".bazelrc"), []byte(bazelrc), 0644))
	workspace.SetForTest(t, ws)
}
