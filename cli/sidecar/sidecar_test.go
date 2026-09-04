package sidecar

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

func TestShouldUseSynchronousBESProxy(t *testing.T) {
	for _, test := range []struct {
		name              string
		synchronousWrites bool
		bazelrc           string
		args              []string
		want              bool
	}{
		{
			name: "command line wait for upload complete",
			args: []string{"build", "--bes_upload_mode=wait_for_upload_complete", "//foo"},
			want: true,
		},
		{
			name: "command line wait for upload complete with separate value",
			args: []string{"build", "--bes_upload_mode", "wait_for_upload_complete", "//foo"},
			want: true,
		},
		{
			name:    "bazelrc wait for upload complete",
			bazelrc: "build --bes_upload_mode=wait_for_upload_complete\n",
			args:    []string{"build", "//foo"},
			want:    true,
		},
		{
			name:    "config wait for upload complete",
			bazelrc: "build:blocking --bes_upload_mode=wait_for_upload_complete\n",
			args:    []string{"build", "--config=blocking", "//foo"},
			want:    true,
		},
		{
			name: "command line do not wait for upload complete",
			args: []string{"build", "--bes_upload_mode=nowait_for_upload_complete", "//foo"},
		},
		{
			name: "command line fully asynchronous",
			args: []string{"build", "--bes_upload_mode=fully_async", "//foo"},
		},
		{
			name: "unspecified uses Bazel default without synchronous proxy",
			args: []string{"build", "//foo"},
		},
		{
			name: "executable argument is not a Bazel flag",
			args: []string{"run", "//foo", "--", "--bes_upload_mode=wait_for_upload_complete"},
		},
		{
			name:              "bb sync overrides asynchronous BES mode",
			synchronousWrites: true,
			args:              []string{"build", "--bes_upload_mode=fully_async", "//foo"},
			want:              true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ws := t.TempDir()
			t.Setenv("HOME", t.TempDir())
			t.Setenv("USERPROFILE", t.TempDir())
			require.NoError(t, os.WriteFile(filepath.Join(ws, "WORKSPACE"), nil, 0644))
			require.NoError(t, os.WriteFile(filepath.Join(ws, ".bazelrc"), []byte(test.bazelrc), 0644))
			workspace.SetForTest(t, ws)

			args, err := arg.NewBazelArgs(test.args)
			require.NoError(t, err)
			require.Equal(t, test.want, shouldUseSynchronousBESProxy(test.synchronousWrites, args))
		})
	}
}
