package fix

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Use a recorded `bazel help` flag database so tests don't shell out.
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

func TestWithTarget(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cmd    []string
		target string
		want   []string
	}{
		{
			name:   "narrows a wildcard pattern",
			cmd:    []string{"test", "//..."},
			target: "//foo:bar_test",
			want:   []string{"test", "//foo:bar_test"},
		},
		{
			name:   "replaces multiple targets",
			cmd:    []string{"test", "//foo/...", "//bar/..."},
			target: "//foo:bar_test",
			want:   []string{"test", "//foo:bar_test"},
		},
		{
			name:   "preserves attached flags",
			cmd:    []string{"test", "--config=ci", "//...", "--nocache_test_results"},
			target: "//foo:bar_test",
			want:   []string{"test", "--config=ci", "--nocache_test_results", "//foo:bar_test"},
		},
		{
			// The recorded command line is un-canonicalized, so a flag and its
			// value can be separate arguments. "errors" must not be mistaken
			// for a target and dropped along with "//...".
			name:   "preserves separated flag values",
			cmd:    []string{"test", "--test_output", "errors", "//..."},
			target: "//foo:bar_test",
			want:   []string{"test", "--test_output", "errors", "//foo:bar_test"},
		},
		{
			name:   "preserves startup options",
			cmd:    []string{"--nosystem_rc", "test", "//..."},
			target: "//foo:bar_test",
			want:   []string{"--nosystem_rc", "test", "//foo:bar_test"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := withTarget(tc.cmd, tc.target)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
