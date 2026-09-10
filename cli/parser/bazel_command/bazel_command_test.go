package bazel_command_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazel_command"
	"github.com/stretchr/testify/assert"
)

func TestCommands(t *testing.T) {
	assert.True(t, bazel_command.IsCommand("build"))
	assert.True(t, bazel_command.IsCommand("test"))
	assert.False(t, bazel_command.IsCommand("not-a-bazel-command"))
}

func TestParent(t *testing.T) {
	assert.Equal(t, "test", bazel_command.Parent("coverage"))
	assert.Equal(t, "build", bazel_command.Parent("test"))
	assert.Empty(t, bazel_command.Parent("build"))
	assert.Empty(t, bazel_command.Parent("not-a-bazel-command"))
}

func TestGetCommandAndIndex(t *testing.T) {
	for _, tc := range []struct {
		name          string
		args          []string
		expectedCmd   string
		expectedIndex int
	}{
		{
			name:          "bazel command",
			args:          []string{"build", "//..."},
			expectedCmd:   "build",
			expectedIndex: 0,
		},
		{
			name:          "bazel command after startup options",
			args:          []string{"--output_base=/tmp/foo", "analyze-profile", "profile.gz"},
			expectedCmd:   "analyze-profile",
			expectedIndex: 1,
		},
		{
			name:          "bazel command after space-separated startup options",
			args:          []string{"--output_base", "/tmp/foo", "analyze-profile", "profile.gz"},
			expectedCmd:   "analyze-profile",
			expectedIndex: 2,
		},
		{
			name:          "cli command with a bazel command name as a subcommand",
			args:          []string{"agent", "analyze-profile", "invocation-id"},
			expectedCmd:   "",
			expectedIndex: -1,
		},
		{
			name:          "cli command with a bazel command name as an argument",
			args:          []string{"install", "--path", "test"},
			expectedCmd:   "",
			expectedIndex: -1,
		},
		{
			name:          "cli command alias",
			args:          []string{"wtf", "test"},
			expectedCmd:   "",
			expectedIndex: -1,
		},
		{
			name:          "no command",
			args:          []string{"--output_base=/tmp/foo"},
			expectedCmd:   "",
			expectedIndex: -1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cmd, idx := bazel_command.GetCommandAndIndex(tc.args)
			assert.Equal(t, tc.expectedCmd, cmd)
			assert.Equal(t, tc.expectedIndex, idx)
		})
	}
}
