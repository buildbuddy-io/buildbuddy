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
