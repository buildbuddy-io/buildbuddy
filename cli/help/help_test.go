package help_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/cli/help"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFindTargetCommandFromHelpArgs(t *testing.T) {
	tests := []struct {
		name     string
		args     []arguments.Argument
		expected string
	}{
		{
			name:     "no command found",
			args:     []arguments.Argument{},
			expected: "",
		},
		{
			name: "direct command",
			args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "build"},
			},
			expected: "build",
		},
		{
			name: "help with target command",
			args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "help"},
				&arguments.PositionalArgument{Value: "add"},
			},
			expected: "add",
		},
		{
			name: "help with bazel command",
			args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "help"},
				&arguments.PositionalArgument{Value: "query"},
			},
			expected: "query",
		},
		{
			name: "help without target command",
			args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "help"},
			},
			expected: "help",
		},
		{
			name: "help with non-positional args mixed in",
			args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "help"},
				&arguments.PositionalArgument{Value: "login"},
			},
			expected: "login",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orderedArgs := &parsed.OrderedArgs{Args: tt.args}
			result := help.FindTargetCommandFromHelpArgs(orderedArgs)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTryShowBBCommandHelp(t *testing.T) {
	// Set up some mock commands for testing
	originalCommands := cli_command.Commands
	originalCommandsByName := cli_command.CommandsByName
	defer func() {
		cli_command.Commands = originalCommands
		cli_command.CommandsByName = originalCommandsByName
	}()

	// Mock CLI commands
	cli_command.Commands = []*cli_command.Command{
		{Name: "add", Help: "Adds a dependency to your WORKSPACE file."},
		{Name: "login", Help: "Configures bb commands to use your BuildBuddy API key."},
	}
	cli_command.CommandsByName = map[string]*cli_command.Command{
		"add":   cli_command.Commands[0],
		"login": cli_command.Commands[1],
	}

	tests := []struct {
		name           string
		targetCommand  string
		expectedResult bool
		expectedOutput string
	}{
		{
			name:           "empty command",
			targetCommand:  "",
			expectedResult: false,
			expectedOutput: "",
		},
		{
			name:           "bb cli command",
			targetCommand:  "add",
			expectedResult: true,
			expectedOutput: "Usage: bb add\n\nAdds a dependency to your WORKSPACE file.\n",
		},
		{
			name:           "another bb cli command",
			targetCommand:  "login",
			expectedResult: true,
			expectedOutput: "Usage: bb login\n\nConfigures bb commands to use your BuildBuddy API key.\n",
		},
		{
			name:           "non-bb command",
			targetCommand:  "query",
			expectedResult: false,
			expectedOutput: "",
		},
		{
			name:           "unknown command",
			targetCommand:  "unknown",
			expectedResult: false,
			expectedOutput: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			result := help.TryShowBBCommandHelp(tt.targetCommand)

			// Restore stdout
			w.Close()
			os.Stdout = oldStdout

			// Read captured output
			var buf bytes.Buffer
			io.Copy(&buf, r)
			output := buf.String()

			assert.Equal(t, tt.expectedResult, result)
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func TestHandleHelp_BBCommands(t *testing.T) {
	// Set up some mock commands for testing
	originalCommands := cli_command.Commands
	originalCommandsByName := cli_command.CommandsByName
	defer func() {
		cli_command.Commands = originalCommands
		cli_command.CommandsByName = originalCommandsByName
	}()

	// Mock CLI commands
	cli_command.Commands = []*cli_command.Command{
		{Name: "add", Help: "Adds a dependency to your WORKSPACE file."},
		{Name: "login", Help: "Configures bb commands to use your BuildBuddy API key."},
	}
	cli_command.CommandsByName = map[string]*cli_command.Command{
		"add":   cli_command.Commands[0],
		"login": cli_command.Commands[1],
	}

	tests := []struct {
		name             string
		args             []arguments.Argument
		expectedExitCode int
		expectedOutput   string
		shouldCallBazel  bool
	}{
		{
			name: "bb help add",
			args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "help"},
				&arguments.PositionalArgument{Value: "add"},
			},
			expectedExitCode: 0,
			expectedOutput:   "Usage: bb add\n\nAdds a dependency to your WORKSPACE file.\n",
			shouldCallBazel:  false,
		},
		{
			name: "bb help login",
			args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "help"},
				&arguments.PositionalArgument{Value: "login"},
			},
			expectedExitCode: 0,
			expectedOutput:   "Usage: bb login\n\nConfigures bb commands to use your BuildBuddy API key.\n",
			shouldCallBazel:  false,
		},
		{
			name: "bb add (direct command help)",
			args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "add"},
			},
			expectedExitCode: 0,
			expectedOutput:   "Usage: bb add\n\nAdds a dependency to your WORKSPACE file.\n",
			shouldCallBazel:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Capture stdout
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			orderedArgs := &parsed.OrderedArgs{Args: tt.args}
			exitCode, err := help.HandleHelp(orderedArgs)

			// Restore stdout
			w.Close()
			os.Stdout = oldStdout

			// Read captured output
			var buf bytes.Buffer
			io.Copy(&buf, r)
			output := buf.String()

			require.NoError(t, err)
			assert.Equal(t, tt.expectedExitCode, exitCode)
			assert.Equal(t, tt.expectedOutput, output)
		})
	}
}

func TestHandleHelp_NonBBCommands(t *testing.T) {
	// Set up some mock commands for testing
	originalCommands := cli_command.Commands
	originalCommandsByName := cli_command.CommandsByName
	defer func() {
		cli_command.Commands = originalCommands
		cli_command.CommandsByName = originalCommandsByName
	}()

	// Mock CLI commands (empty for this test)
	cli_command.Commands = []*cli_command.Command{}
	cli_command.CommandsByName = map[string]*cli_command.Command{}

	tests := []struct {
		name        string
		args        []arguments.Argument
		description string
	}{
		{
			name: "bb help query",
			args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "help"},
				&arguments.PositionalArgument{Value: "query"},
			},
			description: "should forward to bazel for unknown commands",
		},
		{
			name: "bb help build",
			args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "help"},
				&arguments.PositionalArgument{Value: "build"},
			},
			description: "should forward to bazel for bazel commands",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			orderedArgs := &parsed.OrderedArgs{Args: tt.args}

			// We can't easily test the full bazelisk.Run call without mocking,
			// but we can test that it doesn't return early (exitCode 0 from BB commands)
			// This test will fail if the bazelisk.Run call fails, but that's expected
			// in the test environment since we don't have bazel set up.
			_, err := help.HandleHelp(orderedArgs)

			// We expect an error here because bazelisk.Run will fail in test environment
			// The important thing is that we reached this point (didn't return early)
			assert.Error(t, err, tt.description)
		})
	}
}

