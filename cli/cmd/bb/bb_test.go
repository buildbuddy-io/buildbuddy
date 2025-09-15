package bb

import (
	"testing"

	register_cli_commands "github.com/buildbuddy-io/buildbuddy/cli/cli_command/register"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	// Ensure CLI commands are registered for testing
	register_cli_commands.Register()
}

func TestInterpretAsBBCliCommand(t *testing.T) {
	tests := []struct {
		name            string
		args            []string
		expectOpts      bool // whether we expect non-nil options
		expectCommand   *string // expected command name, nil if no command expected
		expectTruncated []string // expected truncated args
	}{
		{
			name:            "valid bb command without options",
			args:            []string{"login"},
			expectOpts:      true,
			expectCommand:   stringPtr("login"),
			expectTruncated: []string{"login"},
		},
		{
			name:            "valid bb command with options",
			args:            []string{"--verbose", "login"},
			expectOpts:      true,
			expectCommand:   stringPtr("login"),
			expectTruncated: []string{"login"},
		},
		{
			name:            "valid bb command with multiple options",
			args:            []string{"--verbose", "--watch", "build", "//..."},
			expectOpts:      true,
			expectCommand:   stringPtr("build"),
			expectTruncated: []string{"build", "//..."},
		},
		{
			name:            "valid bb command alias",
			args:            []string{"ask"},
			expectOpts:      true,
			expectCommand:   stringPtr("ask"),
			expectTruncated: []string{"ask"},
		},
		{
			name:            "valid bb command alias wtf",
			args:            []string{"wtf"},
			expectOpts:      true,
			expectCommand:   stringPtr("ask"), // wtf is an alias for ask
			expectTruncated: []string{"wtf"},
		},
		{
			name:            "bazel command (not bb command)",
			args:            []string{"build", "//..."},
			expectOpts:      false,
			expectCommand:   nil,
			expectTruncated: []string{"build", "//..."},
		},
		{
			name:            "bazel command with options",
			args:            []string{"--compilation_mode=opt", "build", "//..."},
			expectOpts:      false,
			expectCommand:   nil,
			expectTruncated: []string{"--compilation_mode=opt", "build", "//..."},
		},
		{
			name:            "unknown command",
			args:            []string{"unknown_command"},
			expectOpts:      false,
			expectCommand:   nil,
			expectTruncated: []string{"unknown_command"},
		},
		{
			name:            "empty args",
			args:            []string{},
			expectOpts:      false,
			expectCommand:   nil,
			expectTruncated: []string{},
		},
		{
			name:            "only options without command",
			args:            []string{"--verbose"},
			expectOpts:      false,
			expectCommand:   nil,
			expectTruncated: []string{"--verbose"},
		},
		{
			name:            "help command with options",
			args:            []string{"--verbose", "help", "build"},
			expectOpts:      true,
			expectCommand:   stringPtr("help"),
			expectTruncated: []string{"help", "build"},
		},
		{
			name:            "bb command with additional args",
			args:            []string{"search", "pattern", "--path", "src/"},
			expectOpts:      true,
			expectCommand:   stringPtr("search"),
			expectTruncated: []string{"search", "pattern", "--path", "src/"},
		},
		{
			name:            "remote command",
			args:            []string{"remote", "build", "//..."},
			expectOpts:      true,
			expectCommand:   stringPtr("remote"),
			expectTruncated: []string{"remote", "build", "//..."},
		},
		{
			name:            "version command",
			args:            []string{"version"},
			expectOpts:      true,
			expectCommand:   stringPtr("version"),
			expectTruncated: []string{"version"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, command, truncatedArgs := InterpretAsBBCliCommand(tt.args)

			// Check options
			if tt.expectOpts {
				assert.NotNil(t, opts, "Expected non-nil options")
			} else {
				assert.Nil(t, opts, "Expected nil options")
			}

			// Check command
			if tt.expectCommand != nil {
				require.NotNil(t, command, "Expected non-nil command")
				assert.Equal(t, *tt.expectCommand, command.Name, "Command name mismatch")
			} else {
				assert.Nil(t, command, "Expected nil command")
			}

			// Check truncated args
			assert.Equal(t, tt.expectTruncated, truncatedArgs, "Truncated args mismatch")
		})
	}
}

func TestInterpretAsBBCliCommand_WithUnknownOption(t *testing.T) {
	// Test case where unknown option causes function to return original args
	args := []string{"--unknown-startup-option", "login"}
	opts, command, truncatedArgs := InterpretAsBBCliCommand(args)

	assert.Nil(t, opts, "Expected nil options due to unknown option")
	assert.Nil(t, command, "Expected nil command due to unknown option")
	assert.Equal(t, args, truncatedArgs, "Expected original args returned")
}

func TestInterpretAsBBCliCommand_AllRegisteredCommands(t *testing.T) {
	// Test that all registered commands are recognized
	expectedCommands := []string{
		"add", "analyze", "ask", "download", "execute", "fix", "install",
		"login", "logout", "print", "remote", "remote-download", "search",
		"index", "update", "upload", "version", "explain",
	}

	for _, cmdName := range expectedCommands {
		t.Run("command_"+cmdName, func(t *testing.T) {
			args := []string{cmdName}
			opts, command, truncatedArgs := InterpretAsBBCliCommand(args)

			assert.NotNil(t, opts, "Expected non-nil options for command %s", cmdName)
			require.NotNil(t, command, "Expected non-nil command for %s", cmdName)
			assert.Equal(t, cmdName, command.Name, "Command name mismatch for %s", cmdName)
			assert.Equal(t, []string{cmdName}, truncatedArgs, "Truncated args mismatch for %s", cmdName)
		})
	}
}

func TestInterpretAsBBCliCommand_Aliases(t *testing.T) {
	// Test command aliases
	aliases := map[string]string{
		"wtf": "ask",
		"huh": "ask",
	}

	for alias, expectedCmd := range aliases {
		t.Run("alias_"+alias, func(t *testing.T) {
			args := []string{alias}
			opts, command, truncatedArgs := InterpretAsBBCliCommand(args)

			assert.NotNil(t, opts, "Expected non-nil options for alias %s", alias)
			require.NotNil(t, command, "Expected non-nil command for alias %s", alias)
			assert.Equal(t, expectedCmd, command.Name, "Command name mismatch for alias %s", alias)
			assert.Equal(t, []string{alias}, truncatedArgs, "Truncated args mismatch for alias %s", alias)
		})
	}
}

// Helper function to create string pointers for test cases
func stringPtr(s string) *string {
	return &s
}