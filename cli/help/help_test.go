package help_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	register_cli_commands "github.com/buildbuddy-io/buildbuddy/cli/cli_command/register"
	"github.com/buildbuddy-io/buildbuddy/cli/help"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Helper function to test BB CLI command help
func testBBCommandHelp(t *testing.T, cmdName string, expectedCmdName string) {
	t.Helper()

	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	orderedArgs := &parsed.OrderedArgs{Args: []arguments.Argument{
		&arguments.PositionalArgument{Value: "help"},
		&arguments.PositionalArgument{Value: cmdName},
	}}
	exitCode, err := help.HandleHelp(orderedArgs)

	// Restore stdout
	w.Close()
	os.Stdout = oldStdout

	// Read captured output
	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	require.NoError(t, err, "Help should work for BB command: %s", cmdName)
	assert.Equal(t, 0, exitCode, "Exit code should be 0 for BB command: %s", cmdName)
	assert.Contains(t, output, "Usage: bb "+expectedCmdName, "Output should contain usage for: %s", expectedCmdName)
	assert.NotEmpty(t, output, "Output should not be empty for BB command: %s", cmdName)
}

// TestHelpForBBCommands exhaustively tests help for all BB CLI commands found under cli/
func TestHelpForBBCommands(t *testing.T) {
	// Register all CLI commands
	register_cli_commands.Register()

	// All BB CLI command names found under cli/ directories
	bbCommandNames := []string{
		"add",             // cli/add
		"analyze",         // cli/analyze
		"ask",             // cli/ask (with aliases wtf, huh)
		"download",        // cli/download
		"execute",         // cli/execute
		"explain",         // cli/explain
		"fix",             // cli/fix
		"index",           // cli/index
		"login",           // cli/login (includes logout functionality)
		"print",           // cli/printlog
		"remote",          // cli/remotebazel
		"remote-download", // cli/remote_download
		"search",          // cli/search
		"update",          // cli/update
		"upload",          // cli/upload
		"version",         // cli/versioncmd
		"view",            // cli/view
		"install",         // cli/plugin (install command)
		"logout",          // cli/login (logout command)
	}

	// Test "bb help <command>" for each BB command
	for _, cmdName := range bbCommandNames {
		t.Run("bb help "+cmdName, func(t *testing.T) {
			testBBCommandHelp(t, cmdName, cmdName)
		})
	}
}

// TestHelpAliases tests help for all known BB CLI command aliases
func TestHelpAliases(t *testing.T) {
	// Register all CLI commands
	register_cli_commands.Register()

	// Test known aliases
	aliases := map[string]string{
		"wtf": "ask",
		"huh": "ask",
	}

	for alias, originalCmd := range aliases {
		t.Run("bb help "+alias+" (alias)", func(t *testing.T) {
			testBBCommandHelp(t, alias, originalCmd)
		})
	}
}

// TestHelpBazelCommands tests that Bazel commands are not handled as BB commands
func TestHelpBazelCommands(t *testing.T) {
	// Register all CLI commands
	register_cli_commands.Register()

	// Test common Bazel commands - these should NOT be recognized as BB commands
	bazelCommands := []string{
		"build", "test", "run", "query", "cquery", "aquery", "clean", "info",
		"coverage", "fetch", "sync", "dump", "shutdown",
	}

	for _, cmdName := range bazelCommands {
		t.Run("bb help "+cmdName+" (not a BB command)", func(t *testing.T) {
			// Test that this command is not recognized as a BB CLI command
			// by checking our helper functions directly
			orderedArgs := &parsed.OrderedArgs{Args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "help"},
				&arguments.PositionalArgument{Value: cmdName},
			}}

			// This should find the target command
			targetCommand := help.FindTargetCommandFromHelpArgs(orderedArgs)
			assert.Equal(t, cmdName, targetCommand, "Should find target command: %s", cmdName)

			// But TryShowBBCommandHelp should return false (not a BB command)
			// Capture stdout to avoid polluting test output
			oldStdout := os.Stdout
			_, w, _ := os.Pipe()
			os.Stdout = w

			result := help.TryShowBBCommandHelp(targetCommand)

			// Restore stdout
			w.Close()
			os.Stdout = oldStdout

			assert.False(t, result, "Should not handle %s as BB command", cmdName)

			// Note: We don't test the full HandleHelp because that would require
			// mocking bazelisk.Run or having Bazel installed. The important thing
			// is that these commands are correctly identified as non-BB commands
			// and would be forwarded to Bazel.
		})
	}
}
