package help_test

import (
	"bytes"
	"io"
	"os"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/cli_command/register"
	"github.com/buildbuddy-io/buildbuddy/cli/help"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
	"github.com/stretchr/testify/require"
)

func testBBCommandHelp(t *testing.T, cmdName string, expectedCmdName string) {
	t.Helper()

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	orderedArgs := &parsed.OrderedArgs{Args: []arguments.Argument{
		&arguments.PositionalArgument{Value: "help"},
		&arguments.PositionalArgument{Value: cmdName},
	}}
	exitCode, err := help.HandleHelp(orderedArgs)

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	require.NoError(t, err, "Help should work for BB command: %s", cmdName)
	require.Equal(t, 0, exitCode, "Exit code should be 0 for BB command: %s", cmdName)
	require.Contains(t, output, "Usage: bb "+expectedCmdName, "Output should contain usage for: %s", expectedCmdName)
	require.NotEmpty(t, output, "Output should not be empty for BB command: %s", cmdName)
}

func TestHelpForBBCommands(t *testing.T) {
	register.Register()

	bbCommandNames := []string{
		"add",
		"analyze",
		"ask",
		"download",
		"execute",
		"explain",
		"fix",
		"index",
		"login",
		"print",
		"remote",
		"remote-download",
		"search",
		"update",
		"upload",
		"version",
		"view",
		"install",
		"logout",
	}

	for _, cmdName := range bbCommandNames {
		t.Run("bb help "+cmdName, func(t *testing.T) {
			testBBCommandHelp(t, cmdName, cmdName)
		})
	}
}

func TestHelpAliases(t *testing.T) {
	register.Register()

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

func TestHelpBazelCommands(t *testing.T) {
	register.Register()

	bazelCommands := []string{
		"analyze-profile",
		"aquery",
		"build",
		"canonicalize-flags",
		"clean",
		"coverage",
		"cquery",
		"dump",
		"fetch",
		"help",
		"info",
		"license",
		"mobile-install",
		"mod",
		"print_action",
		"query",
		"run",
		"shutdown",
		"sync",
		"test",
		"vendor",
		// Do not test "version", since this is a BB CLI command.
	}

	for _, cmdName := range bazelCommands {
		t.Run("bb help "+cmdName+" (not a BB command)", func(t *testing.T) {
			orderedArgs := &parsed.OrderedArgs{Args: []arguments.Argument{
				&arguments.PositionalArgument{Value: "help"},
				&arguments.PositionalArgument{Value: cmdName},
			}}

			targetCommand := help.FindTargetCommandFromHelpArgs(orderedArgs)
			require.Equal(t, cmdName, targetCommand, "Should find target command: %s", cmdName)

			result := help.TryShowBBCommandHelp(targetCommand)
			require.False(t, result, "Should not handle %s as BB command", cmdName)
		})
	}
}
