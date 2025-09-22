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

// Helper function to test help functionality with captured stdout
func testHelpWithArgs(t *testing.T, args []arguments.Argument, expectSuccess bool, expectedUsage string, testDescription string) {
	t.Helper()

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	orderedArgs := &parsed.OrderedArgs{Args: args}
	exitCode, err := help.HandleHelp(orderedArgs)

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	io.Copy(&buf, r)
	output := buf.String()

	if expectSuccess {
		require.NoError(t, err, "Help should work for %s", testDescription)
		require.Equal(t, 0, exitCode, "Exit code should be 0 for %s", testDescription)
		if expectedUsage != "" {
			require.Contains(t, output, expectedUsage, "Output should contain expected usage for %s", testDescription)
		}
		require.NotEmpty(t, output, "Output should not be empty for %s", testDescription)
	}
}

func testBBCommandAllPatterns(t *testing.T, cmdName, expectedCmdName string) {
	t.Helper()

	{
		args := []arguments.Argument{
			&arguments.PositionalArgument{Value: "help"},
			&arguments.PositionalArgument{Value: cmdName},
		}
		testHelpWithArgs(t, args, true, "Usage: bb "+expectedCmdName, "bb help "+cmdName)
	}

	{
		args := []arguments.Argument{
			&arguments.PositionalArgument{Value: cmdName},
			&arguments.PositionalArgument{Value: "-h"},
		}
		testHelpWithArgs(t, args, true, "Usage: bb "+expectedCmdName, "bb "+cmdName+" -h")
	}

	{
		args := []arguments.Argument{
			&arguments.PositionalArgument{Value: cmdName},
			&arguments.PositionalArgument{Value: "--help"},
		}
		testHelpWithArgs(t, args, true, "Usage: bb "+expectedCmdName, "bb "+cmdName+" --help")
	}
}

func testBazelCommandAllPatterns(t *testing.T, cmdName string) {
	t.Helper()

	{
		orderedArgs := &parsed.OrderedArgs{Args: []arguments.Argument{
			&arguments.PositionalArgument{Value: "help"},
			&arguments.PositionalArgument{Value: cmdName},
		}}

		targetCommand := help.FindTargetCommandFromHelpArgs(orderedArgs)
		require.Equal(t, cmdName, targetCommand, "Should find target command: %s", cmdName)

		result := help.TryShowBBCommandHelp(targetCommand)
		require.False(t, result, "Should not handle %s as BB command", cmdName)
	}

	{
		orderedArgs := &parsed.OrderedArgs{Args: []arguments.Argument{
			&arguments.PositionalArgument{Value: cmdName},
			&arguments.PositionalArgument{Value: "-h"},
		}}

		targetCommand := help.FindTargetCommandFromHelpArgs(orderedArgs)
		require.Equal(t, cmdName, targetCommand, "Should find target command: %s", cmdName)

		result := help.TryShowBBCommandHelp(targetCommand)
		require.False(t, result, "Should not handle %s as BB command", cmdName)
	}

	{
		orderedArgs := &parsed.OrderedArgs{Args: []arguments.Argument{
			&arguments.PositionalArgument{Value: cmdName},
			&arguments.PositionalArgument{Value: "--help"},
		}}

		targetCommand := help.FindTargetCommandFromHelpArgs(orderedArgs)
		require.Equal(t, cmdName, targetCommand, "Should find target command: %s", cmdName)

		result := help.TryShowBBCommandHelp(targetCommand)
		require.False(t, result, "Should not handle %s as BB command", cmdName)
	}
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
		t.Run(cmdName, func(t *testing.T) {
			testBBCommandAllPatterns(t, cmdName, cmdName)
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
		t.Run(alias, func(t *testing.T) {
			testBBCommandAllPatterns(t, alias, originalCmd)
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
		// Do not test "help".
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
		t.Run(cmdName, func(t *testing.T) {
			testBazelCommandAllPatterns(t, cmdName)
		})
	}
}
