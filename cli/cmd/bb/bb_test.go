package bb

import (
	"testing"

	register_cli_commands "github.com/buildbuddy-io/buildbuddy/cli/cli_command/register"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
)

func init() {
	// Ensure CLI commands are registered for testing
	register_cli_commands.Register()
}

type TestResult struct {
	OptionNames   []string
	CommandName   string
	TruncatedArgs []string
}

func TestInterpretAsBBCliCommand(t *testing.T) {
	tcs := []struct {
		name     string
		args     []string
		expected TestResult
	}{
		{
			name: "valid bb command without options",
			args: []string{"login"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "login",
				TruncatedArgs: []string{"login"},
			},
		},
		{
			name: "valid bb command with options",
			args: []string{"--verbose", "login"},
			expected: TestResult{
				OptionNames:   []string{"verbose"},
				CommandName:   "login",
				TruncatedArgs: []string{"login"},
			},
		},
		{
			name: "bazel command (not bb command)",
			args: []string{"build", "//..."},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"build", "//..."},
			},
		},
		{
			name: "unknown command",
			args: []string{"unknown_command"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"unknown_command"},
			},
		},
		{
			name: "empty args",
			args: []string{},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			opts, command, truncatedArgs := InterpretAsBBCliCommand(tc.args)
			optionNames := []string{}
			for _, opt := range opts {
				optionNames = append(optionNames, opt.Name())
			}
			commandName := ""
			if command != nil {
				commandName = command.Name
			}

			actual := TestResult{
				OptionNames:   optionNames,
				CommandName:   commandName,
				TruncatedArgs: truncatedArgs,
			}

			assert.Empty(t, cmp.Diff(tc.expected, actual))
		})
	}
}
