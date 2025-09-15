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
		// Bazel commands from bb-help.txt
		{
			name: "bazel command: analyze-profile",
			args: []string{"analyze-profile"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"analyze-profile"},
			},
		},
		{
			name: "bazel command: aquery",
			args: []string{"aquery"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"aquery"},
			},
		},
		{
			name: "bazel command: canonicalize-flags",
			args: []string{"canonicalize-flags"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"canonicalize-flags"},
			},
		},
		{
			name: "bazel command: clean",
			args: []string{"clean"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"clean"},
			},
		},
		{
			name: "bazel command: coverage",
			args: []string{"coverage"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"coverage"},
			},
		},
		{
			name: "bazel command: cquery",
			args: []string{"cquery"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"cquery"},
			},
		},
		{
			name: "bazel command: dump",
			args: []string{"dump"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"dump"},
			},
		},
		{
			name: "bazel command: fetch",
			args: []string{"fetch"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"fetch"},
			},
		},
		{
			name: "bazel command: help (recognized as bb help)",
			args: []string{"help"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "help",
				TruncatedArgs: []string{"help"},
			},
		},
		{
			name: "bazel command: info",
			args: []string{"info"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"info"},
			},
		},
		{
			name: "bazel command: license",
			args: []string{"license"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"license"},
			},
		},
		{
			name: "bazel command: mobile-install",
			args: []string{"mobile-install"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"mobile-install"},
			},
		},
		{
			name: "bazel command: mod",
			args: []string{"mod"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"mod"},
			},
		},
		{
			name: "bazel command: print_action",
			args: []string{"print_action"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"print_action"},
			},
		},
		{
			name: "bazel command: query",
			args: []string{"query"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"query"},
			},
		},
		{
			name: "bazel command: run",
			args: []string{"run"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"run"},
			},
		},
		{
			name: "bazel command: shutdown",
			args: []string{"shutdown"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"shutdown"},
			},
		},
		{
			name: "bazel command: sync",
			args: []string{"sync"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"sync"},
			},
		},
		{
			name: "bazel command: test",
			args: []string{"test"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"test"},
			},
		},
		{
			name: "bazel command: vendor",
			args: []string{"vendor"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
				TruncatedArgs: []string{"vendor"},
			},
		},
		{
			name: "bazel command: version (recognized as bb version)",
			args: []string{"version"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "version",
				TruncatedArgs: []string{"version"},
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
