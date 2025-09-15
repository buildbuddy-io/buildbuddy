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
			name: "bazel command: help",
			args: []string{"help"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "",
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
		// bb commands from bb-help.txt
		{
			name: "bb command: add",
			args: []string{"add"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "add",
				TruncatedArgs: []string{"add"},
			},
		},
		{
			name: "bb command: analyze",
			args: []string{"analyze"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "analyze",
				TruncatedArgs: []string{"analyze"},
			},
		},
		{
			name: "bb command: ask",
			args: []string{"ask"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "ask",
				TruncatedArgs: []string{"ask"},
			},
		},
		{
			name: "bb command: download",
			args: []string{"download"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "download",
				TruncatedArgs: []string{"download"},
			},
		},
		{
			name: "bb command: execute",
			args: []string{"execute"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "execute",
				TruncatedArgs: []string{"execute"},
			},
		},
		{
			name: "bb command: fix",
			args: []string{"fix"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "fix",
				TruncatedArgs: []string{"fix"},
			},
		},
		{
			name: "bb command: install",
			args: []string{"install"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "install",
				TruncatedArgs: []string{"install"},
			},
		},
		{
			name: "bb command: logout",
			args: []string{"logout"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "logout",
				TruncatedArgs: []string{"logout"},
			},
		},
		{
			name: "bb command: print",
			args: []string{"print"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "print",
				TruncatedArgs: []string{"print"},
			},
		},
		{
			name: "bb command: remote",
			args: []string{"remote"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "remote",
				TruncatedArgs: []string{"remote"},
			},
		},
		{
			name: "bb command: remote-download",
			args: []string{"remote-download"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "remote-download",
				TruncatedArgs: []string{"remote-download"},
			},
		},
		{
			name: "bb command: search",
			args: []string{"search"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "search",
				TruncatedArgs: []string{"search"},
			},
		},
		{
			name: "bb command: index",
			args: []string{"index"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "index",
				TruncatedArgs: []string{"index"},
			},
		},
		{
			name: "bb command: update",
			args: []string{"update"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "update",
				TruncatedArgs: []string{"update"},
			},
		},
		{
			name: "bb command: upload",
			args: []string{"upload"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "upload",
				TruncatedArgs: []string{"upload"},
			},
		},
		{
			name: "bb command: explain",
			args: []string{"explain"},
			expected: TestResult{
				OptionNames:   []string{},
				CommandName:   "explain",
				TruncatedArgs: []string{"explain"},
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
