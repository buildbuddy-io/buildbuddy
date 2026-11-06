package bazel_command

import (
	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
)

var (
	commands = set.Set[string]{
		"analyze-profile":    {},
		"aquery":             {},
		"build":              {},
		"canonicalize-flags": {},
		"clean":              {},
		"config":             {},
		"coverage":           {},
		"cquery":             {},
		"dump":               {},
		"fetch":              {},
		"help":               {},
		"info":               {},
		"license":            {},
		"mobile-install":     {},
		"mod":                {},
		"print_action":       {},
		"query":              {},
		"run":                {},
		"shutdown":           {},
		"sync":               {},
		"test":               {},
		"vendor":             {},
		"version":            {},
	}

	// Inheritance hierarchy: https://bazel.build/run/bazelrc#option-defaults
	// All commands inherit options from "common".
	parentByCommand = map[string]string{
		"aquery":             "build",
		"canonicalize-flags": "build",
		"clean":              "build",
		"config":             "build",
		"info":               "build",
		"license":            "build",
		"mobile-install":     "build",
		"print_action":       "build",
		"run":                "build",
		"test":               "build",

		"coverage": "test",
		"cquery":   "test",
		"fetch":    "test",
		"vendor":   "test",
	}
)

// Commands returns a read-only view of all recognized Bazel commands.
func Commands() set.View[string] {
	return set.KeyView(commands)
}

// IsCommand returns whether command is recognized as a Bazel command.
func IsCommand(command string) bool {
	return commands.Contains(command)
}

// Parent returns the command from which command inherits, or an empty string
// if command has no parent.
func Parent(command string) string {
	return parentByCommand[command]
}

// GetCommandAndIndex returns the bazel command in args and its index, or an
// empty string and -1 if args contains no bazel command.
// Ex. For `bazel build //...` it will return `build` and `1`.
// This can be helpful for splitting bazel commands into their different
// components.
//
// We hard-code the commands here because running `bazel help` to generate them
// can result in undesirable behavior. For example if it's run with different
// startup options than the last bazel command, it will restart the bazel
// server.
//
// TODO: More robust parsing of startup options. For example, this has a bug
// that passing `bazel --output_base build test ...` returns "build" as the
// bazel command, even though "build" is the argument to --output_base.
func GetCommandAndIndex(args []string) (string, int) {
	for i, a := range args {
		// Check for bazel commands first, since a few names (like "version")
		// are both a bazel command and a bb CLI command, and bazel wins when
		// the args are being interpreted as a bazel command at all.
		if IsCommand(a) {
			return a, i
		}
		if cli_command.IsCommand(a) {
			return "", -1
		}
	}
	return "", -1
}
