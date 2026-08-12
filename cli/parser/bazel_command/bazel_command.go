package bazel_command

import (
	"iter"
	"maps"

	"github.com/buildbuddy-io/buildbuddy/server/util/lib/seq"
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

	childrenByCommand = seq.Accumulate(
		maps.Keys(parentByCommand),
		map[string]set.Set[string]{},
		func(m map[string]set.Set[string], child string) map[string]set.Set[string] {
			if m[parentByCommand[child]] == nil {
				m[parentByCommand[child]] = set.Set[string]{}
			}
			m[parentByCommand[child]].Add(child)
			return m
		},
	)
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

// Children returns all commands that inherit from the command, even indirectly.
func Children(command string) iter.Seq[string] {
	return func(yield func(string) bool) {
		stack := []string{command}
		for len(stack) != 0 {
			parent := stack[len(stack)-1]
			stack = stack[:len(stack)-1]
			if children, ok := childrenByCommand[parent]; ok {
				for child := range children {
					if !yield(child) {
						return
					}
					stack = append(stack, child)
				}
			}
		}
	}
}
