package arg

import (
	"flag"
	"fmt"
	"io"
	"strings"
)

// Returns true if the list of args contains desiredArg
func Has(args []string, desiredArg string) bool {
	arg, _, _ := Find(args, desiredArg)
	return arg != ""
}

// Returns the value of the given desiredArg if contained in args
func Get(args []string, desiredArg string) string {
	arg, _, _ := FindLast(args, desiredArg)
	return arg
}

// GetMulti returns all occurrences of the flag with the given name which
// is allowed to be specified more than once.
func GetMulti(args []string, name string) []string {
	var out []string
	for {
		value, i, length := Find(args, name)
		if i < 0 {
			return out
		}
		out = append(out, value)
		args = args[i+length:]
	}
}

// Returns the value of the given desiredArg and a slice with that arg removed
func Pop(args []string, desiredArg string) (string, []string) {
	arg, i, length := Find(args, desiredArg)
	if i < 0 {
		return "", args
	}
	return arg, append(args[:i], args[i+length:]...)
}

// Returns a list with argToRemove removed (if it exists)
func Remove(args []string, argToRemove string) []string {
	_, remaining := Pop(args, argToRemove)
	return remaining
}

// Helper method for finding arguments by prefix within a list of arguments
func Find(args []string, desiredArg string) (value string, index int, length int) {
	exact := fmt.Sprintf("--%s", desiredArg)
	prefix := fmt.Sprintf("--%s=", desiredArg)
	for i, arg := range args {
		// Handle "--name", "value" form
		if arg == exact && i+1 < len(args) {
			return args[i+1], i, 2
		}
		// Handle "--name=value" form
		if strings.HasPrefix(arg, prefix) {
			return strings.TrimPrefix(arg, prefix), i, 1
		}
	}
	return "", -1, 0
}

// FindLast returns the value corresponding to the last occurrence of the given
// argument.
func FindLast(args []string, desiredArg string) (value string, index int, length int) {
	start := 0
	lastValue, lastIndex, lastLength := "", -1, 0
	for start < len(args) {
		value, index, length := Find(args[start:], desiredArg)
		if index == -1 {
			break
		}
		lastValue, lastIndex, lastLength = value, start+index, length
		start = start + index + length
	}
	return lastValue, lastIndex, lastLength
}

// Returns the first non-option found in the list of args (doesn't begin with "-")
func GetCommand(args []string) string {
	command, _ := GetCommandAndIndex(args)
	return command
}

// Returns the first non-option found in the list of args (doesn't begin with "-") and the index at which it was found
func GetCommandAndIndex(args []string) (string, int) {
	for i, arg := range args {
		if strings.HasPrefix(arg, "-") {
			continue
		}
		return arg, i
	}
	return "", -1
}

// Returns any "passthrough" arugments in args (those to the right of the first " -- ", if any)
func GetPassthroughArgs(args []string) []string {
	for i, arg := range args {
		if arg == "--" {
			return append([]string{}, args[i+1:]...)
		}
	}
	return nil
}

// Returns any non "passthrough" arugments in args (those to the left of the first " -- ", if any)
func GetNonPassthroughArgs(args []string) []string {
	splitIndex := len(args)
	for i, arg := range args {
		if arg == "--" {
			splitIndex = i
			break
		}
	}
	return append([]string{}, args[:splitIndex]...)
}

// Splits bazel args and passthrough args into two separate lists. The first
// "--" separator is dropped if it exists.
func SplitPassthroughArgs(args []string) (bazel []string, passthrough []string) {
	return GetNonPassthroughArgs(args), GetPassthroughArgs(args)
}

// JoinPassthroughArgs joins the given args and passthrough args with a "--"
// separator, if the passthrough args are non-empty. Otherwise it returns
// the original non-passthrough args.
func JoinPassthroughArgs(args, passthroughArgs []string) []string {
	out := append([]string{}, args...)
	if len(passthroughArgs) == 0 {
		return out
	}
	out = append(out, "--")
	out = append(out, passthroughArgs...)
	return out
}

// Returns args with any arguments also found in existingArgs removed
func RemoveExistingArgs(args []string, existingArgs []string) []string {
	exists := make(map[string]struct{}, len(existingArgs))
	for _, x := range existingArgs {
		exists[x] = struct{}{}
	}
	var diff []string
	for _, x := range args {
		if _, found := exists[x]; !found {
			diff = append(diff, x)
		}
	}
	return diff
}

// ContainsExact returns whether the slice `args` contains the literal string
// `value`.
func ContainsExact(args []string, value string) bool {
	for _, v := range args {
		if v == value {
			return true
		}
	}
	return false
}

// ParseFlagSet works like flagset.Parse(), except it allows positional
// arguments and flags to be specified in any order.
func ParseFlagSet(flagset *flag.FlagSet, args []string) error {
	flagset.SetOutput(io.Discard)
	var positionalArgs []string
	for {
		if err := flagset.Parse(args); err != nil {
			return err
		}
		// Consume all the flags that were parsed as flags.
		args = args[len(args)-flagset.NArg():]
		if len(args) == 0 {
			break
		}
		// There's at least one flag remaining and it must be a positional arg
		// since we consumed all args that were parsed as flags. Consume just
		// the first one, and retry parsing with the rest of the args, which may
		// include flags.
		positionalArgs = append(positionalArgs, args[0])
		args = args[1:]
	}
	// Parse just the positional args so that flagset.Args() returns the
	// expected value.
	return flagset.Parse(positionalArgs)
}
