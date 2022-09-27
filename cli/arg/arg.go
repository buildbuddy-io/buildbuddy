package arg

import (
	"strings"
)

// Returns true if the list of args contains an arg with the prefix desiredArg
func HasArg(args []string, desiredArg string) bool {
	arg, _ := getArg(args, desiredArg, true)
	return arg != ""
}

// If the list of args contains an arg with the prefix desiredArg, returns the arg with the prefix trimmed
func GetArg(args []string, desiredArg string) string {
	arg, _ := getArg(args, desiredArg, true)
	return arg
}

// If the list containes an arg with the prefix desiredArg, return the arg without the prefix and a slice with that arg removed
func PopArg(args []string, desiredArg string) (string, []string) {
	arg, i := getArg(args, desiredArg, true)
	if i < 0 {
		return "", args
	}
	return arg, append(args[:i], args[i+1:]...)
}

// Returns a list with argToRemove removed (if it exists)
func RemoveArg(args []string, argToRemove string) []string {
	_, remaining := PopArg(args, argToRemove)
	return remaining
}

// Helper method for finding arguments within a list of arguments
func getArg(args []string, desiredArg string, prefix bool) (string, int) {
	for i, arg := range args {
		if (prefix && strings.HasPrefix(arg, desiredArg)) || (!prefix && desiredArg == arg) {
			return strings.TrimPrefix(arg, desiredArg), i
		}
	}
	return "", -1
}

// Returns the first non-option found in the list of args (doesn't begin with "-")
func GetCommand(args []string) string {
	command, _ := GetCommandAndIndex(args)
	return command
}

// Returns the first non-option found in the list of args (doesn't begin with "-") and the index at which it was found
func GetCommandAndIndex(args []string) (string, int) {
	for i, arg := range args {
		if strings.HasPrefix("-", arg) {
			continue
		}
		return arg, i
	}
	return "", -1
}

// Returns any "passthrough" arugments in args (those to the right of the first " -- ", if any)
func GetPassthroughArgs(args []string) []string {
	_, i := getArg(args, "--", false)
	if i < 0 {
		return []string{}
	}
	return args[i+1:]
}

// Returns any non "passthrough" arugments in args (those to the left of the first " -- ", if any)
func GetNonPassthroughArgs(args []string) []string {
	_, i := getArg(args, "--", false)
	if i < 0 {
		return args
	}
	return args[:i]
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
