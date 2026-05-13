package arg

import (
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/parser"
)

// TODO(#7216): Add getters for these fields, so they can't be modified directly, potentially breaking
// the contract for how they should be simultaneously updated.
type BazelArgs struct {
	// TODO(#7216): Add a Forwarded args fields that represents the non-expanded args
	// that are eventually passed to Bazelisk.

	// Resolved are the Bazel args that are used internally within the bb parser.
	// --config and --bazelrc flags are expanded, so the parser has a complete view of the args.
	Resolved []string
}

// New returns a BazelArgs struct from a slice of bazel args.
func NewBazelArgs(args []string) (*BazelArgs, error) {
	parsed := &BazelArgs{}
	if err := parsed.Set(args); err != nil {
		return nil, err
	}
	return parsed, nil
}

// Set updates the BazelArgs struct with a new slice of bazel args.
// It also recomputes the resolved args.
func (a *BazelArgs) Set(args []string) error {
	// Normalize args - apply consistent option representation.
	normalizedArgs, err := parser.CanonicalizeArgs(args)
	if err != nil {
		return err
	}
	// TODO(#7216): Set the Forwarded args field.
	return a.resolve(normalizedArgs)
}

// Append adds a new bazel arg.
func (a *BazelArgs) Append(arg string) error {
	// TODO(#7216): Set the Forwarded args field.

	newArgs := Append(a.Resolved, arg)

	resolve, err := requiresResolve(arg)
	if err != nil {
		return err
	}
	if resolve {
		return a.Set(newArgs)
	}
	a.Resolved = newArgs
	return nil
}

// Prepend adds a new bazel arg to the beginning of the list of args, just after the bazel command.
// If the same flag is specified multiple times, Bazel will use the last value. This is useful for adding flags that should
// be overridden by later flags.
func (a *BazelArgs) Prepend(arg string) error {
	// TODO(#7216): Set the Forwarded args field.

	updatedResolvedArgs := prepend(a.Resolved, arg)
	resolve, err := requiresResolve(arg)
	if err != nil {
		return err
	}
	if resolve {
		return a.Set(updatedResolvedArgs)
	}
	a.Resolved = updatedResolvedArgs
	return nil
}

func prepend(args []string, arg string) []string {
	_, commandIndex := parser.GetBazelCommandAndIndex(args)
	if commandIndex == -1 {
		return append([]string{arg}, args...)
	}

	out := append([]string{}, args[:commandIndex+1]...)
	out = append(out, arg)
	out = append(out, args[commandIndex+1:]...)
	return out
}

// requiresResolve returns true if the arg can change rc/config expansion.
func requiresResolve(arg string) (bool, error) {
	flagName, _ := SplitOptionValue(arg)
	flagName = strings.TrimPrefix(flagName, "--")

	// TODO(#7216): Remove this check once we add the Forwarded args field and can safely resolve --bazelrc.
	if flagName == "bazelrc" {
		return false, fmt.Errorf("cannot resolve --bazelrc with BazelArgs")
	}

	return flagName == "config" || flagName == "bazelrc", nil
}

// Get returns the value of a flag.
// It reads from the resolved args to ensure that flags expanded from --config or --bazelrc are included.
func (a *BazelArgs) Get(flagName string) string {
	return Get(a.Resolved, flagName)
}

func (a *BazelArgs) Has(flagName string) bool {
	return a.Get(flagName) != ""
}

// TODO(#7216): Read the Forwarded args field, instead of passing in args.
// resolve re-evaluates the args and expands all flags.
func (a *BazelArgs) resolve(args []string) error {
	// Expand --config and --bazelrc flags, then normalize the result.
	resolved, err := parser.ResolveAndCanonicalizeArgs(args)
	if err != nil {
		return err
	}
	a.Resolved = resolved
	return nil
}

func (a *BazelArgs) GetTargets() []string {
	return GetTargets(a.Resolved)
}

func (a *BazelArgs) GetCommand() string {
	return GetCommand(a.Resolved)
}

func (a *BazelArgs) GetAllFlagsWithName(flagName string) []string {
	return GetMulti(a.Resolved, flagName)
}

// StripBBFlag removes a CLI-only string flag from the args (so it is
// not passed to Bazelisk) and returns its value.
func (a *BazelArgs) StripBBFlag(flagName string) (string, error) {
	// TODO(#7216): Read the Forwarded args field - we can't strip a field set in a rc file.
	parsed, err := parser.ParseArgs(a.Resolved)
	if err != nil {
		return "", err
	}
	// Remove the flag from the parsed args and return its value.
	flagVal, err := parser.GetCLICommandOptionVal(parsed, flagName)
	if err != nil {
		return "", err
	}
	// Update the args to not have the removed flag.
	// Use direct assignment rather than Set() to avoid re-resolving rc/config
	// flags, which would fail if a plugin added a  --config flag.
	// TODO(#7216): This will be cleaner when we can safely re-resolve the Forwarded args.
	a.Resolved = parsed.Format()
	return flagVal, nil
}

// StripBBBoolFlag removes a CLI-only bool flag from the forwarded args (so it
// is not passed to Bazelisk) and returns whether it was set.
func (a *BazelArgs) StripBBBoolFlag(flagName string) (bool, error) {
	// TODO(#7216): Read the Forwarded args field - we can't strip a field set in a rc file.
	parsed, err := parser.ParseArgs(a.Resolved)
	if err != nil {
		return false, err
	}
	set, err := parser.IsCLICommandOptionSet(parsed, flagName)
	if err != nil {
		return false, err
	}
	// Update the args to not have the removed flag.
	// Use direct assignment rather than Set() to avoid re-resolving rc/config
	// flags, which would fail if a plugin added a  --config flag.
	// TODO(#7216): This will be cleaner when we can safely re-resolve the Forwarded args.
	a.Resolved = parsed.Format()
	return set, nil
}

// GetRemoteHeaderVal returns the value of a --remote_header flag matching the
// given key, reading from the resolved args.
func (a *BazelArgs) GetRemoteHeaderVal(key string) string {
	parsed, err := parser.ParseArgs(a.Resolved)
	if err != nil {
		return ""
	}
	return parser.GetRemoteHeaderVal(parsed, key)
}

// TODO(#7216): Add a test where the resolved flags add another version of flagName at the end of the args.
// We shouldn't pop the newly resolved arg.
//
// Pop removes a flag and returns its value.
// NOTE: Pop does not remove boolean flags.
func (a *BazelArgs) Pop(flagName string) (string, error) {
	value, newArgs := Pop(a.Resolved, flagName)

	if value == "" {
		return "", nil
	}

	resolve, err := requiresResolve(flagName)
	if err != nil {
		return "", err
	}
	if resolve {
		if err := a.Set(newArgs); err != nil {
			return "", err
		}
	} else {
		a.Resolved = newArgs
	}
	// TODO(#7216): Return an error if the flag is only present in the resolved args (i.e. set
	// via a config file), since it cannot be removed from the forwarded args in that case.
	return value, nil
}

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
//
// NOTE: Pop does not remove boolean flags.
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
		if after, ok := strings.CutPrefix(arg, prefix); ok {
			return after, i, 1
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

// Returns a list of bazel targets specified in the given set of arguments, if any.
// This function assumes that the arguments are canonicalized.
func GetTargets(args []string) []string {
	command := GetCommand(args)
	nonOptionArgs := []string{}
	for _, arg := range args {
		if arg == "--" && command == "run" {
			break
		}
		if !strings.HasPrefix(arg, "-") {
			nonOptionArgs = append(nonOptionArgs, arg)
		}
	}
	if len(nonOptionArgs) == 0 {
		return nonOptionArgs
	}
	return nonOptionArgs[1:]
}

// Returns any arguments to be passed to the executable generated by bazel in
// args (those to the right of the first " -- ", if any)
// Ex. bb build //... <bazel_args> -- <executable_args>
func GetExecutableArgs(args []string) []string {
	for i, arg := range args {
		if arg == "--" {
			return append([]string{}, args[i+1:]...)
		}
	}
	return nil
}

// Returns any arugments to be passed to bazel in args (those to the left of the
// first " -- ", if any)
// Ex. bb remote build //... <bazel_args>
func GetBazelArgs(args []string) []string {
	splitIndex := len(args)
	for i, arg := range args {
		if arg == "--" {
			splitIndex = i
			break
		}
	}
	return append([]string{}, args[:splitIndex]...)
}

// Splits bazel args and executable args into two separate lists. The first
// "--" separator is dropped if it exists.
func SplitExecutableArgs(args []string) (bazel []string, exec []string) {
	return GetBazelArgs(args), GetExecutableArgs(args)
}

// JoinExecutableArgs joins the given args and executable args with a "--"
// separator, if the executable args are non-empty. Otherwise it returns
// the original bazel args.
func JoinExecutableArgs(args, execArgs []string) []string {
	out := append([]string{}, args...)
	if len(execArgs) == 0 {
		return out
	}
	if !slices.Contains(args, "--") {
		out = append(out, "--")
	}
	out = append(out, execArgs...)
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

// SplitOptionValue returns the arg parts before and after the first '=' if
// present.
func SplitOptionValue(arg string) (flag string, value string) {
	parts := strings.SplitN(arg, "=", 2)
	switch len(parts) {
	case 2:
		return parts[0], parts[1]
	case 1:
		return parts[0], ""
	default:
		return "", ""
	}
}

func Append(args []string, arg ...string) []string {
	bazelArgs, execArgs := SplitExecutableArgs(args)
	for _, a := range arg {
		if strings.HasPrefix(a, "-") {
			bazelArgs = append(bazelArgs, a)
			continue
		}
		execArgs = append(execArgs, a)
	}
	return JoinExecutableArgs(bazelArgs, execArgs)
}
