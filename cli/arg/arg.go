package arg

import (
	"flag"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
)

type BazelArgs struct {
	// TODO(#7216): Actually pass the forwarded args field to Bazelisk.
	//
	// forwarded are the Bazel args that are eventually passed to Bazelisk.
	// bb might add to these, but --config and --bazelrc flags are not expanded.
	// These should look fairly similarly to the user-supplied args, but with some additional bb-specific args.
	forwarded *parsed.OrderedArgs

	// resolved are the Bazel args that are used internally within the bb parser.
	// --config and --bazelrc flags are expanded, so the parser has a complete view of the args.
	resolved *parsed.OrderedArgs
}

// Forwarded returns the forwarded args as a canonicalized []string.
func (a *BazelArgs) Forwarded() []string {
	return a.forwarded.Canonicalized().Format()
}

// Resolved returns the resolved args as a canonicalized []string.
func (a *BazelArgs) Resolved() []string {
	return a.resolved.Canonicalized().Format()
}

// NewBazelArgs returns a BazelArgs struct from a slice of bazel args.
func NewBazelArgs(args []string) (*BazelArgs, error) {
	b := &BazelArgs{}
	if err := b.Set(args); err != nil {
		return nil, err
	}
	return b, nil
}

// NewBazelArgsNoResolve creates a BazelArgs from an already-resolved []string
// without performing config/bazelrc expansion. Both forwarded and resolved are
// set to the same parsed form of args.
func NewBazelArgsNoResolve(args []string) (*BazelArgs, error) {
	parsed, err := parser.ParseArgs(args)
	if err != nil {
		return nil, err
	}
	return &BazelArgs{forwarded: parsed, resolved: parsed}, nil
}

// Set updates the BazelArgs struct with a new slice of bazel args.
// It also recomputes the resolved args.
func (a *BazelArgs) Set(args []string) error {
	forwarded, err := parser.ParseArgs(args)
	if err != nil {
		return err
	}
	a.forwarded = forwarded
	return a.resolve()
}

// Append adds a new bazel arg.
func (a *BazelArgs) Append(arg string) error {
	newFwd, err := parser.ParseArgs(Append(a.forwarded.Format(), arg))
	if err != nil {
		return err
	}
	a.forwarded = newFwd

	if requiresResolve(arg) {
		return a.resolve()
	}

	newRes, err := parser.ParseArgs(Append(a.resolved.Format(), arg))
	if err != nil {
		return err
	}
	a.resolved = newRes
	return nil
}

// Prepend adds a new bazel arg to the beginning of the list of args, just after the bazel command.
// If the same flag is specified multiple times, Bazel will use the last value. This is useful for adding flags that should
// be overridden by later flags.
func (a *BazelArgs) Prepend(arg string) error {
	newFwd, err := parser.ParseArgs(prepend(a.forwarded.Format(), arg))
	if err != nil {
		return err
	}
	a.forwarded = newFwd

	if requiresResolve(arg) {
		return a.resolve()
	}

	newRes, err := parser.ParseArgs(prepend(a.resolved.Format(), arg))
	if err != nil {
		return err
	}
	a.resolved = newRes
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
func requiresResolve(arg string) bool {
	flagName, _ := SplitOptionValue(arg)
	flagName = strings.TrimPrefix(flagName, "--")
	return flagName == "config" || flagName == "bazelrc"
}

// Get returns the value of a flag.
// It reads from the resolved args to ensure that flags expanded from --config or --bazelrc are included.
func (a *BazelArgs) Get(flagName string) string {
	return Get(a.Resolved(), flagName)
}

func (a *BazelArgs) Has(flagName string) bool {
	return a.Get(flagName) != ""
}

// resolve re-evaluates the forwarded args and expands all flags into the resolved field.
//
// resolve is expensive because it re-parses all rc files and expands configs. It is only necessary
// when requiresResolve returns true for a flag.
func (a *BazelArgs) resolve() error {
	// Clone to avoid mutation of a.forwarded by ResolveArgs.
	clone := &parsed.OrderedArgs{Args: slices.Clone(a.forwarded.Args)}
	resolved, err := parser.ResolveArgs(clone)
	if err != nil {
		return err
	}
	a.resolved = resolved
	return nil
}

func (a *BazelArgs) GetTargets() []string {
	return GetTargets(a.Resolved())
}

func (a *BazelArgs) GetCommand() string {
	return GetCommand(a.Resolved())
}

func (a *BazelArgs) GetAllFlagsWithName(flagName string) []string {
	return GetMulti(a.Resolved(), flagName)
}

// StripBBFlag removes a CLI-only string flag from the args (so it is
// not passed to Bazelisk) and returns its value.
func (a *BazelArgs) StripBBFlag(flagName string) (string, error) {
	// GetCLICommandOptionVal removes the flag from a.forwarded in place.
	flagVal, err := parser.GetCLICommandOptionVal(a.forwarded, flagName)
	if err != nil {
		return "", err
	}
	a.resolved.RemoveCommandOptions(flagName)
	return flagVal, nil
}

// StripBBBoolFlag removes a CLI-only bool flag from the forwarded args (so it
// is not passed to Bazelisk) and returns whether it was set.
func (a *BazelArgs) StripBBBoolFlag(flagName string) (bool, error) {
	// IsCLICommandOptionSet removes the flag from a.forwarded in place.
	set, err := parser.IsCLICommandOptionSet(a.forwarded, flagName)
	if err != nil {
		return false, err
	}
	a.resolved.RemoveCommandOptions(flagName)
	return set, nil
}

// GetRemoteHeaderVal returns the value of a --remote_header flag matching the
// given key, reading from the resolved args.
func (a *BazelArgs) GetRemoteHeaderVal(key string) string {
	return parser.GetRemoteHeaderVal(a.resolved, key)
}

// Pop removes a flag and returns its value.
// NOTE: Pop does not remove boolean flags.
func (a *BazelArgs) Pop(flagName string) (string, error) {
	value, newFwdSlice := Pop(a.forwarded.Format(), flagName)

	if value != "" {
		newFwd, err := parser.ParseArgs(newFwdSlice)
		if err != nil {
			return "", err
		}
		a.forwarded = newFwd

		if requiresResolve(flagName) {
			return value, a.resolve()
		}

		// Remove the exact occurrence from resolved by matching the flag name and value,
		// since the same flag name may appear from multiple sources (command line and config files).
		for _, opt := range a.resolved.GetCommandOptionsByName(flagName) {
			if opt.GetValue() == value {
				a.resolved.Args = slices.Delete(a.resolved.Args, opt.Index, opt.Index+1)
				break
			}
		}
		return value, nil
	}

	if a.Has(flagName) {
		return "", fmt.Errorf("--%s is set via a config file and cannot be removed", flagName)
	}

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
	return slices.Contains(args, value)
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
