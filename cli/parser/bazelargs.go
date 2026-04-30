package parser

import (
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
)

type BazelArgs struct {
	// Forwarded are the Bazel args that are eventually passed to Bazelisk.
	// bb might add to these, but --config and --bazelrc flags are not expanded.
	// These should look fairly similarly to the user-supplied args, but with some additional bb-specific args.
	Forwarded []string

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
	// Normalize args - apply stable sorting and consistent option representation.
	normalizedArgs, err := CanonicalizeArgs(args)
	if err != nil {
		return err
	}
	a.Forwarded = normalizedArgs
	return a.resolve()
}

// Append adds a new bazel arg.
func (a *BazelArgs) Append(arg string) error {
	// If the arg is a --config or --bazelrc flag, we need to recompute the resolved args.
	if strings.HasPrefix(arg, "--config") || strings.HasPrefix(arg, "--bazelrc") {
		return a.Set(append(a.Forwarded, arg))
	}
	a.Forwarded = append(a.Forwarded, arg)
	a.Resolved = append(a.Resolved, arg)
	return nil
}

// Prepend adds a new bazel arg to the beginning of the list of args, just after the bazel command.
// If the same flag is specified multiple times, Bazel will use the last value. This is useful for adding flags that should
// be overriden by later flags.
func (a *BazelArgs) Prepend(arg string) error {
	updatedForwardedArgs := prepend(a.Forwarded, arg)
	if strings.HasPrefix(arg, "--config") || strings.HasPrefix(arg, "--bazelrc") {
		return a.Set(updatedForwardedArgs)
	}

	updatedResolvedArgs := prepend(a.Resolved, arg)

	a.Forwarded = updatedForwardedArgs
	a.Resolved = updatedResolvedArgs
	return nil
}

func prepend(args []string, arg string) []string {
	_, commandIndex := GetBazelCommandAndIndex(args)
	if commandIndex == -1 {
		return append([]string{arg}, args...)
	}
	args = args[:commandIndex+1]
	args = append(args, arg)
	args = append(args, args[commandIndex+1:]...)
	return args
}

// Get returns the value of a flag.
// It reads from the resolved args to ensure that flags expanded from --config or --bazelrc are included.
func (a *BazelArgs) Get(flagName string) string {
	return arg.Get(a.Resolved, flagName)
}

func (a *BazelArgs) Has(flagName string) bool {
	return a.Get(flagName) != ""
}

// resolve re-evaluates the forwarded args and expands all flags.
func (a *BazelArgs) resolve() error {
	// Expand --config and --bazelrc flags, then normalize the result.
	resolved, err := ResolveAndCanonicalizeArgs(a.Forwarded)
	if err != nil {
		return err
	}
	a.Resolved = resolved
	return nil
}

func (a *BazelArgs) GetTargets() []string {
	return arg.GetTargets(a.Resolved)
}

func (a *BazelArgs) GetCommand() string {
	return arg.GetCommand(a.Resolved)
}

func (a *BazelArgs) GetAllFlagsWithName(flagName string) []string {
	return arg.GetMulti(a.Resolved, flagName)
}

// StripBBFlag removes a CLI-only string flag from the forwarded args (so it is
// not passed to Bazelisk) and returns its effective value from the resolved args.
func (a *BazelArgs) StripBBFlag(flagName string) (string, error) {
	parsed, err := ParseArgs(a.Forwarded)
	if err != nil {
		return "", err
	}
	if _, err := GetCLICommandOptionVal(parsed, flagName); err != nil {
		return "", err
	}
	if err := a.Set(parsed.Format()); err != nil {
		return "", err
	}
	return a.Get(flagName), nil
}

// StripBBBoolFlag removes a CLI-only bool flag from the forwarded args (so it
// is not passed to Bazelisk) and returns whether it was set, reading from the
// resolved args. Use this instead of StripBBFlag for boolean flags, since a
// bool flag set without an explicit value (e.g. --stream_run_logs) would return
// an empty string from StripBBFlag even though it is set.
func (a *BazelArgs) StripBBBoolFlag(flagName string) (bool, error) {
	parsed, err := ParseArgs(a.Forwarded)
	if err != nil {
		return false, err
	}
	set, err := IsCLICommandOptionSet(parsed, flagName)
	if err != nil {
		return false, err
	}
	if err := a.Set(parsed.Format()); err != nil {
		return false, err
	}
	return set, nil
}

// GetRemoteHeaderVal returns the value of a --remote_header flag matching the
// given key, reading from the resolved args.
func (a *BazelArgs) GetRemoteHeaderVal(key string) string {
	parsed, err := ParseArgs(a.Resolved)
	if err != nil {
		return ""
	}
	return GetRemoteHeaderVal(parsed, key)
}

// TODO: Add a test - where the resolved flags add another version of flagName. We shouldn't pop the newly resolved version.
// Pop removes a flag and returns its value.
// Returns an error if the flag is only present in the resolved args (i.e. set
// via a config file), since it cannot be removed from the forwarded args in that case.
func (a *BazelArgs) Pop(flagName string) (string, error) {
	value, newForwarded := arg.Pop(a.Forwarded, flagName)
	if value != "" {
		a.Set(newForwarded)
		return value, nil
	}
	if arg.Get(a.Resolved, flagName) != "" {
		return "", fmt.Errorf("--%s is set via a config file and cannot be removed", flagName)
	}
	return "", nil
}
