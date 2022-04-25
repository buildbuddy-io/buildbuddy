package commandline

import (
	"flag"
	"fmt"
	"strings"

	gflags "github.com/jessevdk/go-flags"
)

type BazelFlags struct {
	Config string `long:"config" required:"false"`

	BESBackend     string `long:"bes_backend" required:"false"`
	RemoteCache    string `long:"remote_cache" required:"false"`
	RemoteExecutor string `long:"remote_executor" required:"false"`

	NoSystemRC    bool   `long:"nosystem_rc" required:"false"`
	NoWorkspaceRC bool   `long:"noworkspace_rc" required:"false"`
	NoHomeRC      bool   `long:"nohome_rc" required:"false"`
	BazelRC       string `long:"bazelrc" required:"false"`
}

func ExtractBazelFlags(args []string) *BazelFlags {
	bf := &BazelFlags{}
	parser := gflags.NewParser(bf, gflags.IgnoreUnknown|gflags.PassDoubleDash)
	_, _ = parser.ParseArgs(args) // ignore error
	return bf
}

type BazelArgs struct {
	// Arguments passed to the CLI binary after removing flags that are meant for the CLI itself.
	Filtered []string
	// Arguments appearing after a bare "--" argument.
	Passthrough []string
	// Arguments for Bazel added by the CLI.
	Added []string
}

func (a *BazelArgs) Add(arg ...string) {
	a.Added = append(a.Added, arg...)
}

// ExecArgs returns a combined list of all the arguments.
func (a *BazelArgs) ExecArgs() []string {
	var args []string
	args = append(args, a.Filtered...)
	args = append(args, a.Added...)
	if len(a.Passthrough) > 0 {
		args = append(args, "--")
		args = append(args, a.Passthrough...)
	}
	return args
}

func (a *BazelArgs) String() string {
	return fmt.Sprintf("%s", a.ExecArgs())
}

// ParseFlagsAndRewriteArgs returns the argument list after removing known BuildBuddy CLI flags.
// The filtered arguments are returned via two slices, one for the normal args and one for any "passthrough" args
// (i.e. those appearing after a bare "--" argument).
func ParseFlagsAndRewriteArgs(args []string) *BazelArgs {
	ourFlagNames := []string{}
	flag.CommandLine.VisitAll(func(f *flag.Flag) {
		ourFlagNames = append(ourFlagNames, "--"+f.Name)
	})
	ourArgs := make([]string, 0, len(ourFlagNames))
	newArgs := make([]string, 0, len(args))
	for _, arg := range args {
		wasOurs := false
		for _, flagName := range ourFlagNames {
			if strings.HasPrefix(arg, flagName+"=") || arg == flagName {
				ourArgs = append(ourArgs, arg)
				wasOurs = true
				break
			}
		}
		if !wasOurs {
			newArgs = append(newArgs, arg)
		}
	}
	_ = flag.CommandLine.Parse(ourArgs) // ignore error.
	for i, a := range newArgs {
		if a == "--" {
			return &BazelArgs{Filtered: newArgs[:i], Passthrough: newArgs[i+1:]}
		}
	}
	return &BazelArgs{Filtered: newArgs}
}

// Bazel has many subcommands, each with their own args. To know which options
// from the bazelrc are active, we look at which subcommand was specified.
func GetSubCommand(args []string) string {
	for _, arg := range args {
		switch arg {
		case "analyze-profile":
			fallthrough
		case "aquery":
			fallthrough
		case "build":
			fallthrough
		case "canonicalize-flags":
			fallthrough
		case "clean":
			fallthrough
		case "coverage":
			fallthrough
		case "cquery":
			fallthrough
		case "dump":
			fallthrough
		case "fetch":
			fallthrough
		case "help":
			fallthrough
		case "info":
			fallthrough
		case "license":
			fallthrough
		case "mobile-install":
			fallthrough
		case "print_action":
			fallthrough
		case "query":
			fallthrough
		case "run":
			fallthrough
		case "shutdown":
			fallthrough
		case "sync":
			fallthrough
		case "test":
			fallthrough
		case "version":
			return arg
		}
	}
	return ""
}
