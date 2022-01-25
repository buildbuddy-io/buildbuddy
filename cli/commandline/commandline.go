package commandline

import (
	"flag"
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
	parser := gflags.NewParser(bf, gflags.IgnoreUnknown)
	parser.ParseArgs(args) // ignore error
	return bf
}

// Parsing bazel flags is hard, so instead we look for our specific flags, in
// "--flagName=value" form, and if present, we pull them out of the os.Args
// slice, so that bazel never gets them.
func ParseFlagsAndRewriteArgs(args []string) []string {
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
	flag.CommandLine.Parse(ourArgs) // ignore error.
	return newArgs
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
