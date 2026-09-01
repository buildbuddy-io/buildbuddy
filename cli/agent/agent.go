package agent

import (
	"errors"
	"flag"
	"slices"

	"github.com/buildbuddy-io/buildbuddy/cli/agent/agentflags"
	"github.com/buildbuddy-io/buildbuddy/cli/agent/analyze_profile"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
)

const usage = `
usage: bb agent <subcommand> [args]

Runs an AI coding agent to analyze data.

Subcommands:
	analyze-profile   Analyzes the timing profile for an invocation.
`

// subcommand is a `bb agent` subcommand.
type subcommand struct {
	name  string
	usage string
	flags *flag.FlagSet
	// HandleAgent parses flags before calling handler, so handler receives only the positional args.
	handler func(args []string) (int, error)
}

var (
	subcommands = []*subcommand{
		{
			name:    "analyze-profile",
			usage:   analyze_profile.Usage,
			flags:   analyze_profile.Flags,
			handler: analyze_profile.HandleAnalyzeProfile,
		},
	}
)

// Mirror the common agent flags onto every subcommand's flag set, so that each
// subcommand only declares the flags that are unique to it.
func init() {
	for _, s := range subcommands {
		agentflags.RegisterSharedFlags(s.flags)
	}
}

func HandleAgent(args []string) (int, error) {
	s, rest := findSubcommand(args)
	if s == nil {
		log.Warn("Unknown subcommand")
		log.Print(usage)
		return 1, nil
	}
	if err := arg.ParseFlagSet(s.flags, rest); err != nil {
		if !errors.Is(err, flag.ErrHelp) {
			log.Printf("Failed to parse flags: %s", err)
		}
		log.Print(s.usage)
		return 1, nil
	}

	// The flags are parsed above, so s.handler receives only the positional args.
	return s.handler(s.flags.Args())
}

// findSubcommand returns the first arg naming a subcommand, along with the
// args with that name removed.
func findSubcommand(args []string) (*subcommand, []string) {
	for i, a := range args {
		for _, s := range subcommands {
			if s.name == a {
				return s, slices.Concat(args[:i], args[i+1:])
			}
		}
	}
	return nil, nil
}
