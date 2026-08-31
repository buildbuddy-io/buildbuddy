// Package agentflags declares the flags that are shared by all `bb agent`
// subcommands.
package agentflags

import (
	"flag"

	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/agentutil"
)

var (
	SharedAgentFlags = flag.NewFlagSet("agent", flag.ContinueOnError)
	Agent            = SharedAgentFlags.String("agent", agentutil.Claude, "The agent to use.")
	Model            = SharedAgentFlags.String("model", "", "The agent model to use (Ex. gpt-5.4 or claude-opus-4-8). Defaults to the selected agent's default.")
	Effort           = SharedAgentFlags.String("effort", "", "The agent reasoning effort to use. Defaults to the selected agent's default.")
	APITarget        = SharedAgentFlags.String("target", login.DefaultApiTarget, "The API target to use.")
	HTTPTarget       = SharedAgentFlags.String("url", login.DefaultHTTPTarget, "The BuildBuddy web URL to use.")
)

// RegisterSharedFlags adds the shared agent flags onto the given flagset so that the shared
// flags can be parsed as part of the flagset for the subcommand.
func RegisterSharedFlags(fs *flag.FlagSet) {
	SharedAgentFlags.VisitAll(func(f *flag.Flag) {
		fs.Var(f.Value, f.Name, f.Usage)
	})
}
