package tooltag

import (
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

func ConfigureToolTag(args []string) []string {
	log.Debugf("BuildBuddy CLI version %s invoked as %s", version.String(), os.Args[0])

	if arg.GetCommand(args) != "" && !arg.Has(args, "tool_tag") {
		return append(args, "--tool_tag=buildbuddy-cli")
	}
	return args
}
