package tooltag

import (
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/common"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
)

func ConfigureToolTag(args []string) []string {
	log.Debugf("BuildBuddy CLI version %s invoked as %s", common.Version(), os.Args[0])

	if arg.GetCommand(args) != "" && !arg.Has(args, "tool_tag") {
		return append(args, "--tool_tag=buildbuddy-cli")
	}
	return args
}
