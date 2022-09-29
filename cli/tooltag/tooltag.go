package tooltag

import (
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
)

func ConfigureToolTag(args []string) []string {
	if !arg.Has(args, "tool_tag") {
		return append(args, "--tool_tag=buildbuddy-cli")
	}
	return args
}
