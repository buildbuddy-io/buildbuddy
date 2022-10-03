package tooltag

import (
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
)

func ConfigureToolTag(args []string) []string {
	log.Printf("🚀 \033[1mBUILT WITH BUILDBUDDY\033[0m 🚀")
	if arg.GetCommand(args) != "" && !arg.Has(args, "tool_tag") {
		return append(args, "--tool_tag=buildbuddy-cli")
	}
	return args
}
