package tooltag

import (
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

func ConfigureToolTag(pair *arg.ArgPair) {
	log.Debugf("BuildBuddy CLI version %s invoked as %s", version.String(), os.Args[0])

	v := "development"
	if version.String() != "" {
		v = version.String()
	}
	if arg.GetCommand(pair.Raw) != "" && !arg.Has(pair.Effective, "tool_tag") {
		pair.Append("--tool_tag=buildbuddy-cli-" + v)
	}
}
