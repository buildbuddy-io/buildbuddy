package tooltag

import (
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

func ConfigureToolTag(args *arg.BazelArgs) error {
	log.Debugf("BuildBuddy CLI version %s invoked as %s", version.String(), os.Args[0])

	v := "development"
	if version.String() != "" {
		v = version.String()
	}
	if args.GetCommand() != "" && !args.Has("tool_tag") {
		if err := args.Append("--tool_tag=buildbuddy-cli-" + v); err != nil {
			return err
		}
	}
	return nil
}
