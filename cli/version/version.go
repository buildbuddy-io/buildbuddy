package version

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/server/version"
)

func HandleVersion(args []string) []string {
	if arg.GetCommand(args) != "version" {
		return args
	}
	fmt.Printf("bb %s\n", version.AppVersion())
	return args
}
