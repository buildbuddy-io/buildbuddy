package version

import (
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
)

func HandleVersion(args []string) []string {
	if arg.GetCommand(args) != "version" {
		return args
	}
	// The "version" var is generated in this package according to the
	// Bazel flag value --//cli/version:cli_version
	fmt.Printf("bb %s\n", cliVersionFlag)
	return args
}
