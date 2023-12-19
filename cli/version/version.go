package version

import (
	_ "embed"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazel_command"
	"github.com/buildbuddy-io/buildbuddy/cli/common"
)

// HandleVersion The bb cli "version" var is generated in this package according to the
// Bazel flag value --//common/version:cli_version
func HandleVersion(args []string) (exitCode int, err error) {
	if arg.ContainsExact(args, "--cli") {
		fmt.Println(common.Version())
		return 0, nil
	}

	fmt.Printf("bb %s\n", common.Version())

	// Add the `version` command back to args before forwarding
	// the command to bazel
	args = append([]string{"version"}, args...)
	// Forward the `version` command to bazel (i.e. `bazel version`)
	return bazel_command.Run(args, []string{})
}
