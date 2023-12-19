package version

import (
	_ "embed"
	"fmt"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/common"
)

//go:embed version_flag.txt
var cliVersionFlag string

// HandleVersion The bb cli "version" var is generated in this package according to the
// Bazel flag value --//cli/version:cli_version
func HandleVersion(args []string) (exitCode int, err error) {
	if arg.ContainsExact(args, "--cli") {
		fmt.Println(cliVersionFlag)
		return 0, nil
	}

	fmt.Printf("bb %s\n", cliVersionFlag)
	// Forward the `version` command to bazel (i.e. `bazel version`)
	return common.ForwardCommandToBazelExitCode, nil
}

func String() string {
	return cliVersionFlag
}
