package version

import (
	_ "embed"
	"fmt"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
)

//go:embed version_flag.txt
var cliVersionFlag string

func HandleVersion(args []string) (exitCode int, err error) {
	if arg.GetCommand(args) != "version" {
		return -1, nil
	}
	if arg.ContainsExact(args, "--cli") {
		fmt.Println(cliVersionFlag)
		return 0, nil
	}
	// The "version" var is generated in this package according to the
	// Bazel flag value --//cli/version:cli_version
	fmt.Printf("bb %s\n", cliVersionFlag)
	return -1, nil
}

func String() string {
	return cliVersionFlag
}
