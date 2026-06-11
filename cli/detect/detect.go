package detect

import (
	"flag"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
)

const detectUsage = `
usage: bb detect <subcommand>

Detects issues in the current workspace.

Subcommands:
  nondeterminism   Runs two uncached Bazel builds and compares their compact execution logs.
`

var Flags = flag.NewFlagSet("detect", flag.ContinueOnError)

func HandleDetect(args []string) (int, error) {
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		log.Print(detectUsage)
		return 1, nil
	}
	switch args[0] {
	case "nondeterminism":
		return handleNondeterminism(args[1:])
	default:
		log.Printf("Unknown detect subcommand %q", args[0])
		log.Print(detectUsage)
		return 1, nil
	}
}
