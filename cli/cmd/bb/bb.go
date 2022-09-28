package main

import (
	"flag"
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/cli/tooltag"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

func main() {
	flag.Parse()

	// Parse args
	commandLineArgs := os.Args[1:]
	rcFileArgs := parser.GetArgsFromRCFiles(commandLineArgs)
	args := append(commandLineArgs, rcFileArgs...)

	// Fiddle with args
	args = tooltag.ConfigureToolTag(args)
	args = sidecar.ConfigureSidecar(args)

	// Handle commands
	args = remotebazel.HandleRemoteBazel(args)
	args = version.HandleVersion(args)

	// Remove any args that don't need to be on the command line
	args = arg.RemoveExistingArgs(args, rcFileArgs)

	// Actually run bazel
	bazelisk.Run(args)
}
