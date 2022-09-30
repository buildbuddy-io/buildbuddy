package main

import (
	"flag"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/cli/tooltag"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

func main() {
	flag.Parse()

	// Parse args
	commandLineArgs := flag.Args()
	rcFileArgs := parser.GetArgsFromRCFiles(commandLineArgs)
	args := append(commandLineArgs, rcFileArgs...)

	// Fiddle with args
	args = tooltag.ConfigureToolTag(args)
	args = sidecar.ConfigureSidecar(args)
	args = login.ConfigureAPIKey(args)

	// Handle commands
	args = remotebazel.HandleRemoteBazel(args)
	args = version.HandleVersion(args)
	args = login.HandleLogin(args)

	// Remove any args that don't need to be on the command line
	args = arg.RemoveExistingArgs(args, rcFileArgs)

	// Actually run bazel
	bazelisk.Run(args)
}
