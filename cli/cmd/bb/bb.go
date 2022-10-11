package main

import (
	"flag"
	"os"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/cli/terminal"
	"github.com/buildbuddy-io/buildbuddy/cli/tooltag"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

func main() {
	flag.Parse()
	exitCode, err := run()
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(exitCode)
}

func run() (exitCode int, err error) {
	// Load plugins
	plugins, err := plugin.LoadAll()
	if err != nil {
		return -1, err
	}

	// Parse args
	commandLineArgs := flag.Args()
	rcFileArgs := parser.GetArgsFromRCFiles(commandLineArgs)
	args := append(commandLineArgs, rcFileArgs...)

	// Fiddle with args
	// TODO(bduffany): model these as "built-in" plugins
	args = tooltag.ConfigureToolTag(args)
	args = sidecar.ConfigureSidecar(args)
	args = login.ConfigureAPIKey(args)
	args = terminal.ConfigureOutputMode(args)

	// Run plugin pre-bazel hooks
	for _, p := range plugins {
		args, err = p.PreBazel(args)
		if err != nil {
			return -1, err
		}
	}

	// Handle commands
	args = remotebazel.HandleRemoteBazel(args)
	args = version.HandleVersion(args)
	args = login.HandleLogin(args)

	// Remove any args that don't need to be on the command line
	args = arg.RemoveExistingArgs(args, rcFileArgs)

	// Build the pipeline of bazel output handlers
	wc, err := plugin.PipelineWriter(os.Stdout, plugins)
	if err != nil {
		return -1, err
	}
	defer wc.Close()

	// Actually run bazel
	exitCode, output, err := bazelisk.Run(args, wc)
	if err != nil {
		return -1, err
	}
	defer output.Close()

	// Run plugin post-bazel hooks
	for _, p := range plugins {
		if err := p.PostBazel(output.Name()); err != nil {
			return -1, err
		}
	}

	return exitCode, nil
}
