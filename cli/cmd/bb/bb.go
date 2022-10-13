package main

import (
	"os"
	"path/filepath"

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
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

func main() {
	exitCode, err := run()
	if err != nil {
		log.Fatal(status.Message(err))
	}
	os.Exit(exitCode)
}

func run() (exitCode int, err error) {
	// Prepare a dir for temporary files created by this CLI run
	tempDir, err := os.MkdirTemp("", "buildbuddy-cli-*")
	if err != nil {
		return -1, err
	}
	defer os.RemoveAll(tempDir)

	// Load plugins
	plugins, err := plugin.LoadAll(tempDir)
	if err != nil {
		return -1, err
	}

	// Parse args
	commandLineArgs := os.Args[1:]
	rcFileArgs := parser.GetArgsFromRCFiles(commandLineArgs)
	args := append(commandLineArgs, rcFileArgs...)

	// Fiddle with args
	// TODO(bduffany): model these as "built-in" plugins
	args = log.Configure(args)
	args = tooltag.ConfigureToolTag(args)
	args = sidecar.ConfigureSidecar(args)
	args = login.ConfigureAPIKey(args)
	args = terminal.ConfigureOutputMode(args)

	// Prepare convenience env vars for plugins
	if err := plugin.PrepareEnv(); err != nil {
		return -1, err
	}

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

	// Run bazelisk, capturing the original output in a file and allowing
	// plugins to control how the output is rendered to the terminal.
	outputPath := filepath.Join(tempDir, "bazel.log")
	exitCode, err = bazelisk.RunWithPlugins(args, outputPath, plugins)
	if err != nil {
		return -1, err
	}

	// Run plugin post-bazel hooks
	for _, p := range plugins {
		if err := p.PostBazel(outputPath); err != nil {
			return -1, err
		}
	}

	return exitCode, nil
}
