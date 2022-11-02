package main

import (
	"os"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/help"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/cli/tooltag"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
	"github.com/buildbuddy-io/buildbuddy/cli/watcher"
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
	// Handle global args that don't apply to any specific subcommand
	// (--verbose, etc.)
	args := log.Configure(os.Args[1:])

	// Show help if applicable.
	exitCode, err = help.HandleHelp(args)
	if err != nil || exitCode >= 0 {
		return exitCode, err
	}

	// Handle CLI-specific subcommands.
	// TODO: Refactor so that logging is configured earlier.
	exitCode, err = plugin.HandleInstall(args)
	if err != nil || exitCode >= 0 {
		return exitCode, err
	}

	// Maybe run interactively (watching for changes to files).
	if exitCode, err := watcher.Watch(); exitCode >= 0 || err != nil {
		return exitCode, err
	}

	var scriptPath string
	// Prepare a dir for temporary files created by this CLI run
	tempDir, err := os.MkdirTemp("", "buildbuddy-cli-*")
	if err != nil {
		return -1, err
	}
	defer func() {
		// Remove tempdir. Need to do this before invoking the run script
		// (if applicable), since the run script will replace this process.
		os.RemoveAll(tempDir)

		// Invoke the run script only if the build succeeded.
		if exitCode == 0 && scriptPath != "" {
			exitCode, err = bazelisk.InvokeRunScript(scriptPath)
		}
	}()

	// Load plugins
	plugins, err := plugin.LoadAll(tempDir)
	if err != nil {
		return -1, err
	}

	// Parse args.
	// Split out passthrough args and don't let plugins modify them,
	// since those are intended to be passed to the built binary as-is.
	bazelArgs, passthroughArgs := arg.SplitPassthroughArgs(args)
	rcFileArgs := parser.GetArgsFromRCFiles(bazelArgs)
	args = append(bazelArgs, rcFileArgs...)

	// Fiddle with args
	// TODO(bduffany): model these as "built-in" plugins
	args = tooltag.ConfigureToolTag(args)
	args = sidecar.ConfigureSidecar(args)
	args = login.ConfigureAPIKey(args)

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
	args = remotebazel.HandleRemoteBazel(args, passthroughArgs)
	args = version.HandleVersion(args)
	args = login.HandleLogin(args)

	// Remove any args that don't need to be on the command line
	args = arg.RemoveExistingArgs(args, rcFileArgs)

	// If this is a `bazel run` command, add a --run_script arg so that
	// we can execute post-bazel plugins between the build and the run step.
	args, scriptPath, err = bazelisk.ConfigureRunScript(args)
	if err != nil {
		return -1, err
	}

	// Run bazelisk, capturing the original output in a file and allowing
	// plugins to control how the output is rendered to the terminal.
	outputPath := filepath.Join(tempDir, "bazel.log")
	exitCode, err = bazelisk.RunWithPlugins(
		arg.JoinPassthroughArgs(args, passthroughArgs),
		outputPath, plugins)
	if err != nil {
		return -1, err
	}

	// If the build was interrupted (Ctrl+C), don't run post-bazel plugins.
	if exitCode == 8 /*interrupted*/ {
		return exitCode, nil
	}

	// Run plugin post-bazel hooks
	for _, p := range plugins {
		if err := p.PostBazel(outputPath); err != nil {
			return -1, err
		}
	}

	// TODO: Support post-run hooks?
	// e.g. show a desktop notification once a k8s deploy has finished

	return exitCode, nil
}
