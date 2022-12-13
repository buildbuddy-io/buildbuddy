package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/analyze"
	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/help"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/metadata"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/printlog"
	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/cli/tooltag"
	"github.com/buildbuddy-io/buildbuddy/cli/update"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
	"github.com/buildbuddy-io/buildbuddy/cli/watcher"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	sidecarmain "github.com/buildbuddy-io/buildbuddy/cli/cmd/sidecar"
)

func main() {
	// If we're the sidecar (CLI server) process, run the sidecar instead of the
	// CLI.
	sidecarmain.Handle()

	exitCode, err := run()
	if err != nil {
		log.Fatal(status.Message(err))
	}
	os.Exit(exitCode)
}

func run() (exitCode int, err error) {
	start := time.Now()
	// Record original arguments so we can show them in the UI.
	originalArgs := append([]string{}, os.Args...)

	// Handle global args that don't apply to any specific subcommand
	// (--verbose, etc.)
	args := log.Configure(os.Args[1:])

	// Show help if applicable.
	exitCode, err = help.HandleHelp(args)
	if err != nil || exitCode >= 0 {
		return exitCode, err
	}

	// Handle CLI-specific subcommands.
	exitCode, err = plugin.HandleInstall(args)
	if err != nil || exitCode >= 0 {
		return exitCode, err
	}
	exitCode, err = printlog.HandlePrint(args)
	if err != nil || exitCode >= 0 {
		return exitCode, err
	}
	exitCode, err = update.HandleUpdate(args)
	if err != nil || exitCode >= 0 {
		return exitCode, err
	}
	exitCode, err = version.HandleVersion(args)
	if err != nil || exitCode >= 0 {
		return exitCode, err
	}
	exitCode, err = analyze.HandleAnalyze(args)
	if err != nil || exitCode >= 0 {
		return exitCode, err
	}
	// TODO: Convert this to (exitCode, err) convention
	args = login.HandleLogin(args)

	// If none of the CLI subcommand handlers were triggered, assume we have a
	// bazel invocation.

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
	// TODO: Expanding configs results in a long explicit command line in the BB
	// UI. Need to find a way to override the explicit command line in the UI so
	// that it reflects the args passed to the CLI, not the wrapped Bazel
	// process.
	args, err = parser.ExpandConfigs(bazelArgs)
	if err != nil {
		return -1, err
	}

	// Fiddle with Bazel args
	// TODO(bduffany): model these as "built-in" plugins
	args = tooltag.ConfigureToolTag(args)
	args = login.ConfigureAPIKey(args)

	// Prepare convenience env vars for plugins
	if err := plugin.PrepareEnv(); err != nil {
		return -1, err
	}

	// Run plugin pre-bazel hooks
	args, err = parser.CanonicalizeArgs(args)
	if err != nil {
		return -1, err
	}
	for _, p := range plugins {
		args, err = p.PreBazel(args)
		if err != nil {
			return -1, err
		}
	}

	// Note: sidecar is configured after pre-bazel plugins, since pre-bazel
	// plugins may change the value of bes_backend, remote_cache,
	// remote_instance_name, etc.
	args = sidecar.ConfigureSidecar(args)

	// Handle remote bazel. Note, pre-bazel hooks apply to remote bazel, but not
	// output handlers or post-bazel hooks.
	args = remotebazel.HandleRemoteBazel(args, passthroughArgs)

	// If this is a `bazel run` command, add a --run_script arg so that
	// we can execute post-bazel plugins between the build and the run step.
	args, scriptPath, err = bazelisk.ConfigureRunScript(args)
	if err != nil {
		return -1, err
	}
	// Append metadata just before running bazelisk.
	// Note, this means plugins cannot modify this metadata.
	args, err = metadata.AppendBuildMetadata(args, originalArgs)
	if err != nil {
		return -1, err
	}

	// Run bazelisk, capturing the original output in a file and allowing
	// plugins to control how the output is rendered to the terminal.
	log.Debugf("bb initialized in %s", time.Since(start))
	outputPath := filepath.Join(tempDir, "bazel.log")
	exitCode, err = plugin.RunBazeliskWithPlugins(
		arg.JoinPassthroughArgs(args, passthroughArgs),
		outputPath, plugins)
	if err != nil {
		return -1, err
	}

	// If the build was interrupted (Ctrl+C), don't run post-bazel plugins.
	if exitCode == 8 /*interrupted*/ {
		return exitCode, nil
	}

	// Run plugin post-bazel hooks.
	// Pause the file watcher while these are in progress, so that plugins can
	// apply fixes to files in the workspace without the watcher immediately
	// restarting.
	watcher.Pause()
	defer watcher.Unpause()
	for _, p := range plugins {
		if err := p.PostBazel(outputPath); err != nil {
			return -1, err
		}
	}

	// TODO: Support post-run hooks?
	// e.g. show a desktop notification once a k8s deploy has finished

	return exitCode, nil
}
