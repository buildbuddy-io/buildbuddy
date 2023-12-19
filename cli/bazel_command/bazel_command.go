package bazel_command

// The bazel_command package contains helper functions for running native bazel
// commands (i.e. commands that are directly forwarded to bazel, as opposed to
// bb cli-specific commands)

import (
	"os"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/ask"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/metadata"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/picker"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/cli/tooltag"
	"github.com/buildbuddy-io/buildbuddy/cli/watcher"
)

// Setup prepares us to run a bazel command. It loads plugins, handles auth,
// configures the sidecar, and runs pre-bazel handlers.
func Setup(args []string, tempDir string) (plugins []*plugin.Plugin, bazelArgs []string, execArgs []string, err error) {
	// Load plugins
	plugins, err = plugin.LoadAll(tempDir)
	if err != nil {
		return nil, nil, nil, err
	}

	// Parse args.
	bazelArgs, execArgs = arg.SplitExecutableArgs(args)
	// TODO: Expanding configs results in a long explicit command line in the BB
	// UI. Need to find a way to override the explicit command line in the UI so
	// that it reflects the args passed to the CLI, not the wrapped Bazel
	// process.
	bazelArgs, err = parser.ExpandConfigs(bazelArgs)
	if err != nil {
		return nil, nil, nil, err
	}

	// Fiddle with Bazel args
	// TODO(bduffany): model these as "built-in" plugins
	bazelArgs = tooltag.ConfigureToolTag(bazelArgs)
	bazelArgs, err = login.ConfigureAPIKey(bazelArgs)
	if err != nil {
		return nil, nil, nil, err
	}

	// Prepare convenience env vars for plugins
	if err := plugin.PrepareEnv(); err != nil {
		return nil, nil, nil, err
	}

	// Run plugin pre-bazel hooks
	bazelArgs, err = parser.CanonicalizeArgs(bazelArgs)
	if err != nil {
		return nil, nil, nil, err
	}
	for _, p := range plugins {
		bazelArgs, execArgs, err = p.PreBazel(bazelArgs, execArgs)
		if err != nil {
			return nil, nil, nil, err
		}
	}

	// Save some flags from the current invocation, in case the `ask` command
	// is invoked in the future.
	bazelArgs = ask.SaveFlags(bazelArgs)

	// Note: sidecar is configured after pre-bazel plugins, since pre-bazel
	// plugins may change the value of bes_backend, remote_cache,
	// remote_instance_name, etc.
	bazelArgs = sidecar.ConfigureSidecar(bazelArgs)

	return plugins, bazelArgs, execArgs, nil
}

// Run runs a bazel command, including setup and post-bazel handlers.
//
// originalArgs contains the command as originally typed. We pass it as
// EXPLICIT_COMMAND_LINE metadata to the bazel invocation.
func Run(args []string, originalArgs []string) (int, error) {
	// Maybe run interactively (watching for changes to files).
	if exitCode, err := watcher.Watch(); exitCode >= 0 || err != nil {
		return 1, err
	}

	// Show a picker if target argument is omitted.
	args = picker.HandlePicker(args)

	//Prepare a dir for temporary files created by this CLI run
	tempDir, err := os.MkdirTemp("", "buildbuddy-cli-*")
	if err != nil {
		return 1, err
	}

	var scriptPath string
	var exitCode int
	defer func() {
		// Remove tempdir. Need to do this before invoking the run script
		// (if applicable), since the run script will replace this process.
		os.RemoveAll(tempDir)

		// Invoke the run script only if the build succeeded.
		if exitCode == 0 && scriptPath != "" {
			exitCode, err = bazelisk.InvokeRunScript(scriptPath)
		}
	}()

	plugins, bazelArgs, execArgs, err := Setup(args, tempDir)
	if err != nil {
		return 1, err
	}

	// If this is a `bazel run` command, add a --run_script arg so that
	// we can execute post-bazel plugins between the build and the run step.
	bazelArgs, scriptPath, err = bazelisk.ConfigureRunScript(bazelArgs)
	if err != nil {
		return 1, err
	}
	// Append metadata just before running bazelisk.
	// Note, this means plugins cannot modify this metadata.
	if len(originalArgs) > 0 {
		bazelArgs, err = metadata.AppendBuildMetadata(bazelArgs, originalArgs)
		if err != nil {
			return 1, err
		}
	}

	// Run bazelisk, capturing the original output in a file and allowing
	// plugins to control how the output is rendered to the terminal.
	log.Debugf("CLI finished initialization at %s", time.Now())
	outputPath := filepath.Join(tempDir, "bazel.log")
	exitCode, err = plugin.RunBazeliskWithPlugins(
		arg.JoinExecutableArgs(bazelArgs, execArgs),
		outputPath, plugins)
	if err != nil {
		return 1, err
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
			return 1, err
		}
	}

	// TODO: Support post-run hooks?
	// e.g. show a desktop notification once a k8s deploy has finished

	return exitCode, nil
}
