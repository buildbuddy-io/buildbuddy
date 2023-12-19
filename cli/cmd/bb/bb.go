package main

import (
	"os"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/ask"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/cli/common"
	"github.com/buildbuddy-io/buildbuddy/cli/help"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/metadata"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/picker"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/cli/shortcuts"
	"github.com/buildbuddy-io/buildbuddy/cli/sidecar"
	"github.com/buildbuddy-io/buildbuddy/cli/tooltag"
	"github.com/buildbuddy-io/buildbuddy/cli/watcher"
	"github.com/buildbuddy-io/buildbuddy/server/util/rlimit"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	sidecarmain "github.com/buildbuddy-io/buildbuddy/cli/cmd/sidecar"
)

var (
	// These flags configure the cli at large, and don't apply to any specific
	// cli command
	globalCliFlags = map[string]struct{}{
		// Set to print verbose cli logs
		"verbose": {},
	}
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

	args := handleGlobalCliFlags(os.Args[1:])

	log.Debugf("CLI started at %s", start)
	log.Debugf("args[0]: %s", os.Args[0])
	log.Debugf("env: BAZEL_REAL=%s", os.Getenv("BAZEL_REAL"))
	log.Debugf("env: BAZELISK_SKIP_WRAPPER=%s", os.Getenv("BAZELISK_SKIP_WRAPPER"))
	log.Debugf("env: USE_BAZEL_VERSION=%s", os.Getenv("USE_BAZEL_VERSION"))
	log.Debugf("env: BB_USE_BAZEL_VERSION=%s", os.Getenv("BB_USE_BAZEL_VERSION"))

	bazelisk.ResolveVersion()

	err = rlimit.MaxRLimit()
	if err != nil {
		log.Printf("Failed to bump open file limits: %s", err)
	}

	// Expand command shortcuts like b=>build, t=>test, etc.
	args = shortcuts.HandleShortcuts(args)

	// Make sure startup args are always in the format --foo=bar.
	args, err = parser.CanonicalizeStartupArgs(args)
	if err != nil {
		return -1, err
	}

	// Handle help command.
	exitCode, err = help.HandleHelp(args)
	if err != nil || exitCode >= 0 {
		return exitCode, err
	}

	cliCmd := args[0]
	for _, c := range cli_command.Commands {
		isAlias := false
		for _, alias := range c.Aliases {
			if cliCmd == alias {
				isAlias = true
			}
		}

		if isAlias || cliCmd == c.Name {
			// If the first argument is a cli command, trim it from `args`
			args = args[1:]
			if c.Handler != nil {
				exitCode, err := c.Handler(args)
				if exitCode == common.ForwardCommandToBazelExitCode {
					// Add the cli command back to args before forwarding
					// the command to bazel
					args = append([]string{cliCmd}, args...)
					break
				}
				return exitCode, err
			}
		}
	}

	// Forward the command to bazel.
	// If none of the CLI subcommand handlers were triggered, assume we should
	// handle it as a bazel command.

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

	// Show a picker if target argument is omitted.
	args = picker.HandlePicker(args)

	// Parse args.
	bazelArgs, execArgs := arg.SplitExecutableArgs(args)
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
	args, err = login.ConfigureAPIKey(args)
	if err != nil {
		return -1, err
	}

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
		args, execArgs, err = p.PreBazel(args, execArgs)
		if err != nil {
			return -1, err
		}
	}

	// For the ask command, we want to save some flags from the most recent invocation.
	args = ask.SaveFlags(args)

	// Note: sidecar is configured after pre-bazel plugins, since pre-bazel
	// plugins may change the value of bes_backend, remote_cache,
	// remote_instance_name, etc.
	args = sidecar.ConfigureSidecar(args)

	// Handle remote bazel. Note, pre-bazel hooks apply to remote bazel, but not
	// output handlers or post-bazel hooks.
	if cliCmd == "remote" {
		return remotebazel.HandleRemoteBazel(args, execArgs)
	}

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
		arg.JoinExecutableArgs(args, execArgs),
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

// handleGlobalCliFlags processes global cli args that don't apply to any specific subcommand
// (--verbose, etc.).
// Returns args with all global cli flags removed
func handleGlobalCliFlags(args []string) []string {
	for flag := range globalCliFlags {
		var flagVal string
		flagVal, args = arg.Pop(args, flag)

		// Even if flag is not set and flagVal is "", pass to handlers in case
		// they need to configure a default value
		switch flag {
		case "verbose":
			log.Configure(flagVal)
		}
	}
	return args
}
