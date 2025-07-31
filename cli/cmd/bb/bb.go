package main

import (
	"maps"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/arg"
	"github.com/buildbuddy-io/buildbuddy/cli/bazelisk"
	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/cli/help"
	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/metadata"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
	"github.com/buildbuddy-io/buildbuddy/cli/picker"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/runscript"
	"github.com/buildbuddy-io/buildbuddy/cli/setup"
	"github.com/buildbuddy-io/buildbuddy/cli/watcher"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/seq"
	"github.com/buildbuddy-io/buildbuddy/server/util/rlimit"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	register_cli_commands "github.com/buildbuddy-io/buildbuddy/cli/cli_command/register"
	sidecarmain "github.com/buildbuddy-io/buildbuddy/cli/cmd/sidecar"
	helpoptdef "github.com/buildbuddy-io/buildbuddy/cli/help/option_definitions"
	logoptdef "github.com/buildbuddy-io/buildbuddy/cli/log/option_definitions"
	watchoptdef "github.com/buildbuddy-io/buildbuddy/cli/watcher/option_definitions"
)

const (
	bazelExitCodeTransientBESFailure  = 38
	bazelExitCodePersistentBESFailure = 45
	bazelExitCodeInterrupted          = 8
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

// StartDebug prints relevant debug information related to CLI startup. It is
// intended to be called after Configure, as Configure sets log levels.
func StartupDebug(start time.Time) {
	log.Debugf("CLI started at %s", start)
	log.Debugf("args[0]: %s", os.Args[0])
	log.Debugf("env: BAZEL_REAL=%s", os.Getenv("BAZEL_REAL"))
	log.Debugf("env: BAZELISK_SKIP_WRAPPER=%s", os.Getenv("BAZELISK_SKIP_WRAPPER"))
	log.Debugf("env: USE_BAZEL_VERSION=%s", os.Getenv("USE_BAZEL_VERSION"))
	log.Debugf("env: BB_USE_BAZEL_VERSION=%s", os.Getenv("BB_USE_BAZEL_VERSION"))
}

// Configure takes a slice of options and filters them for the global options
// needed to configure logging and the watcher, accumulates those options to get
// the necessary values, and then configures logging and the watcher.
//
// We template this top support different types of Option slices; for example,
// []*parsed.IndexedOption
func Configure[T options.Option](bbOpts []T) {
	verbose, err := options.AccumulateValues[T](
		*options.NewBoolOrEnum(false),
		seq.Filter(bbOpts, options.NameFilter[T](logoptdef.Verbose.Name())),
	)
	if err != nil {
		log.Configure("")
		log.Warnf("Error encountered reading '%s' flag: %s", logoptdef.Verbose.Name(), err)
	} else if e, ok := verbose.GetEnum(); ok {
		log.Configure(e)
	} else if b, ok := verbose.GetBool(); ok {
		if b {
			log.Configure("1")
		} else {
			log.Configure("0")
		}
	} else {
		// This shouldn't happen
		log.Configure("")
		log.Warnf("Failed to interpret '%s' flag", logoptdef.Verbose.Name())
	}

	watch, err := options.AccumulateValues[T](
		false,
		seq.Filter(bbOpts, options.NameFilter[T](watchoptdef.Watch.Name())),
	)
	if err != nil {
		log.Warnf("Error encountered reading '%s' flag: %s", watchoptdef.Watch.Name(), err)
	}
	if watch {
		watcherFlags, err := options.AccumulateValues[T](
			[]string{},
			seq.Filter(bbOpts, options.NameFilter[T](watchoptdef.WatcherFlags.Name())),
		)
		if err != nil {
			log.Warnf("Error encountered reading '%s' flag: %s", watchoptdef.WatcherFlags.Name(), err)
		}
		watcher.Configure(true, watcherFlags)
	}
}

func run() (exitCode int, err error) {
	start := time.Now()
	// Record original arguments so we can show them in the UI.
	originalArgs := slices.Clone(os.Args)

	if err := rlimit.MaxRLimit(); err != nil {
		log.Printf("Failed to bump open file limits: %s", err)
	}

	// special case if the only argument is `--version`: this is not a proper
	// startup option, but rather a special case in bazel that skips starting a
	// bazel server while still allowing access to version information.
	if len(os.Args[1:]) == 1 && os.Args[1] == "--version" {
		return bazelisk.Run(os.Args[1:], &bazelisk.RunOpts{Stdout: os.Stdout, Stderr: os.Stderr})
	}

	// Register all known cli commands so that we can query or iterate them later.
	register_cli_commands.Register()

	// If this is a bb command, parse any global flags that precede it and handle
	// the command.
	// This is to shortcut the startup-time of the bazel client / server if they
	// do not need to be run.
	if opts, command, args := interpretAsBBCliCommand(os.Args[1:]); command != nil && command.Name != "help" {
		// Let the help parser handle a help command; otherwise, let's handle the
		// CLI command.
		Configure(opts)
		StartupDebug(start)
		// If the first argument is a cli command, trim it from `args`
		args = args[1:]
		return command.Handler(args)
	}

	// Since we do overly-permissive parsing for help commands to try to reduce
	// frustration when learning how to use the CLI, we should attempt to parse
	// this as a help command.
	if helpArgs, err := interpretAsHelpCommand(os.Args[1:]); err != nil {
		return -1, err
	} else if helpArgs.GetCommand() == "help" {
		// Handle help command if applicable.
		Configure(helpArgs.RemoveStartupOptions(logoptdef.Verbose.Name(), watchoptdef.Watch.Name(), watchoptdef.WatcherFlags.Name()))
		StartupDebug(start)
		return runHelp(helpArgs)
	}

	if _, err := bazelisk.ResolveVersion(); err != nil {
		log.Printf("Failed to resolve bazel version: %s", err)
	}

	// This was neither a bb CLI command nor a help command; interpret it as a
	// bazel command.
	parsedArgs, err := parser.ParseArgs(os.Args[1:])
	if err != nil {
		return -1, err
	}
	Configure(parsedArgs.RemoveStartupOptions(logoptdef.Verbose.Name(), watchoptdef.Watch.Name(), watchoptdef.WatcherFlags.Name()))
	StartupDebug(start)
	parsedArgs, err = parser.ResolveArgs(parsedArgs)
	if err != nil {
		return -1, err
	}
	canonicalizedArgs := parsedArgs.Canonicalized()

	// If none of the CLI subcommand handlers were triggered, assume we should
	// handle it as a bazel command.
	return handleBazelCommand(start, canonicalizedArgs.Format(), originalArgs)
}

// interpretAsBBCliCommand strips the bb options from the beginning of a bb
// command and returns the options, the command, and the truncated args. If any
// unrecognized option is encountered before the first positional argument or if
// the first positional argument is not a bb command (like it might be if it
// were a bazel command, for example), the args are returned untouched, the
// options will be nil, and the command will be nil.
func interpretAsBBCliCommand(args []string) ([]options.Option, *cli_command.Command, []string) {
	p := parser.GetNativeParser().StartupOptionParser
	p.Permissive = true
	opts, argIndex, err := p.ParseOptions(args, "startup")
	if err != nil {
		log.Warnf("Error parsing global options: %s", err)
		return nil, nil, args
	}
	for _, opt := range opts {
		if opt.PluginID() == options.UnknownBuiltinPluginID {
			return nil, nil, args
		}
	}
	if argIndex == len(args) {
		return nil, nil, args
	}
	if command := cli_command.GetCommand(args[argIndex]); command != nil {
		return opts, command, args[argIndex:]
	}
	return nil, nil, args
}

func interpretAsHelpCommand(args []string) (*parsed.OrderedArgs, error) {
	helpParser, err := parser.GetHelpParser()
	if err != nil {
		return nil, err
	}
	helpParser.CommandOptionParser.Permissive = true
	helpArgs, err := helpParser.ParseArgs(args)
	if err != nil {
		return nil, err
	}

	helpOptionValue, err := options.AccumulateValues[*parsed.IndexedOption](
		false,
		slices.Concat(
			helpArgs.RemoveStartupOptions(helpoptdef.Help.Name()),
			helpArgs.RemoveCommandOptions(helpoptdef.Help.Name()),
		),
	)
	if err != nil {
		return nil, err
	}
	if helpOptionValue {
		i, _ := parsed.Find[*parsed.Command](helpArgs.Args)
		if i == -1 {
			i = len(helpArgs.Args)
		}
		helpParser.CommandOptionParser.Permissive = false
		reparsed, err := helpParser.ParseArgs(
			(&parsed.OrderedArgs{
				Args: append(
					[]arguments.Argument{&arguments.PositionalArgument{Value: "help"}},
					helpArgs.Args[i:]...,
				),
			}).Format(),
		)
		if err != nil {
			return nil, err
		}
		helpArgs.Args = append(arguments.FromConcrete(helpArgs.Args[:i]), reparsed.Args...)
	}
	return helpArgs, nil
}

func runHelp(args *parsed.OrderedArgs) (int, error) {
	// Let's extract any command options from the startup options
	helpParser, err := parser.GetHelpParser()
	if err != nil {
		return -1, err
	}
	startupCommandOptions := args.RemoveStartupOptions(slices.Collect(maps.Keys(helpParser.CommandOptionParser.ByName))...)
	var toPrepend []arguments.Argument
	for _, indexed := range startupCommandOptions {
		o := indexed.Option
		o.SetDefinition(helpParser.CommandOptionParser.ByName[o.Name()])
		toPrepend = append(toPrepend, o)
	}
	// Now prepend any command options extracted from the startup options to the
	// command options.
	if len(toPrepend) != 0 {
		startupOpts := args.GetStartupOptions()
		if len(startupOpts) == len(args.Args) {
			// we need a command if we want command options
			args.Append(&arguments.PositionalArgument{Value: "help"})
		}
		args.Args = slices.Concat(
			arguments.FromConcrete(startupOpts),
			args.Args[len(startupOpts):len(startupOpts)+1],
			toPrepend,
			args.Args[len(startupOpts)+1:],
		)
	}
	args, err = helpParser.ResolveArgs(args)
	if err != nil {
		return -1, err
	}
	return help.HandleHelp(args)
}

// handleBazelCommand handles a native bazel command (i.e. commands that are
// directly forwarded to bazel, as opposed to bb cli-specific commands)
//
// originalArgs contains the command as originally typed. We pass it as
// EXPLICIT_COMMAND_LINE metadata to the bazel invocation.
func handleBazelCommand(start time.Time, args []string, originalArgs []string) (int, error) {
	// Maybe run interactively (watching for changes to files).
	if exitCode, err := watcher.Watch(append([]string{os.Args[0]}, args...)); exitCode >= 0 || err != nil {
		return exitCode, err
	}

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
			exitCode, err = runscript.Invoke(scriptPath)
		}
	}()

	plugins, bazelArgs, execArgs, sidecar, err := setup.Setup(args, tempDir)
	if err != nil {
		return 1, err
	}

	// Show a picker if the target is omitted. Note: we do this after expanding
	// args, in case a bazelrc specifies the target patterns (e.g. via
	// --target_pattern_file).
	//
	// Note: if there are args after "--" then we never show the picker:
	// - For "build"/"test" commands, the args after "--" are target patterns.
	// - For "run" commands, if there is no target pattern, then the first arg
	//   after "--" is treated as the pattern.
	if len(execArgs) == 0 {
		bazelArgs = picker.HandlePicker(bazelArgs)
	}

	// If this is a `bazel run` command, add a --run_script arg so that
	// we can execute post-bazel plugins between the build and the run step.
	bazelArgs, scriptPath, err = runscript.Configure(bazelArgs)
	if err != nil {
		return 1, err
	}
	// Append metadata just before running bazelisk.
	// Note, this means plugins cannot modify this metadata.
	bazelArgs, err = metadata.AppendBuildMetadata(bazelArgs, originalArgs)
	if err != nil {
		return 1, err
	}

	// Run bazelisk, capturing the original output in a file and allowing
	// plugins to control how the output is rendered to the terminal.
	log.Debugf("bb initialized in %s", time.Since(start))
	outputPath := filepath.Join(tempDir, "bazel.log")
	exitCode, err = plugin.RunBazeliskWithPlugins(
		arg.JoinExecutableArgs(bazelArgs, execArgs),
		outputPath, plugins)
	if err != nil {
		return 1, err
	}

	// If the build was interrupted (Ctrl+C), don't run post-bazel plugins.
	if exitCode == bazelExitCodeInterrupted {
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

	// If bazel failed with BES errors, show sidecar warnings/errors to help diagnose.
	if exitCode != 0 && sidecar != nil &&
		(exitCode == bazelExitCodeTransientBESFailure ||
			exitCode == bazelExitCodePersistentBESFailure) {
		sidecar.PrintLogsSince(start)
	}

	return exitCode, nil
}
