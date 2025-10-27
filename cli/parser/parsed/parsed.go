// Package parsed provides types and functions for manipulating parsed bazel
// arguments.
//
// It abstracts operations on sets of bazel arguments so that they can be easily
// modified and inspected without the user of library having to concern
// themselves with any of the implementation details of bazel command-line
// parsing.
package parsed

import (
	"fmt"
	"iter"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/server/util/lib/set"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
)

type Args interface {
	// Format renders the Args as a (lexed) slice of string tokens.
	Format() []string

	// Canonicalized does not modify the original Args, and instead returns a new
	// Args, which represents the arguments after being canonicalized.
	// Canonicalization consists of:
	// - removing non-multi options that are overridden later in the arguments
	// - reordering the arguments to match the form:
	//     [startupOptions...] [command [commandOptions...] [targets... ["--" execArgs...]]]
	//   or, if Args contains ar least one negative target (prefixed by "-"):
	//     [startupOptions...] command [commandOptions...] "--" targets... [execArgs...]
	// - replacing all options with their normalized forms, as returned by their
	//   Normalized() method.
	//
	// Note that Canonicalized does NOT sort options lexicographically, as
	// Canonicalized does not require that all options are expanded, so sorting
	// could lead to a re-ordering of options that, after expansion, changes which
	// options are overridden or the order of positional arguments, and
	// canonicalization should of course never result in semantic changes to the
	// Args.
	//
	// Additionally, no guarantee is made that this a deep clone, so changes to
	// the underlying Arguments in either the original or the canonicalized Args
	// may affect the Arguments in the other.
	Canonicalized() Args

	// GetStartupOptions returns a slice of all the startup options Args contains.
	GetStartupOptions() []options.Option

	// GetCommand returns the command this Args contains, or "help" if it does not
	// contain a command.
	GetCommand() string

	// GetStartupOptions returns a slice of all the command options Args contains.
	GetCommandOptions() []options.Option

	// GetTargets returns a slice of all the bazel targets Args contains.
	GetTargets() []*arguments.PositionalArgument

	// GetExecArgs returns a slice of all the arguments bazel will forward to the
	// executable it will run. Executable args only ever exist in the presence of
	// a "run" command.
	GetExecArgs() []*arguments.PositionalArgument

	// GetStartupOptionsByName returns a slice of all startup options whose
	// definitions have names that match optionName in the order in which they
	// appear in Args. The accompanying index is the index at which they are
	// located in Args.
	GetStartupOptionsByName(optionName string) []*IndexedOption

	// GetCommandOptionsByName returns a slice of all command options whose
	// definitions have names that match optionName in the order in which they
	// appear in Args. The accompanying index is the index at which they are
	// located in Args.
	GetCommandOptionsByName(optionName string) []*IndexedOption

	// RemoveStartupOptions removes all startup options whose definitions have
	// names that match optionName and returns a slice of all removed options, if
	// any, in the order in which they appeared in the args. The accompanying
	// index is the index at which they were removed in Args.
	RemoveStartupOptions(...string) []*IndexedOption

	// RemoveCommandOptions removes all command options whose definitions have
	// names that match optionName and returns a slice of all removed options, if
	// any, in the order in which they appeared in the args. The accompanying
	// index is the index at which they were removed in Args.
	RemoveCommandOptions(...string) []*IndexedOption

	// Append appends the given arguments by inserting them in the last valid
	// location for that type of argument. Startup options
	Append(...arguments.Argument) error

	// SplitExecutableArgs returns the args split up between the args that bazel
	// consumes directly and the args that will passed to the executable bazel is
	// running. This is a concept that only makes sense when using the `run`
	// command. The arguments that are passed to the executable are all of the
	// positional arguments that follow the target that is passed to the bazel run
	// command.
	SplitExecutableArgs() ([]string, []string)
}

// Interface for argument classifications to implement
type Classified interface {
	Arg() arguments.Argument

	// lower case method to stop declaration of structs that satisfy Classified
	// from outside the package.
	classified()
}

// Interface for argument classifications to implement if they describe options
type IsOption interface {
	AsOption() options.Option
}

// Interface to use as a constraint in the option retrieval methods.
type ClassifiedOption interface {
	Classified
	IsOption
}

// Argument classifications

type UnsupportedArgument struct{ arguments.Argument }

func (u *UnsupportedArgument) Arg() arguments.Argument { return u.Argument }
func (u *UnsupportedArgument) classified()             {}

type StartupOption struct{ options.Option }

func (s *StartupOption) Arg() arguments.Argument  { return s.Option }
func (s *StartupOption) AsOption() options.Option { return s.Option }
func (s *StartupOption) classified()              {}

type Command struct{ *arguments.PositionalArgument }

func (c *Command) Arg() arguments.Argument { return c.PositionalArgument }
func (c *Command) classified()             {}

type CommandOption struct{ options.Option }

func (c *CommandOption) Arg() arguments.Argument  { return c.Option }
func (c *CommandOption) AsOption() options.Option { return c.Option }
func (c *CommandOption) classified()              {}

type DoubleDash struct{ *arguments.DoubleDash }

func (d *DoubleDash) Arg() arguments.Argument { return d.DoubleDash }
func (d *DoubleDash) classified()             {}

type Target struct{ *arguments.PositionalArgument }

func (t *Target) Arg() arguments.Argument { return t.PositionalArgument }
func (t *Target) classified()             {}

type ExecArg struct{ *arguments.PositionalArgument }

func (e *ExecArg) Arg() arguments.Argument { return e.PositionalArgument }
func (e *ExecArg) classified()             {}

// classifier tracks relevant context while iterating over a slice of
// Arguments in command-line order. It is primarily intended for use
// by the `Classify` and `Find` functions.
type classifier struct {
	command          *string
	foundFirstTarget bool
}

func (c *classifier) ClassifyAndAccumulate(arg arguments.Argument) Classified {
	classified := c.Classify(arg)
	c.Accumulate(classified)
	return classified
}

func (c *classifier) Accumulate(classified Classified) {
	switch v := classified.(type) {
	case *Command:
		command := v.Value
		c.command = &command
	case *Target:
		c.foundFirstTarget = true
	}
}

func (c *classifier) Classify(arg arguments.Argument) Classified {
	switch arg := arg.(type) {
	case *arguments.DoubleDash:
		return &DoubleDash{arg}
	case *arguments.PositionalArgument:
		switch {
		case c.command == nil:
			return &Command{arg}
		case *c.command == "run" && c.foundFirstTarget:
			return &ExecArg{arg}
		default:
			return &Target{arg}
		}
	case options.Option:
		if c.command == nil {
			return &StartupOption{arg}
		}
		return &CommandOption{arg}
	}
	return &UnsupportedArgument{arg}
}

// Classify takes a slice of elements that implement arguments.Argument and
// returns a sequence of those Arguments in the same order, wrapped in
// Classified types. Classify assumes that the arguments passed in are a full
// list of arguments in command-line order and has unspecified behavior if they
// are not. Command-line order looks like:
//
//	[ startupOptions... ]
//	[
//	  command [ commandOption | target ]...
//	  {
//	    [ { commandOption | execArg } ]... [ "--" [ execArgs...] ]  |
//	    "--" targets... [ execArgs... ]
//	  }
//	]
func Classify[E arguments.Argument](args []E) iter.Seq2[int, Classified] {
	return func(yield func(int, Classified) bool) {
		c := &classifier{}
		for i, a := range args {
			if !yield(i, c.ClassifyAndAccumulate(a)) {
				return
			}
		}
	}
}

// Find takes a slice of elements that implement arguments.Argument and returns
// the first index and argument element that is classified as the provided type
// `T`, with the argument in question wrapped in the provided type. If no
// qualifying argument is found, Find returns -1 and the zero-value of T. Find
// assumes that the Arguments passed in are a full list of arguments in
// command-line order and has unspecified behavior if they are not. Command-line
// order looks like:
//
//	[ startupOptions... ]
//	[
//	  command [ commandOption | target ]...
//	  {
//	    [ { commandOption | execArg } ]... [ "--" [ execArgs...] ]  |
//	    "--" targets... [ execArgs... ]
//	  }
//	]
func Find[T Classified, E arguments.Argument](args []E) (int, T) {
	for i, c := range Classify(args) {
		if t, ok := c.(T); ok {
			return i, t
		}
	}
	return -1, *new(T)
}

// OrderedArgs is a parsed representation of Arguments that will faithfully
// reproduce the string slice they were generated from if `Format()` is called.
type OrderedArgs struct {
	Args []arguments.Argument
}

// PartitionedArgs is a parsed representation of Arguments that has been
// partitioned by argument classification (startup options, command, command
// options, targets, and exec args). It is more performant when adding or
// removing arguments than OrderedArgs, and `Format()` will always produce
// args of the form:
// [startupOptions...] [command [commandOptions...] { [targets... ["--" execArgs...]] | "--" targets... [execArgs] } ]
// "--" will precede `targets` iff there is at least one negative target (a
// target prefixed with "-").
type PartitionedArgs struct {
	StartupOptions []options.Option
	Command        *string
	CommandOptions []options.Option
	Targets        []*arguments.PositionalArgument
	ExecArgs       []*arguments.PositionalArgument
}

func (a *OrderedArgs) Format() []string {
	tokens := []string{}
	for _, arg := range a.Args {
		if arg == nil {
			tokens = append(tokens, "<nil>")
			continue
		}
		tokens = append(tokens, arg.Format()...)
	}
	return tokens
}

func (a *OrderedArgs) Canonicalized() Args {
	partitioned := Partition(a.Args)
	partitioned.StartupOptions = options.Canonicalize(partitioned.StartupOptions)
	partitioned.CommandOptions = options.Canonicalize(partitioned.CommandOptions)
	if partitioned.Command == nil {
		command := "help"
		partitioned.Command = &command
	}
	return partitioned
}

func (a *OrderedArgs) GetStartupOptions() []options.Option {
	var startupOptions []options.Option
	for _, c := range Classify(a.Args) {
		switch c := c.(type) {
		case *StartupOption:
			startupOptions = append(startupOptions, c.Option)
		case *Command:
			return startupOptions
		}
	}
	return startupOptions
}

func (a *OrderedArgs) GetCommand() string {
	if i, command := Find[*Command](a.Args); i != -1 {
		return command.Value
	}
	// bazel treats no command as a "help" command.
	return "help"
}

func (a *OrderedArgs) GetCommandOptions() []options.Option {
	var commandOptions []options.Option
	for _, c := range Classify(a.Args) {
		switch c := c.(type) {
		case *DoubleDash:
			return commandOptions
		case *CommandOption:
			commandOptions = append(commandOptions, c.Option)
		}
	}
	return commandOptions
}

func (a *OrderedArgs) GetTargets() []*arguments.PositionalArgument {
	var targets []*arguments.PositionalArgument
	command := ""
	for _, c := range Classify(a.Args) {
		switch c := c.(type) {
		case *Command:
			command = c.Value
		case *Target:
			targets = append(targets, c.PositionalArgument)
			if command == "run" {
				// "run" command has only one target; return early
				return targets
			}
		}
	}
	return targets
}

func (a *OrderedArgs) GetExecArgs() []*arguments.PositionalArgument {
	var execArgs []*arguments.PositionalArgument
	for _, c := range Classify(a.Args) {
		switch c := c.(type) {
		case *Command:
			if c.Value != "run" {
				// only "run" has ExecArgs; return early
				return nil
			}
		case *ExecArg:
			execArgs = append(execArgs, c.PositionalArgument)
		}
	}
	return execArgs
}

func (a *OrderedArgs) SplitExecutableArgs() ([]string, []string) {
	bazelArgs := []string{}
	executableArgs := []string{}
	for _, c := range Classify(a.Args) {
		switch c := c.(type) {
		case *Command:
			if c.Value != "run" {
				// there are no executable args to split out.
				return a.Format(), nil
			}
			bazelArgs = append(bazelArgs, c.Format()...)
		case *DoubleDash:
			continue
		case *ExecArg:
			executableArgs = append(executableArgs, c.Format()...)
		case *Target:
			if strings.HasPrefix(c.GetValue(), "-") {
				// This is a negative target to a bazel "run" command; the only way this
				// can happen is if it was immediately preceded by a double dash. This
				// command won't run correctly, but restore the double dash to let bazel
				// output the correct error to the user.
				bazelArgs = append(bazelArgs, "--")
			}
			bazelArgs = append(bazelArgs, c.Format()...)
		default:
			bazelArgs = append(bazelArgs, c.Arg().Format()...)
		}
	}
	return bazelArgs, executableArgs
}

// IndexedOption is used by Args.GetStartupOptionsByName,
// Args.GetCommandOptionsByName, Args.RemoveStartupOptions, and
// Args.RemoveCommandOptions to couple options with their associated indices
// when returning them.
type IndexedOption struct {
	options.Option
	Index int
}

func (a *OrderedArgs) GetStartupOptionsByName(optionName string) []*IndexedOption {
	return getOptionsByName[*StartupOption](a, optionName)
}

func (a *OrderedArgs) GetCommandOptionsByName(optionName string) []*IndexedOption {
	return getOptionsByName[*CommandOption](a, optionName)
}

func getOptionsByName[DesiredOption ClassifiedOption](a *OrderedArgs, optionName string) []*IndexedOption {
	var matchedOptions []*IndexedOption
	for i, c := range Classify(a.Args) {
		if c, ok := c.(DesiredOption); ok && c.AsOption().Name() == optionName {
			matchedOptions = append(matchedOptions, &IndexedOption{Option: c.AsOption(), Index: i})
		}
	}
	return matchedOptions
}

func (a *OrderedArgs) RemoveStartupOptions(optionNames ...string) []*IndexedOption {
	return removeOptions[*StartupOption, *Command](a, optionNames...)
}

func (a *OrderedArgs) RemoveCommandOptions(optionNames ...string) []*IndexedOption {
	return removeOptions[*CommandOption, *DoubleDash](a, optionNames...)
}

func removeOptions[DesiredOption ClassifiedOption, Terminates Classified](a *OrderedArgs, optionNames ...string) []*IndexedOption {
	toRemove := set.From(optionNames...)
	var removed []*IndexedOption
	c := &classifier{}
	for i := 0; i < len(a.Args); i++ {
		switch v := c.ClassifyAndAccumulate(a.Args[i]).(type) {
		case DesiredOption:
			if toRemove.Contains(v.AsOption().Name()) {
				a.Args = slices.Delete(a.Args, i, i+1)
				removed = append(removed, &IndexedOption{Option: v.AsOption(), Index: i})
				// account for the fact that the next element is now at index i instead
				// of i+1
				i--
			}
		case Terminates:
			return removed
		}
	}
	return removed
}

func (a *OrderedArgs) Append(args ...arguments.Argument) error {
	startupOptionInsertIndex := -1
	commandOptionInsertIndex := -1
	for _, arg := range args {
		switch arg := arg.(type) {
		case *arguments.DoubleDash:
			var err error
			if startupOptionInsertIndex, commandOptionInsertIndex, err = a.appendDoubleDash(startupOptionInsertIndex, commandOptionInsertIndex); err != nil {
				return err
			}
		case *arguments.PositionalArgument:
			startupOptionInsertIndex, commandOptionInsertIndex = a.appendPositionalArgument(arg, startupOptionInsertIndex, commandOptionInsertIndex)
		case options.Option:
			var err error
			if startupOptionInsertIndex, commandOptionInsertIndex, err = a.appendOption(arg, startupOptionInsertIndex, commandOptionInsertIndex); err != nil {
				return err
			}
		default:
			return fmt.Errorf("Append only supports Options and PositionalArguments, but %#v was of type %T.", arg, arg)
		}
	}
	return nil
}

func (a *OrderedArgs) appendDoubleDash(startupOptionInsertIndex, commandOptionInsertIndex int) (int, int, error) {
	if startupOptionInsertIndex == -1 {
		if startupOptionInsertIndex, _ = Find[*Command](a.Args); startupOptionInsertIndex == -1 {
			startupOptionInsertIndex = len(a.Args)
		}
	}
	if startupOptionInsertIndex == len(a.Args) {
		// There is no command; we cannot add a double dash.
		return startupOptionInsertIndex, commandOptionInsertIndex, fmt.Errorf("Failed to append double-dash: double-dash is not supported when no command is present.")
	}
	if commandOptionInsertIndex == -1 {
		if commandOptionInsertIndex, _ = Find[*DoubleDash](a.Args[startupOptionInsertIndex:]); commandOptionInsertIndex == -1 {
			commandOptionInsertIndex = len(a.Args)
		}
	}

	if commandOptionInsertIndex == len(a.Args) {
		// only append if we don't already have a double-dash
		a.Args = append(a.Args, &arguments.DoubleDash{})
	}
	return startupOptionInsertIndex, commandOptionInsertIndex, nil
}

func (a *OrderedArgs) appendPositionalArgument(arg *arguments.PositionalArgument, startupOptionInsertIndex, commandOptionInsertIndex int) (int, int) {
	if startupOptionInsertIndex == -1 {
		if startupOptionInsertIndex, _ = Find[*Command](a.Args); startupOptionInsertIndex == -1 {
			startupOptionInsertIndex = len(a.Args)
		}
	}
	if startupOptionInsertIndex == len(a.Args) {
		// We're appending the command
		a.Args = append(a.Args, arg)
		return startupOptionInsertIndex, len(a.Args)
	}
	if commandOptionInsertIndex == -1 {
		if commandOptionInsertIndex, _ = Find[*DoubleDash](a.Args[startupOptionInsertIndex:]); commandOptionInsertIndex == -1 {
			commandOptionInsertIndex = len(a.Args)
		}
	}
	if commandOptionInsertIndex == len(a.Args) {
		if strings.HasPrefix(arg.GetValue(), "-") {
			a.Args = append(a.Args, &arguments.DoubleDash{})
		} else {
			// If there's no double dash, commandOptionInsertIndex needs to be
			// incremented to still be len(p.Args) after we append the argument for
			// when we return.
			commandOptionInsertIndex += 1
		}
	}
	a.Args = append(a.Args, arg)
	return startupOptionInsertIndex, commandOptionInsertIndex
}

func (a *OrderedArgs) appendOption(option options.Option, startupOptionInsertIndex, commandOptionInsertIndex int) (int, int, error) {
	if startupOptionInsertIndex == -1 {
		if startupOptionInsertIndex, _ = Find[*Command](a.Args); startupOptionInsertIndex == -1 {
			startupOptionInsertIndex = len(a.Args)
		}
	}
	command := "startup"
	if startupOptionInsertIndex < len(a.Args) {
		command = a.Args[startupOptionInsertIndex].GetValue()
	}
	if option.PluginID() == options.UnknownBuiltinPluginID && !option.HasSupportedCommands() {
		// If this is an unknown option with no listed supported commands, assume it
		// supports this command (or "startup" in the rare case that no command was
		// provided.
		option.GetDefinition().AddSupportedCommand(command)
	}
	if option.Supports("startup") {
		a.Args = slices.Insert(a.Args, startupOptionInsertIndex, arguments.Argument(option))
		startupOptionInsertIndex += 1
		if commandOptionInsertIndex != -1 {
			commandOptionInsertIndex += 1
		}
		return startupOptionInsertIndex, commandOptionInsertIndex, nil
	}
	if command == "startup" {
		return startupOptionInsertIndex, commandOptionInsertIndex, fmt.Errorf("Failed to append Option: option '%s' is not a startup option and no command was provided.", option.Name())
	}
	if option.Supports(command) {
		if commandOptionInsertIndex == -1 {
			if startupOptionInsertIndex == -1 {
				if startupOptionInsertIndex, _ = Find[*Command](a.Args); startupOptionInsertIndex == -1 {
					startupOptionInsertIndex = len(a.Args)
				}
			}
			if commandOptionInsertIndex, _ = Find[*DoubleDash](a.Args[startupOptionInsertIndex:]); commandOptionInsertIndex == -1 {
				commandOptionInsertIndex = len(a.Args)
			}
		}
		a.Args = slices.Insert(a.Args, commandOptionInsertIndex, arguments.Argument(option))
		commandOptionInsertIndex += 1
		return startupOptionInsertIndex, commandOptionInsertIndex, nil
	}
	return startupOptionInsertIndex, commandOptionInsertIndex, fmt.Errorf("Failed to append Option: option '%s' is not a startup option and the command '%s' does not support it.", option.Name(), command)
}

// ConsumeRCFileOptions removes all rc-file related options from the provided
// args and appends an `ignore_all_rc_files` option to the startup options.
// Returns a slice of all the rc files that should be parsed.
func (a *OrderedArgs) ConsumeRCFileOptions(workspaceDir string) (rcFiles []string, err error) {
	if ignore, err := options.AccumulateValues[*IndexedOption](false, a.RemoveStartupOptions("ignore_all_rc_files")); err != nil {
		return nil, fmt.Errorf("Failed to get value from 'ignore_all_rc_files' option: %s", err)
	} else if ignore {
		// Before we do anything, check whether --ignore_all_rc_files is already
		// set. If so, return an empty list of RC files, since bazel will do the
		// same.
		a.RemoveStartupOptions("system_rc", "workspace_rc", "home_rc")
		return nil, nil
	}

	// Parse rc files in the order defined here:
	// https://bazel.build/run/bazelrc#bazelrc-file-locations
	for _, optName := range []string{"system_rc", "workspace_rc", "home_rc"} {
		if v, err := options.AccumulateValues[*IndexedOption](true, a.RemoveStartupOptions(optName)); err != nil {
			return nil, fmt.Errorf("Failed to get value from '%s' option: %s", optName, err)
		} else if !v {
			// When these flags are false, they have no effect on the list of
			// rcFiles we should parse.
			continue
		}
		switch optName {
		case "system_rc":
			rcFiles = append(rcFiles, "/etc/bazel.bazelrc")
			rcFiles = append(rcFiles, `%ProgramData%\bazel.bazelrc`)
		case "workspace_rc":
			if workspaceDir != "" {
				rcFiles = append(rcFiles, filepath.Join(workspaceDir, ".bazelrc"))
			}
		case "home_rc":
			// Use $HOME or %USERPROFILE% to locate the home_rc file.
			// This enables mocking $HOME in test environments.
			//
			// On Unix, if $HOME variable is unset, fallback to finding home directory
			// by syscall (getpwuid), which typically parse /etc/passwd for the information.
			if homeDir, osErr := os.UserHomeDir(); osErr != nil && homeDir != "" {
				rcFiles = append(rcFiles, filepath.Join(homeDir, ".bazelrc"))
			} else if currUser, userErr := user.Current(); userErr != nil && currUser.HomeDir != "" {
				rcFiles = append(rcFiles, filepath.Join(currUser.HomeDir, ".bazelrc"))
			} else {
				log.Debugf("Unable to locate home_rc: %s - %s", osErr, userErr)
			}
		}
	}
	for _, indexedOption := range a.RemoveStartupOptions("bazelrc") {
		o := indexedOption.Option
		if o.GetValue() == "/dev/null" {
			// if we encounter --bazelrc=/dev/null, that means bazel will ignore
			// subsequent --bazelrc args, so we ignore them as well.
			break
		}
		rcFiles = append(rcFiles, o.GetValue())
		continue
	}
	return rcFiles, nil
}

type Config struct {
	ByPhase map[string][]arguments.Argument
}

func NewConfig() *Config {
	return &Config{
		ByPhase: make(map[string][]arguments.Argument),
	}
}

// ExpandConfigs expands all the config options in the args, using the
// provided config parameters to resolve them. It also expands the
// `enable_platform_specific_config` option, if it exists.
func (a *OrderedArgs) ExpandConfigs(
	namedConfigs map[string]*Config,
	defaultConfig *Config,
) (*OrderedArgs, error) {
	command := a.GetCommand()
	expanded, err := a.expandConfigs(namedConfigs, defaultConfig)
	if err != nil {
		return nil, err
	}
	// Replace the last occurrence of `--enable_platform_specific_config` with
	// `--config=<bazelOS>`, so long as the last occurrence evaluates as true.
	opts := expanded.RemoveCommandOptions(bazelrc.EnablePlatformSpecificConfigFlag)
	if v, err := options.AccumulateValues[*IndexedOption](false, opts); err != nil {
		return nil, fmt.Errorf("Failed to get value from '%s' option: %s", bazelrc.EnablePlatformSpecificConfigFlag, err)
	} else if v {
		index := opts[len(opts)-1].Index
		bazelOS := bazelrc.GetBazelOS()
		if platformConfig, ok := namedConfigs[bazelOS]; ok {
			phases := bazelrc.GetPhases(command)
			expansion, err := platformConfig.appendArgsForConfig(nil, namedConfigs, phases, []string{bazelOS}, true)
			if err != nil {
				return nil, err
			}
			expanded.Args = slices.Insert(expanded.Args, index, expansion...)
			// Remove all occurrences of the enable platform-specific config flag
			// that may have been added when expanding the platform-specific config.
			expanded.RemoveCommandOptions(bazelrc.EnablePlatformSpecificConfigFlag)
		}
	}
	return expanded, nil
}

// expandConfigs expands all the config options in the args, using the
// provided config parameters to resolve them.
func (a *OrderedArgs) expandConfigs(
	namedConfigs map[string]*Config,
	defaultConfig *Config,
) (*OrderedArgs, error) {
	// Expand startup args first, before any other args (including explicit
	// startup args).
	//
	// startup config is guaranteed to only be startup options.
	startupConfig := defaultConfig.ByPhase["startup"]

	commandIndex, command := Find[*Command](a.Args)
	if commandIndex == -1 {
		// No command is a help command that does not expand anything but the startup config.
		return &OrderedArgs{Args: slices.Concat(startupConfig, a.Args)}, nil
	}
	expanded := slices.Concat(startupConfig, a.Args[:commandIndex])
	expanded = append(expanded, command.PositionalArgument)

	// Always apply bazelrc rules in order of the precedence hierarchy. For
	// example, for the "test" command, apply options in order of "always",
	// then "common", then "build", then "test".
	phases := bazelrc.GetPhases(command.GetValue())
	log.Debugf("Bazel command: %q, rc rule classes: %v", command, phases)

	// We'll refer to args in bazelrc which aren't expanded from a --config
	// option as "default" args, like a .bazelrc line that just says
	// "common -c dbg" or "build -c dbg" as opposed to something qualified like
	// "build:dbg -c dbg".
	//
	// These default args take lower precedence than explicit command line args
	// so we expand those first just after the command.
	log.Debugf("Args before expanding default rc rules: %#v", arguments.FormatAll(expanded))
	var err error
	expanded, err = defaultConfig.appendArgsForConfig(expanded, namedConfigs, phases, []string{}, true)
	if err != nil {
		return nil, fmt.Errorf("failed to evaluate bazelrc configuration: %s", err)
	}
	log.Debugf("Args after expanding default rc rules: %v", arguments.FormatAll(expanded))
	expanded, err = appendExpansion(expanded, a.Args[commandIndex+1:], command.Value, namedConfigs, phases, []string{}, false)
	if err != nil {
		return nil, err
	}
	log.Debugf("Fully expanded args: %s", shlex.Quote(arguments.FormatAll(expanded)...))

	// Append to new OrderedArgs to make sure `--` is handled correctly.
	expandedArgs := &OrderedArgs{}
	if err := expandedArgs.Append(expanded...); err != nil {
		return nil, err
	}
	return expandedArgs, err
}

// Expands and appends all applicable args from the provided Config to the
// provided argument slice and returns it.
func (c *Config) appendArgsForConfig(
	expanded []arguments.Argument,
	namedConfigs map[string]*Config,
	phases []string,
	configStack []string,
	allowEmpty bool,
) ([]arguments.Argument, error) {
	empty := true
	for _, phase := range phases {
		toExpand, ok := c.ByPhase[phase]
		if !ok {
			continue
		}
		empty = false
		var err error
		expanded, err = appendExpansion(
			expanded,
			toExpand,
			phase,
			namedConfigs,
			phases,
			configStack,
			true,
		)
		if err != nil {
			return nil, err
		}
		log.Debugf("Expanded for phase %s: %#v", phase, arguments.FormatAll(expanded))
	}
	if empty && !allowEmpty {
		// empty config names do not require supported phases
		return nil, fmt.Errorf("config value not defined in any .rc file")
	}
	return expanded, nil
}

// appendExpansion expands and appends all args in `toExpand` to `expanded`.
func appendExpansion(
	expanded []arguments.Argument,
	toExpand []arguments.Argument,
	phase string,
	namedConfigs map[string]*Config,
	phases []string,
	configStack []string,
	removeDoubleDash bool,
) ([]arguments.Argument, error) {
	for _, a := range toExpand {
		log.Debugf("Expanding %s", shlex.Quote(a.Format()...))
		switch a := a.(type) {
		case *arguments.DoubleDash:
			if removeDoubleDash {
				continue
			}
			expanded = append(expanded, &arguments.DoubleDash{})
		case options.Option:
			// For the unconditional phases, only append the arg if it's supported by
			// the command.
			if bazelrc.IsUnconditionalCommandPhase(phase) && !a.Supports(phases[len(phases)-1]) {
				if phase == bazelrc.AlwaysPhase {
					log.Warnf("Inherited '%s' options: %v", bazelrc.AlwaysPhase, arguments.FormatAll(toExpand))
					return nil, fmt.Errorf("%[1]s :: Unrecognized option %[1]s", a.Format()[0])
				}
				// TODO(zoey): return an error here if the option does not support any
				// command; unknown options are disallowed in rc files.
				log.Debugf("common rc rule: opt %q is unsupported by command %q; skipping", a.Name(), phases[len(phases)-1])
				continue
			}
			if a.Name() != "config" {
				expanded = append(expanded, a)
				continue
			}
			// This is a config option; expand it.
			if _, ok := a.(*options.RequiredValueOption); !ok {
				return nil, fmt.Errorf("config options must be of '*RequiredValueOption', but was of type '%T'.", a)
			}
			config, ok := namedConfigs[a.GetValue()]
			if !ok {
				return nil, fmt.Errorf("config value '%s' is not defined in any .rc file", a.GetValue())
			}
			if slices.Index(configStack, a.GetValue()) != -1 {
				return nil, fmt.Errorf("circular --config reference detected: %s", strings.Join(append(configStack, a.GetValue()), " -> "))
			}
			var err error
			if expanded, err = config.appendArgsForConfig(expanded, namedConfigs, phases, append(configStack, a.GetValue()), false); err != nil {
				return nil, fmt.Errorf("error expanding config '%s': %s", a.GetValue(), err)
			}
		case *arguments.PositionalArgument:
			expanded = append(expanded, a)
		}
	}
	return expanded, nil
}

func Partition(args []arguments.Argument) *PartitionedArgs {
	partitioned := &PartitionedArgs{}
	for _, c := range Classify(args) {
		switch c := c.(type) {
		case *StartupOption:
			partitioned.StartupOptions = append(partitioned.StartupOptions, c.Option)
		case *Command:
			partitioned.Command = &c.Value
		case *CommandOption:
			partitioned.CommandOptions = append(partitioned.CommandOptions, c.Option)
		case *Target:
			partitioned.Targets = append(partitioned.Targets, c.PositionalArgument)
		case *ExecArg:
			partitioned.ExecArgs = append(partitioned.ExecArgs, c.PositionalArgument)
		}
	}
	return partitioned
}

func (a *PartitionedArgs) Format() []string {
	formatted := arguments.FormatAll(a.StartupOptions)
	if a.Command != nil {
		formatted = append(formatted, *a.Command)
	}
	formatted = append(formatted, arguments.FormatAll(a.CommandOptions)...)
	for _, arg := range a.Targets {
		if strings.HasPrefix(arg.Value, "-") {
			formatted = append(formatted, "--")
			formatted = append(formatted, arguments.FormatAll(a.Targets)...)
			formatted = append(formatted, arguments.FormatAll(a.ExecArgs)...)
			return formatted
		}
	}
	formatted = append(formatted, arguments.FormatAll(a.Targets)...)
	if len(a.ExecArgs) != 0 {
		formatted = append(formatted, "--")
		formatted = append(formatted, arguments.FormatAll(a.ExecArgs)...)
	}
	return formatted
}

func (a *PartitionedArgs) Canonicalized() Args {
	return &PartitionedArgs{
		StartupOptions: options.Canonicalize(a.StartupOptions),
		Command:        a.Command,
		CommandOptions: options.Canonicalize(a.CommandOptions),
		Targets:        slices.Clone(a.Targets),
		ExecArgs:       slices.Clone(a.ExecArgs),
	}
}

func (a *PartitionedArgs) GetStartupOptions() []options.Option {
	return a.StartupOptions
}

func (a *PartitionedArgs) GetCommand() string {
	if a.Command == nil {
		return "help"
	}
	return *a.Command
}

func (a *PartitionedArgs) GetCommandOptions() []options.Option {
	return a.CommandOptions
}

func (a *PartitionedArgs) GetTargets() []*arguments.PositionalArgument {
	return a.Targets
}

func (a *PartitionedArgs) GetExecArgs() []*arguments.PositionalArgument {
	return a.ExecArgs
}

func (a *PartitionedArgs) GetStartupOptionsByName(optionName string) []*IndexedOption {
	var matchedOptions []*IndexedOption
	for i, o := range a.StartupOptions {
		if o.Name() == optionName {
			matchedOptions = append(matchedOptions, &IndexedOption{Option: o, Index: i})
		}
	}
	return matchedOptions
}

func (a *PartitionedArgs) GetCommandOptionsByName(optionName string) []*IndexedOption {
	var matchedOptions []*IndexedOption
	for i, o := range a.CommandOptions {
		if o.Name() == optionName {
			matchedOptions = append(matchedOptions, &IndexedOption{Option: o, Index: i + len(a.StartupOptions) + 1})
		}
	}
	return matchedOptions
}

func (a *PartitionedArgs) RemoveStartupOptions(optionNames ...string) []*IndexedOption {
	toRemove := set.From(optionNames...)
	var removed []*IndexedOption
	for i := 0; i < len(a.StartupOptions); {
		o := a.StartupOptions[i]
		if _, ok := toRemove[o.Name()]; ok {
			a.StartupOptions = slices.Delete(a.StartupOptions, i, i+1)
			removed = append(removed, &IndexedOption{Option: o, Index: i})
			// account for the fact that the next element is now at index i instead
			// of i+1
			i--
		}
	}
	return removed
}

func (a *PartitionedArgs) RemoveCommandOptions(optionNames ...string) []*IndexedOption {
	toRemove := set.From(optionNames...)
	var removed []*IndexedOption
	for i := 0; i < len(a.CommandOptions); {
		o := a.CommandOptions[i]
		if _, ok := toRemove[o.Name()]; ok {
			a.CommandOptions = slices.Delete(a.CommandOptions, i, i+1)
			removed = append(removed, &IndexedOption{Option: o, Index: i + len(a.StartupOptions) + 1})
			// account for the fact that the next element is now at index i instead
			// of i+1
			i--
		}
	}
	return removed
}

func (a *PartitionedArgs) Append(args ...arguments.Argument) error {
	for _, arg := range args {
		switch arg := arg.(type) {
		case *arguments.DoubleDash:
			// skip
		case *arguments.PositionalArgument:
			switch {
			case a.Command == nil:
				a.Command = &arg.Value
			case *a.Command == "run" && len(a.Targets) > 0:
				a.ExecArgs = append(a.ExecArgs, arg)
			default:
				a.Targets = append(a.Targets, arg)
			}
		case options.Option:
			if arg.PluginID() == options.UnknownBuiltinPluginID && !arg.HasSupportedCommands() {
				// If this is an unknown option with no listed supported commands, assume it
				// supports this command (or "startup" in the rare case that no command was
				// provided.
				command := "startup"
				if a.Command != nil {
					command = *a.Command
				}
				arg.GetDefinition().AddSupportedCommand(command)
			}
			switch {
			case arg.Supports("startup"):
				a.StartupOptions = append(a.StartupOptions, arg)
			case a.Command == nil:
				return fmt.Errorf("Failed to append Option: option '%s' is not a startup option and no command was provided.", arg.Name())
			case arg.Supports(*a.Command):
				a.CommandOptions = append(a.CommandOptions, arg)
			default:
				return fmt.Errorf("Failed to append Option: option '%s' is not a startup option and the command '%s' does not support it.", arg.Name(), *a.Command)

			}
		}
	}
	return nil
}

func (a *PartitionedArgs) SplitExecutableArgs() ([]string, []string) {
	bazelArgs := arguments.FormatAll(a.StartupOptions)
	if a.Command != nil {
		bazelArgs = append(bazelArgs, *a.Command)
	}
	bazelArgs = append(bazelArgs, arguments.FormatAll(a.CommandOptions)...)
	for _, arg := range a.Targets {
		if strings.HasPrefix(arg.Value, "-") {
			bazelArgs = append(bazelArgs, "--")
			bazelArgs = append(bazelArgs, arguments.FormatAll(a.Targets)...)
			return bazelArgs, arguments.FormatAll(a.ExecArgs)
		}
	}
	bazelArgs = append(bazelArgs, arguments.FormatAll(a.Targets)...)
	return bazelArgs, arguments.FormatAll(a.ExecArgs)
}
