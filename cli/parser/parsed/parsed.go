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
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
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

	// GetTargets returns a slice of all the bazel tagets Args contains.
	GetTargets() []*arguments.PositionalArgument

	// GetExecArgs returns a slice of all the arguments bazel will forward to the
	// executable it will run. Executable args only ever exist in the presence of
	// a "run" command.
	GetExecArgs() []*arguments.PositionalArgument

	// GetOptionsByName returns a slice of all options whose definitions have
	// names that match optionName in the order in which they appear in Args.
	// The accompanying index is the index at which they are located in Args.
	GetOptionsByName(optionName string) []*IndexedOption

	// RemoveOption removes all options whose definitions have names that match
	// optionName and returns a slice of all removed options, if any, in the order
	// in which they appeared in the args. The accompanying index is the index at
	// which they were removed in Args.
	RemoveOptions(...string) []*IndexedOption

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
	arg() arguments.Argument
}

// Interface for argument classifications to implement if they describe options
type IsOption interface {
	AsOption() options.Option
}

// Argument classifications

type UnsupportedArgument struct{ arguments.Argument }

func (u *UnsupportedArgument) arg() arguments.Argument { return u.Argument }

type StartupOption struct{ options.Option }

func (s *StartupOption) arg() arguments.Argument  { return s.Option }
func (s *StartupOption) AsOption() options.Option { return s.Option }

type Command struct{ *arguments.PositionalArgument }

func (c *Command) arg() arguments.Argument { return c.PositionalArgument }

type CommandOption struct{ options.Option }

func (c *CommandOption) arg() arguments.Argument  { return c.Option }
func (c *CommandOption) AsOption() options.Option { return c.Option }

type DoubleDash struct{ *arguments.PositionalArgument }

func (d *DoubleDash) arg() arguments.Argument { return d.PositionalArgument }

type Target struct{ *arguments.PositionalArgument }

func (t *Target) arg() arguments.Argument { return t.PositionalArgument }

type ExecArg struct{ *arguments.PositionalArgument }

func (e *ExecArg) arg() arguments.Argument { return e.PositionalArgument }

// classifier tracks relevant context while iterating over a slice of
// Arguments in command-line order. It is primarily intended for use
// by the `Classify` and `Find` functions.
type classifier struct {
	command          *string
	foundDoubleDash  bool
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
	case *DoubleDash:
		c.foundDoubleDash = true
	case *Target:
		c.foundFirstTarget = true
	}
}

func (c *classifier) Classify(arg arguments.Argument) Classified {
	switch arg := arg.(type) {
	case *arguments.PositionalArgument:
		switch {
		case c.command == nil:
			return &Command{arg}
		case !c.foundDoubleDash && arg.Value == "--":
			return &DoubleDash{arg}
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
			bazelArgs = append(bazelArgs, c.arg().Format()...)
		}
	}
	return bazelArgs, executableArgs
}

// IndexedOption is used by Args.GetOptionsByName and Args.RemoveOptions to
// couple options with their associated indices when returning them.
type IndexedOption struct {
	options.Option
	Index int
}

func (a *OrderedArgs) GetOptionsByName(optionName string) []*IndexedOption {
	var matchedOptions []*IndexedOption
	for i, c := range Classify(a.Args) {
		if c, ok := c.(IsOption); ok && c.AsOption().Name() == optionName {
			matchedOptions = append(matchedOptions, &IndexedOption{Option: c.AsOption(), Index: i})
		}
	}
	return matchedOptions
}

func (a *OrderedArgs) RemoveOptions(optionNames ...string) []*IndexedOption {
	toRemove := make(map[string]struct{}, len(optionNames))
	for _, n := range optionNames {
		toRemove[n] = struct{}{}
	}
	var removed []*IndexedOption
	for i := 0; i < len(a.Args); i++ {
		if o, ok := a.Args[i].(options.Option); ok {
			if _, ok := toRemove[o.Name()]; ok {
				a.Args = slices.Delete(a.Args, i, i+1)
				removed = append(removed, &IndexedOption{Option: o, Index: i})
				// account for the fact that the next element is now at index i instead
				// of i+1
				i--
			}
		}
	}
	return removed
}

func (a *OrderedArgs) Append(args ...arguments.Argument) error {
	startupOptionInsertIndex := -1
	commandOptionInsertIndex := -1
	for _, arg := range args {
		switch arg := arg.(type) {
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
		if commandOptionInsertIndex, _ = Find[*Command](a.Args[startupOptionInsertIndex:]); commandOptionInsertIndex == -1 {
			commandOptionInsertIndex = len(a.Args)
		}
	}

	if commandOptionInsertIndex == len(a.Args) {
		if strings.HasPrefix(arg.GetValue(), "-") {
			a.Args = append(a.Args, &arguments.PositionalArgument{Value: "--"})
		} else {
			// If there's no double dash, commandOptionInsertIndex needs to be
			// incremented to still be len(p.Args) afer we append the argument for
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
			if commandOptionInsertIndex, _ = Find[*Command](a.Args[startupOptionInsertIndex:]); commandOptionInsertIndex == -1 {
				commandOptionInsertIndex = len(a.Args)
			}
		}
		a.Args = slices.Insert(a.Args, commandOptionInsertIndex, arguments.Argument(option))
		commandOptionInsertIndex += 1
		return startupOptionInsertIndex, commandOptionInsertIndex, nil
	}
	return startupOptionInsertIndex, commandOptionInsertIndex, fmt.Errorf("Failed to append Option: option '%s' is not a startup option and the command '%s' does not support it.", option.Name(), command)
}

// Offset exists as a temporary hack while we continue to move away from
// passing arguments around as strings. It returns the offset in the
// string-based arg slice where the argument at this index is located.
func (a *OrderedArgs) Offset(index int) int {
	offset := 0
	for _, arg := range a.Args[:index] {
		offset += len(arg.Format())
	}
	return offset
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

func (a *PartitionedArgs) GetOptionsByName(optionName string) []*IndexedOption {
	var matchedOptions []*IndexedOption
	for i, o := range a.StartupOptions {
		if o.Name() == optionName {
			matchedOptions = append(matchedOptions, &IndexedOption{Option: o, Index: i})
		}
	}
	for i, o := range a.CommandOptions {
		if o.Name() == optionName {
			matchedOptions = append(matchedOptions, &IndexedOption{Option: o, Index: i + len(a.StartupOptions) + 1})
		}
	}
	return matchedOptions
}

func (a *PartitionedArgs) RemoveOptions(optionNames ...string) []*IndexedOption {
	toRemove := make(map[string]struct{}, len(optionNames))
	for _, n := range optionNames {
		toRemove[n] = struct{}{}
	}
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
