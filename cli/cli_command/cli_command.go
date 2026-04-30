package cli_command

import (
	"flag"
	"slices"
	"strings"
)

type Command struct {
	Name    string
	Help    string
	Handler func(args []string) (exitCode int, err error)
	Aliases []string
	// RCSubcommands lists canonical subcommand path suffixes that may be used in
	// .bbrc files, for example "get" for "execution get".
	RCSubcommands []string
	// RCArgBoundary returns the index in args after which .bbrc expansion should
	// stop. This is useful for commands like `bb remote` where args after the
	// Bazel subcommand should be left untouched.
	RCArgBoundary func(args []string) int
	// Flags used for `bb help <command>`
	Flags *flag.FlagSet
}

var (
	// Commands is a slice of all known CLI commands, sorted by their Name fields.
	//
	// It is nil until Register in cli_command/register is called.
	Commands []*Command

	// CommandsByName is a map of every known CLI command, each indexed by its
	// Name field.
	//
	// It is nil until Register in cli_command/register is called.
	CommandsByName map[string]*Command

	// Aliases maps every known alias to its corresponding CLI command.
	//
	// It is nil until Register in cli_command/register is called.
	Aliases map[string]*Command
)

// GetCommand returns the Command corresponding to the provided command name or
// alias, or nil if no such Command exists.
func GetCommand(commandName string) *Command {
	if command, ok := CommandsByName[commandName]; ok {
		return command
	}
	if command, ok := Aliases[commandName]; ok {
		return command
	}
	return nil
}

// ResolveRCPath returns the canonical .bbrc path corresponding to this
// command invocation along with the number of args consumed from the beginning
// of args after the top-level command.
func (c *Command) ResolveRCPath(args []string) (string, int) {
	path := c.Name
	longestMatch := 0
	for _, subcommand := range c.RCSubcommands {
		tokens := strings.Fields(subcommand)
		if len(tokens) == 0 || len(tokens) > len(args) {
			continue
		}
		if !slices.Equal(tokens, args[:len(tokens)]) {
			continue
		}
		if len(tokens) > longestMatch {
			longestMatch = len(tokens)
			path = c.Name + " " + subcommand
		}
	}
	return path, longestMatch
}

func (c *Command) RCBoundary(args []string) int {
	if c.RCArgBoundary == nil {
		return len(args)
	}
	return c.RCArgBoundary(args)
}

// AllRCPaths returns all canonical command paths supported in .bbrc files.
func AllRCPaths() []string {
	paths := make([]string, 0, len(Commands))
	for _, command := range Commands {
		paths = append(paths, command.Name)
		for _, subcommand := range command.RCSubcommands {
			paths = append(paths, command.Name+" "+subcommand)
		}
	}
	return paths
}
