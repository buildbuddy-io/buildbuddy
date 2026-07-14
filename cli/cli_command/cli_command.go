package cli_command

import "flag"

type Command struct {
	Name    string
	Help    string
	Handler func(args []string) (exitCode int, err error)
	Aliases []string
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

	// BBFlagSetsByCommand contains BB-specific flags.
	// Unlike CommandsByName, it may contain Bazel commands such as "run" if they have BB-specific flags.
	BBFlagSetsByCommand = map[string][]*flag.FlagSet{}
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

// RegisterBBFlagSet adds BB-specific flags to a command. The command may be a
// BB command or a Bazel command.
func RegisterBBFlagSet(commandName string, flags *flag.FlagSet) {
	BBFlagSetsByCommand[commandName] = append(BBFlagSetsByCommand[commandName], flags)
}

// GetBBFlagSets returns the BB-specific flag sets registered for a command.
func GetBBFlagSets(commandName string) []*flag.FlagSet {
	return BBFlagSetsByCommand[commandName]
}
