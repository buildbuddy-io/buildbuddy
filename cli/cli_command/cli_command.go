package cli_command

type Command struct {
	Name    string
	Help    string
	Handler func(args []string) (exitCode int, err error)
	Aliases []string
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
