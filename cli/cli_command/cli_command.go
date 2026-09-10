package cli_command

import "flag"

type Command struct {
	Name    string
	Help    string
	Aliases []string
	// Handler runs the command.
	//
	// It is nil until Register in cli_command/register is called.
	Handler func(args []string) (exitCode int, err error)
	// Flags used for `bb help <command>`.
	//
	// It is nil until Register in cli_command/register is called, and remains
	// nil for commands that declare no flags.
	Flags *flag.FlagSet
}

// Commands is a slice of all known CLI commands.
//
// Handlers and flags are attached to these commands by Register in
// cli_command/register.
var Commands = []*Command{
	{
		Name: "add",
		Help: "Adds a dependency to your WORKSPACE file.",
	},
	{
		Name: "agent",
		Help: "Runs an AI coding agent to analyze data.",
	},
	{
		Name: "analyze",
		Help: "Analyzes the dependency graph.",
	},
	{
		Name:    "ask",
		Help:    "Asks for suggestions about your last invocation.",
		Aliases: []string{"wtf", "huh"},
	},
	{
		Name: "box",
		Help: "Starts a remote Firecracker VM box and opens a session in it.",
	},
	{
		Name: "detect",
		Help: "Detects issues in the current workspace.",
	},
	{
		Name: "download",
		Help: "Downloads artifacts from a remote cache.",
	},
	{
		Name: "execute",
		Help: "Executes arbitrary commands using remote execution.",
	},
	{
		Name: "execution",
		Help: "Remote execution tools",
	},
	{
		Name: "explain",
		Help: "Explains your build using compact execution logs.",
	},
	{
		Name: "fix",
		Help: "Applies fixes to WORKSPACE and BUILD files.",
	},
	// Handle 'help' command separately. It operates on parsed args rather than raw
	// ones, so it does not fit the Handler signature and is dispatched by
	// cli/cmd/bb before the args are resolved (see cli/help).
	{
		Name: "install",
		Help: "Installs a bb plugin (https://buildbuddy.io/plugins).",
	},
	{
		Name: "login",
		Help: "Configures bb commands to use your BuildBuddy API key.",
	},
	{
		Name: "logout",
		Help: "Configures bb commands to no longer use your saved API key.",
	},
	{
		Name: "print",
		Help: "Displays various log file types written by bazel.",
	},
	{
		Name: "record",
		Help: "Records command output and streams it to BuildBuddy.",
	},
	{
		Name: "remote",
		Help: "Runs a bazel command in the cloud with BuildBuddy's hosted bazel service.",
	},
	{
		Name: "remote-download",
		Help: "Fetches a remote asset via an intermediate cache.",
	},
	{
		Name: "search",
		Help: "Searches for code in the remote codesearch index.",
	},
	{
		Name: "ssh",
		Help: "Runs an SSH client on a user-mode wireguard network.",
	},
	{
		Name: "ssh-server",
		Help: "Runs an SSH server on a user-mode wireguard network.",
	},
	{
		Name: "index",
		Help: "Sends updates to the remote codesearch index.",
	},
	{
		Name: "ui",
		Help: "Opens an interactive terminal UI for viewing builds.",
	},
	{
		Name: "update",
		Help: "Updates the bb CLI to the latest version.",
	},
	{
		Name: "upload",
		Help: "Uploads files to the remote cache.",
	},
	{
		Name: "version",
		Help: "Prints bb cli version info.",
	},
	{
		Name: "view",
		Help: "Views build logs from BuildBuddy.",
	},
}

var (
	// CommandsByName is a map of every known CLI command, each indexed by its
	// Name field.
	CommandsByName = make(map[string]*Command, len(Commands))

	// Aliases maps every known alias to its corresponding CLI command.
	Aliases = map[string]*Command{}
)

func init() {
	for _, command := range Commands {
		CommandsByName[command.Name] = command
		for _, alias := range command.Aliases {
			Aliases[alias] = command
		}
	}
}

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

// IsCommand returns whether name is recognized as a bb CLI command name or
// alias.
func IsCommand(name string) bool {
	return GetCommand(name) != nil
}
