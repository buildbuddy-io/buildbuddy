package register

import (
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/add"
	"github.com/buildbuddy-io/buildbuddy/cli/analyze"
	"github.com/buildbuddy-io/buildbuddy/cli/ask"
	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/cli/download"
	"github.com/buildbuddy-io/buildbuddy/cli/execute"
	"github.com/buildbuddy-io/buildbuddy/cli/explain"
	"github.com/buildbuddy-io/buildbuddy/cli/fix"
	"github.com/buildbuddy-io/buildbuddy/cli/index"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/printlog"
	"github.com/buildbuddy-io/buildbuddy/cli/record"
	"github.com/buildbuddy-io/buildbuddy/cli/remote_download"
	"github.com/buildbuddy-io/buildbuddy/cli/remotebazel"
	"github.com/buildbuddy-io/buildbuddy/cli/search"
	"github.com/buildbuddy-io/buildbuddy/cli/update"
	"github.com/buildbuddy-io/buildbuddy/cli/upload"
	"github.com/buildbuddy-io/buildbuddy/cli/versioncmd"
	"github.com/buildbuddy-io/buildbuddy/cli/view"
)

// Register registers all known cli commands in the structures laid out in
// cli/cli_command. It is meant to be called immediately on CLI
// startup.
//
// This indirection prevents dependency cycles from occurring when, for example,
// an imported package tries to use the parser, which itself needs to know all
// of the cli commands.
var Register = sync.OnceFunc(register)

func register() {
	cli_command.Commands = []*cli_command.Command{
		{
			Name:    "add",
			Help:    "Adds a dependency to your WORKSPACE file.",
			Handler: add.HandleAdd,
		},
		{
			Name:    "analyze",
			Help:    "Analyzes the dependency graph.",
			Handler: analyze.HandleAnalyze,
		},
		{
			Name:    "ask",
			Help:    "Asks for suggestions about your last invocation.",
			Handler: ask.HandleAsk,
			Aliases: []string{"wtf", "huh"},
		},
		{
			Name:    "download",
			Help:    "Downloads artifacts from a remote cache.",
			Handler: download.HandleDownload,
		},
		{
			Name:    "execute",
			Help:    "Executes arbitrary commands using remote execution.",
			Handler: execute.HandleExecute,
		},
		{
			Name:    "fix",
			Help:    "Applies fixes to WORKSPACE and BUILD files.",
			Handler: fix.HandleFix,
		},
		// Handle 'help' command separately to avoid circular dependency with `cli_command`
		// package
		{
			Name:    "install",
			Help:    "Installs a bb plugin (https://buildbuddy.io/plugins).",
			Handler: plugin.HandleInstall,
		},
		{
			Name:    "login",
			Help:    "Configures bb commands to use your BuildBuddy API key.",
			Handler: login.HandleLogin,
		},
		{
			Name:    "logout",
			Help:    "Configures bb commands to no longer use your saved API key.",
			Handler: login.HandleLogout,
		},
		{
			Name:    "print",
			Help:    "Displays various log file types written by bazel.",
			Handler: printlog.HandlePrint,
		},
		{
			Name:    "record",
			Help:    "Records command output and streams it to BuildBuddy.",
			Handler: record.HandleRecord,
		},
		{
			Name:    "remote",
			Help:    "Runs a bazel command in the cloud with BuildBuddy's hosted bazel service.",
			Handler: remotebazel.HandleRemoteBazel,
		},
		{
			Name:    "remote-download",
			Help:    "Fetches a remote asset via an intermediate cache.",
			Handler: remote_download.HandleRemoteDownload,
		},
		{
			Name:    "search",
			Help:    "Searches for code in the remote codesearch index.",
			Handler: search.HandleSearch,
		},
		{
			Name:    "index",
			Help:    "Sends updates to the remote codesearch index.",
			Handler: index.HandleIndex,
		},
		{
			Name:    "update",
			Help:    "Updates the bb CLI to the latest version.",
			Handler: update.HandleUpdate,
		},
		{
			Name:    "upload",
			Help:    "Uploads files to the remote cache.",
			Handler: upload.HandleUpload,
		},
		{
			Name:    "version",
			Help:    "Prints bb cli version info.",
			Handler: versioncmd.HandleVersion,
		},
		{
			Name:    "view",
			Help:    "Views build logs from BuildBuddy.",
			Handler: view.HandleView,
		},
		{
			Name:    "explain",
			Help:    "Explains the difference between two compact execution logs.",
			Handler: explain.HandleExplain,
		},
	}
	cli_command.CommandsByName = make(
		map[string]*cli_command.Command,
		len(cli_command.Commands),
	)
	cli_command.Aliases = make(map[string]*cli_command.Command)
	for _, command := range cli_command.Commands {
		cli_command.CommandsByName[command.Name] = command
		for _, alias := range command.Aliases {
			cli_command.Aliases[alias] = command
		}
	}
}
