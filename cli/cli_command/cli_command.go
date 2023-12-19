package cli_command

import (
	"github.com/buildbuddy-io/buildbuddy/cli/add"
	"github.com/buildbuddy-io/buildbuddy/cli/analyze"
	"github.com/buildbuddy-io/buildbuddy/cli/ask"
	"github.com/buildbuddy-io/buildbuddy/cli/common"
	"github.com/buildbuddy-io/buildbuddy/cli/download"
	"github.com/buildbuddy-io/buildbuddy/cli/execute"
	"github.com/buildbuddy-io/buildbuddy/cli/fix"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/plugin"
	"github.com/buildbuddy-io/buildbuddy/cli/printlog"
	"github.com/buildbuddy-io/buildbuddy/cli/update"
	"github.com/buildbuddy-io/buildbuddy/cli/upload"
	"github.com/buildbuddy-io/buildbuddy/cli/version"
)

type Command struct {
	Name    string
	Help    string
	Handler func(args []string) (exitCode int, err error)
	Aliases []string
}

var Commands = []Command{
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
		Name: "remote",
		Help: "Runs a bazel command in the cloud with BuildBuddy's hosted bazel service.",
		// Because `remote` shares some setup with running a regular bazel command
		// straight up (i.e. bb build), handle both together outside of the typical
		// cli command handlers.
		Handler: func(args []string) (exitCode int, err error) {
			return common.ForwardCommandToBazelExitCode, nil
		},
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
		Handler: version.HandleVersion,
	},
}
