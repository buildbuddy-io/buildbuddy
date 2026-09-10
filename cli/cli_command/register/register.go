package register

import (
	"flag"
	"fmt"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/cli/add"
	"github.com/buildbuddy-io/buildbuddy/cli/agent"
	"github.com/buildbuddy-io/buildbuddy/cli/agent/agentflags"
	"github.com/buildbuddy-io/buildbuddy/cli/analyze"
	"github.com/buildbuddy-io/buildbuddy/cli/ask"
	"github.com/buildbuddy-io/buildbuddy/cli/box"
	"github.com/buildbuddy-io/buildbuddy/cli/cli_command"
	"github.com/buildbuddy-io/buildbuddy/cli/detect"
	"github.com/buildbuddy-io/buildbuddy/cli/download"
	"github.com/buildbuddy-io/buildbuddy/cli/execute"
	"github.com/buildbuddy-io/buildbuddy/cli/execution"
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
	"github.com/buildbuddy-io/buildbuddy/cli/ssh"
	"github.com/buildbuddy-io/buildbuddy/cli/ssh_server"
	"github.com/buildbuddy-io/buildbuddy/cli/ui"
	"github.com/buildbuddy-io/buildbuddy/cli/update"
	"github.com/buildbuddy-io/buildbuddy/cli/upload"
	"github.com/buildbuddy-io/buildbuddy/cli/versioncmd"
	"github.com/buildbuddy-io/buildbuddy/cli/view"
)

// implementation holds the parts of a cli command that require importing its
// handler package. The commands themselves (names, help text and aliases) are
// declared in cli_command.Commands, which stays dependency-free so that the
// list of cli command names can be consulted without pulling in every handler.
type implementation struct {
	handler func(args []string) (exitCode int, err error)
	flags   *flag.FlagSet
}

// implementationsByCommandName must contain exactly one entry per command in
// cli_command.Commands; register panics otherwise.
var implementationsByCommandName = map[string]implementation{
	"add":             {handler: add.HandleAdd, flags: add.Flags},
	"agent":           {handler: agent.HandleAgent, flags: agentflags.SharedAgentFlags},
	"analyze":         {handler: analyze.HandleAnalyze, flags: analyze.Flags},
	"ask":             {handler: ask.HandleAsk, flags: ask.Flags},
	"box":             {handler: box.HandleBox, flags: box.Flags},
	"detect":          {handler: detect.HandleDetect, flags: detect.Flags},
	"download":        {handler: download.HandleDownload, flags: download.Flags},
	"execute":         {handler: execute.HandleExecute, flags: execute.Flags},
	"execution":       {handler: execution.HandleExecution, flags: execution.Flags},
	"explain":         {handler: explain.HandleExplain, flags: explain.Flags},
	"fix":             {handler: fix.HandleFix, flags: fix.Flags},
	"index":           {handler: index.HandleIndex, flags: index.Flags},
	"install":         {handler: plugin.HandleInstall, flags: plugin.Flags},
	"login":           {handler: login.HandleLogin, flags: login.Flags},
	"logout":          {handler: login.HandleLogout},
	"print":           {handler: printlog.HandlePrint, flags: printlog.Flags},
	"record":          {handler: record.HandleRecord, flags: record.Flags},
	"remote":          {handler: remotebazel.HandleRemoteBazel, flags: remotebazel.RemoteFlagset},
	"remote-download": {handler: remote_download.HandleRemoteDownload, flags: remote_download.Flags},
	"search":          {handler: search.HandleSearch, flags: search.Flags},
	"ssh":             {handler: ssh.HandleSSH, flags: ssh.Flags},
	"ssh-server":      {handler: ssh_server.HandleSSHServer, flags: ssh_server.Flags},
	"ui":              {handler: ui.HandleUI, flags: ui.Flags},
	"update":          {handler: update.HandleUpdate, flags: update.Flags},
	"upload":          {handler: upload.HandleUpload, flags: upload.Flags},
	"version":         {handler: versioncmd.HandleVersion},
	"view":            {handler: view.HandleView, flags: view.Flags},
}

// Register attaches the handler and flags of every command declared in
// cli_command.Commands. It is meant to be called immediately on CLI startup.
//
// This indirection prevents dependency cycles from occurring when, for example,
// an imported package tries to use the parser, which itself needs to know all
// of the cli commands.
var Register = sync.OnceFunc(register)

func register() {
	for _, command := range cli_command.Commands {
		impl, ok := implementationsByCommandName[command.Name]
		if !ok {
			panic(fmt.Sprintf("cli command %q has no registered handler", command.Name))
		}
		command.Handler = impl.handler
		command.Flags = impl.flags
	}
	for name := range implementationsByCommandName {
		if _, ok := cli_command.CommandsByName[name]; !ok {
			panic(fmt.Sprintf("handler registered for unknown cli command %q", name))
		}
	}
}
