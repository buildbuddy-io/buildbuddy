// Package option_definitions defines the command-line option schemas for
// run log streaming flags. It specifies flags like --stream_run_logs and
// --on_stream_run_logs_failure that control whether and how run command logs
// are streamed to BuildBuddy.
package option_definitions

import "github.com/buildbuddy-io/buildbuddy/cli/parser/options"

var (
	StreamRunLogs = options.NewDefinition(
		"stream_run_logs",
		options.WithNegative(),
		options.WithPluginID(options.NativeBuiltinPluginID),
		options.WithSupportFor("run"),
	)
	OnStreamRunLogsFailure = options.NewDefinition(
		"on_stream_run_logs_failure",
		options.WithRequiresValue(),
		options.WithPluginID(options.NativeBuiltinPluginID),
		options.WithSupportFor("run"),
	)
)
