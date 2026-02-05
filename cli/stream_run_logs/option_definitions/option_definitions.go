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
