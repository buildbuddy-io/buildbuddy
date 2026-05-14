package option_definitions

import (
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
)

var (
	Watch = options.NewDefinition(
		"watch",
		options.WithShortName("w"),
		options.WithNegative(),
		options.WithPluginID(options.NativeBuiltinPluginID),
		options.WithSupportFor("startup"),
	)

	WatcherFlags = options.NewDefinition(
		"watcher_flags",
		options.WithRequiresValue(),
		options.WithMulti(),
		options.WithPluginID(options.NativeBuiltinPluginID),
		options.WithSupportFor("startup"),
	)
)
