// Package option_definitions defines the command-line option schemas for
// file watcher flags. It specifies flags like --watch/-w and --watcher_flags
// that control file watching behavior and configuration.
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
