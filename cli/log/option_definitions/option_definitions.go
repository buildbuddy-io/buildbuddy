// Package option_definitions defines the command-line option schemas for
// logging-related flags. It specifies the structure and behavior of flags like
// --verbose/-v that control logging verbosity across the CLI.
package option_definitions

import (
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
)

var Verbose = options.NewDefinition(
	"verbose",
	options.WithShortName("v"),
	options.WithNegative(),
	options.WithPluginID(options.NativeBuiltinPluginID),
	options.WithSupportFor("startup"),
)
