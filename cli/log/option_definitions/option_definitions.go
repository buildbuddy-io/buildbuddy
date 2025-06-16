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
