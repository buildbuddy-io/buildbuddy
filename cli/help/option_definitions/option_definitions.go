// Package option_definitions defines the command-line option schemas for
// help-related flags. It specifies the structure, aliases, and behavior of
// flags like --help/-h that control help system functionality across the CLI.
package option_definitions

import (
	"slices"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
)

var (
	// Help defines the option `--help` and the abbreviation `-h` to support
	// command-line invocations like `bb --help build`
	Help = options.NewDefinition(
		"help",
		options.WithShortName("h"),
		options.WithNegative(),
		options.WithPluginID(options.NativeBuiltinPluginID),
		options.WithSupportFor("startup"),
		options.WithSupportFor(slices.Collect(bazelrc.BazelCommands().All())...),
	)
)
