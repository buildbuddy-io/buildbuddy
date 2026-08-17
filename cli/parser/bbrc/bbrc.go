package bbrc

import (
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
)

const (
	ConfigFlagName           = "bb_config"
	FileFlagName             = "bbrc"
	IgnoreAllRCFilesFlagName = "ignore_all_bb_rc_files"
	FileName                 = ".bbrc"
)

// NewIgnoreAllRCFilesOptionDefinition returns the
// --ignore_all_bb_rc_files startup option definition.
func NewIgnoreAllRCFilesOptionDefinition() *options.Definition {
	return options.NewDefinition(
		IgnoreAllRCFilesFlagName,
		options.WithNegative(),
		options.WithSupportFor("startup"),
	)
}

// NewFileOptionDefinition returns the repeatable --bbrc startup option
// definition.
func NewFileOptionDefinition() *options.Definition {
	return options.NewDefinition(
		FileFlagName,
		options.WithMulti(),
		options.WithRequiresValue(),
		options.WithSupportFor("startup"),
	)
}

// NewConfigOptionDefinition returns the --bb_config option definition.
// The flag is supported for the given commands.
func NewConfigOptionDefinition(commands ...string) *options.Definition {
	return options.NewDefinition(
		ConfigFlagName,
		options.WithMulti(),
		options.WithRequiresValue(),
		options.WithSupportFor(commands...),
	)
}

// NewConfigExpansionPolicy returns the policy used to expand --bb_config flags.
// .bbrc files follow Bazel's command hierarchy.
func NewConfigExpansionPolicy() *parsed.ConfigExpansionPolicy {
	return &parsed.ConfigExpansionPolicy{
		FlagName:  ConfigFlagName,
		GetPhases: bazelrc.GetPhases,
	}
}

// ExpandConfigs expands .bbrc --bb_config flags.
func ExpandConfigs(
	args *parsed.OrderedArgs,
	namedConfigs map[string]*parsed.Config,
	defaultConfig *parsed.Config,
) (*parsed.OrderedArgs, error) {
	return args.ExpandConfigsWithPolicy(namedConfigs, defaultConfig, NewConfigExpansionPolicy())
}

// RemoveOptions removes all options related to .bbrc files.
func RemoveOptions(args *parsed.OrderedArgs) {
	args.RemoveStartupOptions(FileFlagName, IgnoreAllRCFilesFlagName)
	args.RemoveCommandOptions(ConfigFlagName)
}
