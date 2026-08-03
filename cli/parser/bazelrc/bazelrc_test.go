package bazelrc_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/bazelrc"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
	"github.com/stretchr/testify/require"
)

func mustNewOption(t *testing.T, name string, definition *options.Definition) options.Option {
	option, err := options.NewOption(name, nil, definition)
	require.NoError(t, err)
	return option
}

func TestExpandConfigs_ExpandsPlatformSpecificConfig(t *testing.T) {
	enablePlatformConfig := options.NewDefinition(
		bazelrc.EnablePlatformSpecificConfigFlag,
		options.WithNegative(),
		options.WithSupportFor("build"),
	)
	platformOption := options.NewDefinition(
		"platform_option",
		options.WithNegative(),
		options.WithSupportFor("build"),
	)
	args := &parsed.OrderedArgs{Args: []arguments.Argument{
		&arguments.PositionalArgument{Value: "build"},
		mustNewOption(t, bazelrc.EnablePlatformSpecificConfigFlag, enablePlatformConfig),
	}}
	defaultConfig := parsed.NewConfig()
	namedConfigs := map[string]*parsed.Config{
		bazelrc.GetBazelOS(): {
			ByPhase: map[string][]arguments.Argument{
				"build": {mustNewOption(t, "platform_option", platformOption)},
			},
		},
	}

	expanded, err := bazelrc.ExpandConfigs(args, namedConfigs, defaultConfig)
	require.NoError(t, err)
	require.Equal(t, []string{"build", "--platform_option"}, expanded.Format())
}
