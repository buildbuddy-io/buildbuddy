package parsed_test

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/parsed"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustNewOption(t *testing.T, optName string, v *string, d *options.Definition) options.Option {
	opt, err := options.NewOption(optName, v, d)
	require.NoError(t, err)
	return opt
}

func ptr[T any](v T) *T {
	return &v
}

func TestExpandConfigsWithPolicy(t *testing.T) {
	// The explain command supports the following options:
	// - bb_config
	// - config
	// - common_flag
	// - detailed_flag
	bbConfigDefinition := options.NewDefinition(
		"bb_config",
		options.WithRequiresValue(),
		options.WithSupportFor("explain"),
	)
	bazelConfigDefinition := options.NewDefinition(
		"config",
		options.WithRequiresValue(),
		options.WithSupportFor("explain"),
	)
	commonDefinition := options.NewDefinition(
		"common_flag",
		options.WithNegative(),
		options.WithSupportFor("explain"),
	)
	detailedDefinition := options.NewDefinition(
		"detailed_flag",
		options.WithNegative(),
		options.WithSupportFor("explain"),
	)

	// Args: explain --config=untouched --bb_config=detailed invocation-id
	args := &parsed.OrderedArgs{Args: []arguments.Argument{
		&arguments.PositionalArgument{Value: "explain"},
		mustNewOption(t, "config", ptr("untouched"), bazelConfigDefinition),
		mustNewOption(t, "bb_config", ptr("detailed"), bbConfigDefinition),
		&arguments.PositionalArgument{Value: "invocation-id"},
	}}

	// Simulates a .rc line that looks like: `common --common_flag`
	defaultConfig := parsed.NewConfig()
	defaultConfig.ByPhase["common"] = []arguments.Argument{
		mustNewOption(t, "common_flag", nil, commonDefinition),
	}

	// Simulates a .rc line that looks like: `explain:detailed --detailed_flag`
	namedConfigs := map[string]*parsed.Config{
		"detailed": {
			ByPhase: map[string][]arguments.Argument{
				"explain": {mustNewOption(t, "detailed_flag", nil, detailedDefinition)},
			},
		},
	}

	configPolicy := &parsed.ConfigExpansionPolicy{
		FlagName: "bb_config",
		GetPhases: func(command string) []string {
			// Common flags should be applied first, then explain-specific flags.
			require.Equal(t, "explain", command)
			return []string{"common", "explain"}
		},
	}
	expanded, err := args.ExpandConfigsWithPolicy(
		namedConfigs,
		defaultConfig,
		configPolicy,
	)
	require.NoError(t, err)
	require.Equal(t, []string{
		"explain",
		// Common flags should've been applied first.
		"--common_flag",
		// We were only expanding bb_config, so the --config=untouched flag should be left alone.
		"--config=untouched",
		// The --bb_config=detailed flag should be expanded to --detailed_flag.
		"--detailed_flag",
		"invocation-id",
	}, expanded.Format())
}

func TestRemoveAndAccumulateStartupOption(t *testing.T) {
	startupOptionBoolName := "startup_option_bool"
	startupOptionBoolDefinition := options.NewDefinition(
		startupOptionBoolName,
		options.WithNegative(),
		options.WithSupportFor("startup"),
	)
	startupOptionMultiName := "startup_option_multi"
	startupOptionMultiDefinition := options.NewDefinition(
		startupOptionMultiName,
		options.WithMulti(),
		options.WithRequiresValue(),
		options.WithSupportFor("startup"),
	)
	startupOptionRequiresValueName := "startup_option_requires_value"
	startupOptionRequiresValueDefinition := options.NewDefinition(
		startupOptionRequiresValueName,
		options.WithRequiresValue(),
		options.WithSupportFor("startup"),
	)
	commandOptionBoolName := "command_option_bool"
	commandOptionBoolDefinition := options.NewDefinition(
		commandOptionBoolName,
		options.WithNegative(),
		options.WithSupportFor("command"),
	)
	commandOptionMultiName := "command_option_multi"
	commandOptionMultiDefinition := options.NewDefinition(
		commandOptionMultiName,
		options.WithMulti(),
		options.WithRequiresValue(),
		options.WithSupportFor("command"),
	)
	commandOptionRequiresValueName := "command_option_requires_value"
	commandOptionRequiresValueDefinition := options.NewDefinition(
		commandOptionRequiresValueName,
		options.WithRequiresValue(),
		options.WithSupportFor("command"),
	)
	args := &parsed.OrderedArgs{
		Args: []arguments.Argument{
			mustNewOption(t, "no"+startupOptionBoolName, nil, startupOptionBoolDefinition),
			mustNewOption(t, startupOptionMultiName, ptr("foo"), startupOptionMultiDefinition),
			mustNewOption(t, startupOptionBoolName, nil, startupOptionBoolDefinition),
			mustNewOption(t, startupOptionMultiName, ptr("bar"), startupOptionMultiDefinition),
			mustNewOption(t, startupOptionRequiresValueName, ptr("foo"), startupOptionRequiresValueDefinition),
			mustNewOption(t, startupOptionMultiName, ptr("bar"), startupOptionMultiDefinition),
			mustNewOption(t, startupOptionRequiresValueName, ptr("bar"), startupOptionRequiresValueDefinition),
			&arguments.PositionalArgument{Value: "command"},
			mustNewOption(t, "no"+commandOptionBoolName, nil, commandOptionBoolDefinition),
			mustNewOption(t, commandOptionMultiName, ptr("foo"), commandOptionMultiDefinition),
			mustNewOption(t, commandOptionBoolName, nil, commandOptionBoolDefinition),
			mustNewOption(t, commandOptionMultiName, ptr("bar"), commandOptionMultiDefinition),
			mustNewOption(t, commandOptionRequiresValueName, ptr("bar"), commandOptionRequiresValueDefinition),
			mustNewOption(t, commandOptionMultiName, ptr("foo"), commandOptionMultiDefinition),
			mustNewOption(t, commandOptionRequiresValueName, ptr("foo"), commandOptionRequiresValueDefinition),
		},
	}

	startupOptionBoolValue, err := parsed.RemoveAndAccumulateStartupOption[bool](args, startupOptionBoolName)
	require.NoError(t, err)
	assert.Equal(t, startupOptionBoolValue, true)

	startupOptionBoolValue, err = parsed.RemoveAndAccumulateStartupOption[bool](args, startupOptionBoolName)
	require.NoError(t, err)
	assert.Equal(t, startupOptionBoolValue, false)

	startupOptionMultiValue, err := parsed.RemoveAndAccumulateStartupOption[[]string](args, startupOptionMultiName)
	require.NoError(t, err)
	assert.Equal(t, startupOptionMultiValue, []string{"foo", "bar", "bar"})

	startupOptionMultiValue, err = parsed.RemoveAndAccumulateStartupOption[[]string](args, startupOptionMultiName)
	require.NoError(t, err)
	assert.Equal(t, startupOptionMultiValue, []string(nil))

	startupOptionRequiresValueValue, err := parsed.RemoveAndAccumulateStartupOption[string](args, startupOptionRequiresValueName)
	require.NoError(t, err)
	assert.Equal(t, startupOptionRequiresValueValue, "bar")

	startupOptionRequiresValueValue, err = parsed.RemoveAndAccumulateStartupOption[string](args, startupOptionRequiresValueName)
	require.NoError(t, err)
	assert.Equal(t, startupOptionRequiresValueValue, "")

}

func TestRemoveAndAccumulateCommandOption(t *testing.T) {

	startupOptionBoolName := "startup_option_bool"
	startupOptionBoolDefinition := options.NewDefinition(
		startupOptionBoolName,
		options.WithNegative(),
		options.WithSupportFor("startup"),
	)
	startupOptionMultiName := "startup_option_multi"
	startupOptionMultiDefinition := options.NewDefinition(
		startupOptionMultiName,
		options.WithMulti(),
		options.WithRequiresValue(),
		options.WithSupportFor("startup"),
	)
	startupOptionRequiresValueName := "startup_option_requires_value"
	startupOptionRequiresValueDefinition := options.NewDefinition(
		startupOptionRequiresValueName,
		options.WithRequiresValue(),
		options.WithSupportFor("startup"),
	)
	commandOptionBoolName := "command_option_bool"
	commandOptionBoolDefinition := options.NewDefinition(
		commandOptionBoolName,
		options.WithNegative(),
		options.WithSupportFor("command"),
	)
	commandOptionMultiName := "command_option_multi"
	commandOptionMultiDefinition := options.NewDefinition(
		commandOptionMultiName,
		options.WithMulti(),
		options.WithRequiresValue(),
		options.WithSupportFor("command"),
	)
	commandOptionRequiresValueName := "command_option_requires_value"
	commandOptionRequiresValueDefinition := options.NewDefinition(
		commandOptionRequiresValueName,
		options.WithRequiresValue(),
		options.WithSupportFor("command"),
	)
	args := &parsed.OrderedArgs{
		Args: []arguments.Argument{
			mustNewOption(t, "no"+startupOptionBoolName, nil, startupOptionBoolDefinition),
			mustNewOption(t, startupOptionMultiName, ptr("foo"), startupOptionMultiDefinition),
			mustNewOption(t, startupOptionBoolName, nil, startupOptionBoolDefinition),
			mustNewOption(t, startupOptionMultiName, ptr("bar"), startupOptionMultiDefinition),
			mustNewOption(t, startupOptionRequiresValueName, ptr("foo"), startupOptionRequiresValueDefinition),
			mustNewOption(t, startupOptionMultiName, ptr("bar"), startupOptionMultiDefinition),
			mustNewOption(t, startupOptionRequiresValueName, ptr("bar"), startupOptionRequiresValueDefinition),
			&arguments.PositionalArgument{Value: "command"},
			mustNewOption(t, "no"+commandOptionBoolName, nil, commandOptionBoolDefinition),
			mustNewOption(t, commandOptionMultiName, ptr("foo"), commandOptionMultiDefinition),
			mustNewOption(t, commandOptionBoolName, nil, commandOptionBoolDefinition),
			mustNewOption(t, commandOptionMultiName, ptr("bar"), commandOptionMultiDefinition),
			mustNewOption(t, commandOptionRequiresValueName, ptr("bar"), commandOptionRequiresValueDefinition),
			mustNewOption(t, commandOptionMultiName, ptr("foo"), commandOptionMultiDefinition),
			mustNewOption(t, commandOptionRequiresValueName, ptr("foo"), commandOptionRequiresValueDefinition),
		},
	}

	commandOptionBoolValue, err := parsed.RemoveAndAccumulateCommandOption[bool](args, commandOptionBoolName)
	require.NoError(t, err)
	assert.Equal(t, commandOptionBoolValue, true)

	commandOptionBoolValue, err = parsed.RemoveAndAccumulateCommandOption[bool](args, commandOptionBoolName)
	require.NoError(t, err)
	assert.Equal(t, commandOptionBoolValue, false)

	commandOptionMultiValue, err := parsed.RemoveAndAccumulateCommandOption[[]string](args, commandOptionMultiName)
	require.NoError(t, err)
	assert.Equal(t, commandOptionMultiValue, []string{"foo", "bar", "foo"})

	commandOptionMultiValue, err = parsed.RemoveAndAccumulateCommandOption[[]string](args, commandOptionMultiName)
	require.NoError(t, err)
	assert.Equal(t, commandOptionMultiValue, []string(nil))

	commandOptionRequiresValueValue, err := parsed.RemoveAndAccumulateCommandOption[string](args, commandOptionRequiresValueName)
	require.NoError(t, err)
	assert.Equal(t, commandOptionRequiresValueValue, "foo")

	commandOptionRequiresValueValue, err = parsed.RemoveAndAccumulateCommandOption[string](args, commandOptionRequiresValueName)
	require.NoError(t, err)
	assert.Equal(t, commandOptionRequiresValueValue, "")

}

func TestPrepend(t *testing.T) {

	startupOptionBoolName := "startup_option_bool"
	startupOptionBoolDefinition := options.NewDefinition(
		startupOptionBoolName,
		options.WithNegative(),
		options.WithSupportFor("startup"),
	)
	startupOptionMultiName := "startup_option_multi"
	startupOptionMultiDefinition := options.NewDefinition(
		startupOptionMultiName,
		options.WithMulti(),
		options.WithRequiresValue(),
		options.WithSupportFor("startup"),
	)
	startupOptionRequiresValueName := "startup_option_requires_value"
	startupOptionRequiresValueDefinition := options.NewDefinition(
		startupOptionRequiresValueName,
		options.WithRequiresValue(),
		options.WithSupportFor("startup"),
	)
	commandOptionBoolName := "command_option_bool"
	commandOptionBoolDefinition := options.NewDefinition(
		commandOptionBoolName,
		options.WithNegative(),
		options.WithSupportFor("command"),
	)
	commandOptionMultiName := "command_option_multi"
	commandOptionMultiDefinition := options.NewDefinition(
		commandOptionMultiName,
		options.WithMulti(),
		options.WithRequiresValue(),
		options.WithSupportFor("command"),
	)
	commandOptionRequiresValueName := "command_option_requires_value"
	commandOptionRequiresValueDefinition := options.NewDefinition(
		commandOptionRequiresValueName,
		options.WithRequiresValue(),
		options.WithSupportFor("command"),
	)
	args := &parsed.OrderedArgs{
		Args: []arguments.Argument{
			mustNewOption(t, "no"+startupOptionBoolName, nil, startupOptionBoolDefinition),
			mustNewOption(t, startupOptionMultiName, ptr("foo"), startupOptionMultiDefinition),
			mustNewOption(t, startupOptionBoolName, nil, startupOptionBoolDefinition),
			mustNewOption(t, startupOptionMultiName, ptr("bar"), startupOptionMultiDefinition),
			mustNewOption(t, startupOptionRequiresValueName, ptr("foo"), startupOptionRequiresValueDefinition),
			mustNewOption(t, startupOptionMultiName, ptr("bar"), startupOptionMultiDefinition),
			mustNewOption(t, startupOptionRequiresValueName, ptr("bar"), startupOptionRequiresValueDefinition),
			&arguments.PositionalArgument{Value: "command"},
			mustNewOption(t, "no"+commandOptionBoolName, nil, commandOptionBoolDefinition),
			mustNewOption(t, commandOptionMultiName, ptr("foo"), commandOptionMultiDefinition),
			mustNewOption(t, commandOptionBoolName, nil, commandOptionBoolDefinition),
			mustNewOption(t, commandOptionMultiName, ptr("bar"), commandOptionMultiDefinition),
			mustNewOption(t, commandOptionRequiresValueName, ptr("bar"), commandOptionRequiresValueDefinition),
			mustNewOption(t, commandOptionMultiName, ptr("foo"), commandOptionMultiDefinition),
			mustNewOption(t, commandOptionRequiresValueName, ptr("foo"), commandOptionRequiresValueDefinition),
		},
	}
	args.Prepend(
		mustNewOption(t, "no"+commandOptionBoolName, nil, commandOptionBoolDefinition),
		mustNewOption(t, commandOptionMultiName, ptr("foofoo"), commandOptionMultiDefinition),
		mustNewOption(t, commandOptionBoolName, nil, commandOptionBoolDefinition),
		mustNewOption(t, "no"+startupOptionBoolName, nil, startupOptionBoolDefinition),
		mustNewOption(t, startupOptionMultiName, ptr("foobar"), startupOptionMultiDefinition),
		mustNewOption(t, startupOptionBoolName, nil, startupOptionBoolDefinition),
	)

	assert.Equal(
		t,
		[]string{
			"--nostartup_option_bool",
			"--startup_option_multi=foobar",
			"--startup_option_bool",
			"--nostartup_option_bool",
			"--startup_option_multi=foo",
			"--startup_option_bool",
			"--startup_option_multi=bar",
			"--startup_option_requires_value=foo",
			"--startup_option_multi=bar",
			"--startup_option_requires_value=bar",
			"command",
			"--nocommand_option_bool",
			"--command_option_multi=foofoo",
			"--command_option_bool",
			"--nocommand_option_bool",
			"--command_option_multi=foo",
			"--command_option_bool",
			"--command_option_multi=bar",
			"--command_option_requires_value=bar",
			"--command_option_multi=foo",
			"--command_option_requires_value=foo",
		},
		args.Format(),
	)

}
