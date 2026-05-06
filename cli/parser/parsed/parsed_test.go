package parsed

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/parser/arguments"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/options"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustOption(t *testing.T, optName string, value *string, d *options.Definition) options.Option {
	t.Helper()
	o, err := options.NewOption(optName, value, d)
	require.NoError(t, err)
	return o
}

func strPtr(s string) *string {
	return &s
}

func TestRemoveOptionsMutatesOrderedArgs(t *testing.T) {
	startupDef := options.NewDefinition("output_base", options.WithSupportFor("startup"), options.WithRequiresValue())
	commandDef := options.NewDefinition("test_arg", options.WithSupportFor("test"), options.WithRequiresValue(), options.WithMulti())
	args := &OrderedArgs{Args: []arguments.Argument{
		mustOption(t, "output_base", strPtr("/tmp/bazel-output"), startupDef),
		&arguments.PositionalArgument{Value: "test"},
		mustOption(t, "test_arg", strPtr("before"), commandDef),
		&arguments.PositionalArgument{Value: "//:target"},
		mustOption(t, "test_arg", strPtr("after"), commandDef),
	}}

	removedStartup := args.RemoveStartupOptions("output_base")
	removedCommand := args.RemoveCommandOptions("test_arg")

	require.Len(t, removedStartup, 1)
	assert.Equal(t, "/tmp/bazel-output", removedStartup[0].GetValue())
	require.Len(t, removedCommand, 2)
	assert.Equal(t, []string{"before", "after"}, []string{removedCommand[0].GetValue(), removedCommand[1].GetValue()})
	assert.Equal(t, []string{"test", "//:target"}, args.Format())
}

func TestAppendMutatesOrderedArgsByArgumentClass(t *testing.T) {
	startupDef := options.NewDefinition("output_base", options.WithSupportFor("startup"), options.WithRequiresValue())
	commandDef := options.NewDefinition("test_arg", options.WithSupportFor("test"), options.WithRequiresValue(), options.WithMulti())
	args := &OrderedArgs{Args: []arguments.Argument{
		&arguments.PositionalArgument{Value: "test"},
		&arguments.PositionalArgument{Value: "//:target"},
	}}

	err := args.Append(
		mustOption(t, "output_base", strPtr("/tmp/bazel-output"), startupDef),
		mustOption(t, "test_arg", strPtr("value"), commandDef),
	)

	require.NoError(t, err)
	assert.Equal(t, []string{"--output_base=/tmp/bazel-output", "test", "//:target", "--test_arg=value"}, args.Format())
}
