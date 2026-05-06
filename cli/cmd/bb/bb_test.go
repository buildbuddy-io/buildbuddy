package main

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/cli_command/register"
	"github.com/buildbuddy-io/buildbuddy/cli/parser"
	"github.com/buildbuddy-io/buildbuddy/cli/parser/test_data"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func init() {
	parser.SetBazelHelpForTesting(test_data.BazelHelpFlagsAsProtoOutput)
}

func TestInterpretAsBBCliCommandRecognizesNativeCommand(t *testing.T) {
	register.Register()
	input := []string{"--verbose", "login", "--api_key=abc123"}

	opts, command, args := interpretAsBBCliCommand(input)

	require.NotNil(t, command)
	assert.Equal(t, "login", command.Name)
	require.Len(t, opts, 1)
	assert.Equal(t, "verbose", opts[0].Name())
	assert.Equal(t, []string{"login", "--api_key=abc123"}, args)
	assert.Equal(t, []string{"--verbose", "login", "--api_key=abc123"}, input)
}

func TestInterpretAsBBCliCommandFallsBackOnUnknownStartupOption(t *testing.T) {
	register.Register()
	input := []string{"--output_base=/tmp/out", "login"}

	opts, command, args := interpretAsBBCliCommand(input)

	assert.Nil(t, opts)
	assert.Nil(t, command)
	assert.Equal(t, input, args)
}

func TestInterpretAsHelpCommandRewritesHelpFlagToHelpCommand(t *testing.T) {
	register.Register()

	args, err := interpretAsHelpCommand([]string{"test", "//:target", "--help"})

	require.NoError(t, err)
	assert.Equal(t, []string{"help", "test", "//:target"}, args.Format())
	assert.Equal(t, "help", args.GetCommand())
}
