package agent

import (
	"flag"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/agent/agentflags"
	"github.com/buildbuddy-io/buildbuddy/cli/login"
	"github.com/buildbuddy-io/buildbuddy/cli/util/agent/agentutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// resetSharedFlags restores every shared agent flag to its default value.
//
// RegisterSharedFlags aliases the same flag.Value into every subcommand's flag
// set, so parsing any of them mutates package-level state. Without a reset,
// values set by one test case would leak into the next and make assertions
// order-dependent. Iterating the flag set covers flags added later for free.
func resetSharedFlags(t *testing.T) {
	t.Helper()
	agentflags.SharedAgentFlags.VisitAll(func(f *flag.Flag) {
		require.NoError(t, f.Value.Set(f.DefValue), "reset --%s to its default", f.Name)
	})
}

// installTestSubcommand replaces the registered subcommands with a single fake
// one for the duration of the test, and returns a pointer to the positional
// args that its handler received.
//
// The shared agent flags are reset both before and after the test, so that the
// test neither inherits values from an earlier one nor leaks its own.
func installTestSubcommand(t *testing.T) *[]string {
	t.Helper()

	resetSharedFlags(t)

	var handlerArgs []string
	flags := flag.NewFlagSet("test-subcommand", flag.ContinueOnError)
	agentflags.RegisterSharedFlags(flags)

	previousSubcommands := subcommands
	subcommands = []*subcommand{{
		name:  "test-subcommand",
		usage: "usage: bb agent test-subcommand",
		flags: flags,
		handler: func(args []string) (int, error) {
			handlerArgs = args
			return 0, nil
		},
	}}
	t.Cleanup(func() {
		subcommands = previousSubcommands
		resetSharedFlags(t)
	})
	return &handlerArgs
}

func TestHandleAgent_Parsing(t *testing.T) {
	defaultAgent := agentutil.Claude
	for _, test := range []struct {
		name                   string
		args                   []string
		expectedPositionalArgs []string
		expectedModel          string
		expectedAgent          string
		expectedEffort         string
		expectedAPITarget      string
	}{
		{
			name:                   "No flags specified, defaults applied",
			args:                   []string{"test-subcommand", "invocation-id"},
			expectedPositionalArgs: []string{"invocation-id"},
			expectedModel:          "",
			expectedAgent:          defaultAgent,
			expectedEffort:         "",
			expectedAPITarget:      login.DefaultApiTarget,
		},
		// bbrc expansion puts options ahead of the subcommand name.
		{
			name:                   "Flags specified before subcommand",
			args:                   []string{"--model=from-bbrc", "test-subcommand", "invocation-id"},
			expectedPositionalArgs: []string{"invocation-id"},
			expectedModel:          "from-bbrc",
			expectedAgent:          defaultAgent,
			expectedEffort:         "",
			expectedAPITarget:      login.DefaultApiTarget,
		},
		{
			name:                   "Multiple flags before subcommand",
			args:                   []string{"--model=from-bbrc", "--agent=codex", "test-subcommand", "invocation-id"},
			expectedPositionalArgs: []string{"invocation-id"},
			expectedModel:          "from-bbrc",
			expectedAgent:          agentutil.Codex,
			expectedEffort:         "",
			expectedAPITarget:      login.DefaultApiTarget,
		},
		{
			name:                   "Flags specified before and after subcommand",
			args:                   []string{"--model=from-bbrc", "test-subcommand", "--agent=codex", "invocation-id"},
			expectedPositionalArgs: []string{"invocation-id"},
			expectedModel:          "from-bbrc",
			expectedAgent:          agentutil.Codex,
			expectedEffort:         "",
			expectedAPITarget:      login.DefaultApiTarget,
		},
		{
			// The subcommand is found by name rather than by taking the first
			// non-option arg, which would pick up an option's value here.
			name:                   "Space-separated flag before subcommand",
			args:                   []string{"--model", "from-bbrc", "test-subcommand", "invocation-id"},
			expectedPositionalArgs: []string{"invocation-id"},
			expectedModel:          "from-bbrc",
			expectedAgent:          defaultAgent,
			expectedEffort:         "",
			expectedAPITarget:      login.DefaultApiTarget,
		},
		{
			name:                   "All shared flags specified",
			args:                   []string{"test-subcommand", "--model=from-bbrc", "--agent=codex", "--effort=high", "--target=grpcs://example.invalid", "invocation-id"},
			expectedPositionalArgs: []string{"invocation-id"},
			expectedModel:          "from-bbrc",
			expectedAgent:          agentutil.Codex,
			expectedEffort:         "high",
			expectedAPITarget:      "grpcs://example.invalid",
		},
		{
			name:                   "MultiplePositionalArgs",
			args:                   []string{"--model=from-bbrc", "test-subcommand", "first", "second"},
			expectedPositionalArgs: []string{"first", "second"},
			expectedModel:          "from-bbrc",
			expectedAgent:          defaultAgent,
			expectedEffort:         "",
			expectedAPITarget:      login.DefaultApiTarget,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			handlerArgs := installTestSubcommand(t)

			exitCode, err := HandleAgent(test.args)

			require.NoError(t, err)
			assert.Equal(t, 0, exitCode)
			assert.Equal(t, test.expectedPositionalArgs, *handlerArgs)
			assert.Equal(t, test.expectedModel, *agentflags.Model)
			assert.Equal(t, test.expectedAgent, *agentflags.Agent)
			assert.Equal(t, test.expectedEffort, *agentflags.Effort)
			assert.Equal(t, test.expectedAPITarget, *agentflags.APITarget)
		})
	}
}

func TestFindSubcommand(t *testing.T) {
	for _, test := range []struct {
		name         string
		args         []string
		expectedName string
		expectedRest []string
	}{
		{
			name:         "Subcommand first",
			args:         []string{"test-subcommand", "invocation-id"},
			expectedName: "test-subcommand",
			expectedRest: []string{"invocation-id"},
		},
		{
			name:         "Flags specified before subcommand",
			args:         []string{"--model=from-bbrc", "test-subcommand", "invocation-id"},
			expectedName: "test-subcommand",
			expectedRest: []string{"--model=from-bbrc", "invocation-id"},
		},
		{
			name:         "Flags specified after subcommand",
			args:         []string{"test-subcommand", "--model=from-bbrc"},
			expectedName: "test-subcommand",
			expectedRest: []string{"--model=from-bbrc"},
		},
		{
			name:         "Unknown subcommand",
			args:         []string{"--model=from-bbrc", "frobnicate"},
			expectedName: "",
			expectedRest: nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			installTestSubcommand(t)

			s, rest := findSubcommand(test.args)

			if test.expectedName == "" {
				assert.Nil(t, s)
				return
			}
			require.NotNil(t, s)
			assert.Equal(t, test.expectedName, s.name)
			assert.Equal(t, test.expectedRest, rest)
		})
	}
}
