package box

import (
	"testing"

	"github.com/stretchr/testify/require"

	gwpb "github.com/buildbuddy-io/buildbuddy/proto/gateway"
)

// TestFlagParsingStopsAtFirstPositional pins the property that makes
// `bb box mybox claude --continue` pass --continue to the remote command
// rather than parsing it as a bb flag.
func TestFlagParsingStopsAtFirstPositional(t *testing.T) {
	t.Cleanup(func() { createFlags.Set("t", "false") })
	require.NoError(t, createFlags.Parse([]string{"-t", "mybox", "claude", "--continue", "-N"}))
	require.Equal(t, []string{"mybox", "claude", "--continue", "-N"}, createFlags.Args())
	require.True(t, *forceTTY)
	require.False(t, *noCommand, "-N after the positional must not set the local flag")
}

func TestBoxName(t *testing.T) {
	tests := []struct {
		name  string
		valid bool
	}{
		{name: "mybox", valid: true},
		{name: "my-box_1.2", valid: true},
		{name: "9lives", valid: true},
		{name: "-leading-dash"},
		{name: ".."},
		{name: "a/b"},
		{name: "with space"},
		{name: ""},
	}
	for _, tc := range tests {
		require.Equal(t, tc.valid, boxNameRE.MatchString(tc.name), tc.name)
	}
	// Subcommand names are valid by the regexp but claimed by the dispatcher.
	for name := range reservedNames {
		require.True(t, boxNameRE.MatchString(name), name)
	}
}

func TestGenerateName(t *testing.T) {
	name := generateName(nil)
	require.True(t, boxNameRE.MatchString(name), name)
	require.False(t, reservedNames[name], name)

	// A name in use by a running box is never handed out again.
	taken := []*gwpb.Peer{{Name: name}}
	for i := 0; i < 20; i++ {
		require.NotEqual(t, name, generateName(taken))
	}
}
