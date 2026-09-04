package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitCommand(t *testing.T) {
	argv := []string{"bbcert", "update", "-commit", "abc123"}
	command, flags := splitCommand(argv[1:])
	require.Equal(t, "update", command)
	// main rebuilds os.Args in place from the flags; that must not change them.
	argv = append(argv[:1], flags...)
	require.Equal(t, []string{"-commit", "abc123"}, flags)
	require.Equal(t, []string{"bbcert", "-commit", "abc123"}, argv)

	command, flags = splitCommand([]string{"-server", "grpcs://a"})
	require.Equal(t, "", command, "flags alone select the default command")
	require.Equal(t, []string{"-server", "grpcs://a"}, flags)

	command, flags = splitCommand(nil)
	require.Equal(t, "", command)
	require.Empty(t, flags)
}

func TestBuiltInServers(t *testing.T) {
	require.Equal(t, []string{"grpcs://prod.example.com", "grpcs://dev.example.com"},
		builtInServers("grpcs://prod.example.com,grpcs://dev.example.com"))
	require.Equal(t, []string{"grpcs://a"}, builtInServers(" grpcs://a , "), "whitespace and empties are dropped")
	require.Nil(t, builtInServers(""), "nothing stamped")
	require.Nil(t, builtInServers("{STABLE_BBCERT_DEFAULT_SERVERS}"), "an unstamped build keeps the placeholder")
}
