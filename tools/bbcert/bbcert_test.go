package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuiltInServers(t *testing.T) {
	require.Equal(t, []string{"grpcs://prod.example.com", "grpcs://dev.example.com"},
		builtInServers("grpcs://prod.example.com,grpcs://dev.example.com"))
	require.Equal(t, []string{"grpcs://a"}, builtInServers(" grpcs://a , "), "whitespace and empties are dropped")
	require.Nil(t, builtInServers(""), "nothing stamped")
	require.Nil(t, builtInServers("{STABLE_BBCERT_DEFAULT_SERVERS}"), "an unstamped build keeps the placeholder")
}
