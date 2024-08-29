package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRequests(t *testing.T) {
	err := doRequests()
	require.NoError(t, err)
}
