package main

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
)

func TestFlagsAgainstConfig(t *testing.T) {
	flags.CheckFlagsAgainstConfig(t)
}
