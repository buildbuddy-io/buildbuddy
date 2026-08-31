package seccomp

import (
	"encoding/json"
	"strings"
	"testing"

	specs "github.com/opencontainers/runtime-spec/specs-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew_Default(t *testing.T) {
	want := &specs.LinuxSeccomp{}
	require.NoError(t, json.Unmarshal(defaultProfileJSON, want))

	got, err := New(nil)
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestNew_AdditionalSyscalls(t *testing.T) {
	additionalSyscalls := []string{
		"io_uring_setup",
		"io_uring_enter",
		"io_uring_register",
		"io_uring_setup",
	}
	profile, err := New(additionalSyscalls)
	require.NoError(t, err)

	wantNames := []string{
		"io_uring_enter",
		"io_uring_register",
		"io_uring_setup",
	}
	matchingRules := 0
	for _, rule := range profile.Syscalls {
		for _, name := range rule.Names {
			if !strings.HasPrefix(name, "io_uring_") {
				continue
			}
			matchingRules++
			assert.Equal(t, specs.ActAllow, rule.Action)
			assert.Empty(t, rule.Args)
			assert.Equal(t, wantNames, rule.Names)
			break
		}
	}
	assert.Equal(t, 1, matchingRules)
	assert.Equal(t, []string{
		"io_uring_setup",
		"io_uring_enter",
		"io_uring_register",
		"io_uring_setup",
	}, additionalSyscalls)
}
