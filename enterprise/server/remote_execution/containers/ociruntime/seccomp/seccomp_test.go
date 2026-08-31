package seccomp

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	specs "github.com/opencontainers/runtime-spec/specs-go"
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

func TestNew_AdditionalSyscallsOverrideDefaultDenyRules(t *testing.T) {
	// The default profile has deny rules covering userfaultfd and setns.
	// userfaultfd's only matching rule is a deny. setns appears in three
	// rules: the default allow list, then a cap-conditional allow and a
	// cap-conditional deny whose conditions are dropped when parsing the JSON
	// into specs.LinuxSeccomp.
	defaultProfile, err := New(nil)
	require.NoError(t, err)
	for _, name := range []string{"userfaultfd", "setns"} {
		denyRules := 0
		for _, rule := range defaultProfile.Syscalls {
			if slices.Contains(rule.Names, name) && rule.Action == specs.ActErrno {
				denyRules++
			}
		}
		require.GreaterOrEqual(t, denyRules, 1, "expected a default deny rule for %q", name)
	}

	// Allow both syscalls. Expect the appended allow rule to become the only
	// rule matching them. Keeping the default rules in place would leave the
	// outcome to runtime-specific conflict resolution. crun keeps whichever
	// rule for a syscall comes first, so userfaultfd would stay denied.
	// setns exercises removing a name that appears in several rules,
	// including allow rules that precede its deny rule.
	profile, err := New([]string{"userfaultfd", "setns"})
	require.NoError(t, err)
	for _, name := range []string{"userfaultfd", "setns"} {
		var matchingRules []specs.LinuxSyscall
		for _, rule := range profile.Syscalls {
			if slices.Contains(rule.Names, name) {
				matchingRules = append(matchingRules, rule)
			}
		}
		require.Len(t, matchingRules, 1, "expected exactly one rule for %q", name)
		assert.Equal(t, specs.ActAllow, matchingRules[0].Action)
		assert.Empty(t, matchingRules[0].Args)
	}

	// Expect syscalls that share a default rule with the configured names to
	// remain denied. The default rule denying userfaultfd also denies
	// vmsplice.
	var vmspliceActions []specs.LinuxSeccompAction
	for _, rule := range profile.Syscalls {
		if slices.Contains(rule.Names, "vmsplice") {
			vmspliceActions = append(vmspliceActions, rule.Action)
		}
	}
	assert.Equal(t, []specs.LinuxSeccompAction{specs.ActErrno}, vmspliceActions)
}
