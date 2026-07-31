package identity_test

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/test_health/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalizeCase(t *testing.T) {
	got, err := identity.CanonicalizeCase(identity.CaseInput{
		RepositoryURL: "git@github.com:buildbuddy-io/buildbuddy.git",
		TargetLabel:   "//enterprise/server/remote_execution/containers/firecracker:firecracker_test",
		CaseName:      "TestFirecrackerRunSimple/this test has spaces",
	})
	require.NoError(t, err)
	assert.Equal(t, identity.CaseAddress{
		Repository:  "https://github.com/buildbuddy-io/buildbuddy",
		TargetLabel: "//enterprise/server/remote_execution/containers/firecracker:firecracker_test",
		CaseName:    "TestFirecrackerRunSimple/this test has spaces",
	}, got.Address)
	assert.Equal(t, "enterprise/server/remote_execution/containers/firecracker", got.Target.PackagePath)

	proto := got.Proto()
	assert.Equal(t, got.Address, identity.CaseAddressFromProto(proto))
	assert.Equal(t, got.Address.Repository, proto.GetTarget().GetRepoUrl())
	assert.Equal(t, got.Address.TargetLabel, proto.GetTarget().GetTargetLabel())
	assert.Equal(t, got.Address.CaseName, proto.GetCaseName())
}

func TestCanonicalizeTarget(t *testing.T) {
	for input, want := range map[string]string{
		"//pkg":                  "//pkg:pkg",
		"//pkg:target":           "//pkg:target",
		"@//pkg:target":          "//pkg:target",
		"@@repo+1.0//pkg:target": "@@repo+1.0//pkg:target",
	} {
		t.Run(input, func(t *testing.T) {
			got, err := identity.CanonicalizeTarget(input)
			require.NoError(t, err)
			assert.Equal(t, want, got.Label)
		})
	}
}

func TestInvalidAddressesAreRejected(t *testing.T) {
	for _, input := range []identity.CaseInput{
		{RepositoryURL: "", TargetLabel: "//pkg:test", CaseName: "Test"},
		{RepositoryURL: "https://github.com/acme/repo", TargetLabel: "pkg:test", CaseName: "Test"},
		{RepositoryURL: "https://github.com/acme/repo", TargetLabel: "//pkg:test", CaseName: ""},
		{RepositoryURL: "https://github.com/acme/repo", TargetLabel: "//pkg:test", CaseName: "Test\nNewline"},
	} {
		_, err := identity.CanonicalizeCase(input)
		assert.Error(t, err, input)
	}

	_, err := identity.CanonicalizeCase(identity.CaseInput{
		RepositoryURL: "https://github.com/acme/repo",
		TargetLabel:   "//pkg:test",
		CaseName:      strings.Repeat("x", identity.MaxCaseNameBytes+1),
	})
	assert.Error(t, err)
}

func TestAddressRenderingIsReadableAndUnambiguous(t *testing.T) {
	address := identity.CaseAddress{
		Repository: "https://github.com/acme/repo", TargetLabel: "//pkg:test", CaseName: `subtest "quoted"`,
	}
	assert.Equal(t, `"https://github.com/acme/repo":"//pkg:test":"subtest \"quoted\""`, address.String())
	assert.False(t, strings.Contains(address.String(), "\n"))
}
