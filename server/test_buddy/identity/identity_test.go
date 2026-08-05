package identity_test

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tbpb "github.com/buildbuddy-io/buildbuddy/proto/test_buddy"
)

func TestCanonicalize(t *testing.T) {
	got, err := identity.Canonicalize(
		"git@github.com:buildbuddy-io/buildbuddy.git",
		&tbpb.TestIdentity{
			TargetLabel: "//enterprise/server/remote_execution/containers/firecracker:firecracker_test",
			CaseName:    "TestFirecrackerRunSimple/this test has spaces",
		},
	)
	require.NoError(t, err)
	assert.Equal(t, identity.Address{
		Repository:  "https://github.com/buildbuddy-io/buildbuddy",
		PackagePath: "enterprise/server/remote_execution/containers/firecracker",
		TargetName:  "firecracker_test",
		CaseName:    "TestFirecrackerRunSimple/this test has spaces",
	}, got)
	require.NoError(t, got.Validate())

	roundTrip, err := identity.Canonicalize(got.Repository, got.Proto())
	require.NoError(t, err)
	assert.Equal(t, got, roundTrip)
}

func TestCanonicalizeTarget(t *testing.T) {
	for input, want := range map[string]string{
		"//pkg":         "//pkg:pkg",
		"//pkg:target":  "//pkg:target",
		"@//pkg:target": "//pkg:target",
	} {
		t.Run(input, func(t *testing.T) {
			got, err := identity.Canonicalize(
				"https://github.com/acme/repo",
				&tbpb.TestIdentity{TargetLabel: input},
			)
			require.NoError(t, err)
			assert.Equal(t, want, got.TargetLabel())
			assert.Empty(t, got.CaseName)
		})
	}
}

func TestCanonicalizePreservesUnicodeCaseName(t *testing.T) {
	caseName := "TestTruncateStringSlice/[ツ]/🙂"
	got, err := identity.Canonicalize(
		"https://github.com/acme/repo",
		&tbpb.TestIdentity{TargetLabel: "//pkg:test", CaseName: caseName},
	)
	require.NoError(t, err)
	assert.Equal(t, caseName, got.CaseName)
}

func TestCanonicalizeRepositoryURLIsStable(t *testing.T) {
	got, err := identity.Canonicalize(
		"https://github.com/acme/repo.git.git",
		&tbpb.TestIdentity{TargetLabel: "//pkg:test"},
	)
	require.NoError(t, err)
	assert.Equal(t, "https://github.com/acme/repo", got.Repository)
	require.NoError(t, got.Validate())
}

func TestAddressValidation(t *testing.T) {
	assert.Error(t, (identity.Address{
		Repository: "git@github.com:acme/repo.git", PackagePath: "pkg", TargetName: "test",
	}).Validate())
	assert.Error(t, (identity.Address{
		Repository: "https://github.com/acme/repo", PackagePath: "pkg",
	}).Validate())
}

func TestInvalidAddressesAreRejected(t *testing.T) {
	for _, input := range []struct {
		repository string
		target     string
		caseName   string
	}{
		{repository: "", target: "//pkg:test"},
		{repository: "https://", target: "//pkg:test"},
		{repository: "/", target: "//pkg:test"},
		{repository: "buildbuddy", target: "//pkg:test"},
		{repository: "https://github.com/acme/repo?ref=main", target: "//pkg:test"},
		{repository: "https://github.com/acme/repo", target: "pkg:test"},
		{repository: "https://github.com/acme/repo", target: "@@repo+1.0//pkg:test"},
		{repository: "https://github.com/acme/repo", target: "//..."},
		{repository: "https://github.com/acme/repo", target: "//foo/..."},
		{repository: "https://github.com/acme/repo", target: "//pkg:all"},
		{repository: "https://github.com/acme/repo", target: "//pkg:*"},
		{repository: "https://github.com/acme/repo", target: "//pkg:test", caseName: "Test\nNewline"},
		{repository: "https://github.com/acme/repo", target: "//pkg:test", caseName: "Test\tTab"},
	} {
		_, err := identity.Canonicalize(input.repository, &tbpb.TestIdentity{
			TargetLabel: input.target,
			CaseName:    input.caseName,
		})
		assert.Error(t, err, input)
	}

	_, err := identity.Canonicalize(
		"https://github.com/acme/repo",
		&tbpb.TestIdentity{TargetLabel: "//pkg:test", CaseName: strings.Repeat("x", 513)},
	)
	assert.Error(t, err)
}

func TestAddressRenderingIsReadableAndUnambiguous(t *testing.T) {
	address := identity.Address{
		Repository: "https://github.com/acme/repo", PackagePath: "pkg", TargetName: "test",
		CaseName: `subtest "quoted"`,
	}
	assert.Equal(t, `"https://github.com/acme/repo":"pkg":"test":"subtest \"quoted\""`, address.String())
	assert.False(t, strings.Contains(address.String(), "\n"))
}
