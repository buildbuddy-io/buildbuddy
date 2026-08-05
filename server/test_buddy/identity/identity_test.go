package identity_test

import (
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/test_buddy/identity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCanonicalizeCase(t *testing.T) {
	got, err := identity.CanonicalizeCase(
		"git@github.com:buildbuddy-io/buildbuddy.git",
		"//enterprise/server/remote_execution/containers/firecracker:firecracker_test",
		"TestFirecrackerRunSimple/this test has spaces",
	)
	require.NoError(t, err)
	assert.Equal(t, identity.CaseAddress{
		TargetAddress: identity.TargetAddress{
			Repository:  "https://github.com/buildbuddy-io/buildbuddy",
			PackagePath: "enterprise/server/remote_execution/containers/firecracker",
			TargetName:  "firecracker_test",
		},
		CaseName: "TestFirecrackerRunSimple/this test has spaces",
	}, got)
	assert.Equal(t, identity.PackageAddress{
		Repository:  "https://github.com/buildbuddy-io/buildbuddy",
		PackagePath: "enterprise/server/remote_execution/containers/firecracker",
	}, got.Target().Package())

	proto := got.Proto()
	roundTrip, err := identity.CaseAddressFromProto(got.Repository, proto)
	require.NoError(t, err)
	assert.Equal(t, got, roundTrip)
	assert.Equal(t, got.Target().Label(), proto.GetTarget().GetTargetLabel())
	assert.Equal(t, got.CaseName, proto.GetCaseName())
}

func TestCaseNameStorageKey(t *testing.T) {
	for _, test := range []struct {
		name        string
		wantEncoded bool
	}{
		{name: "TestCaseName"},
		{name: "TestTruncateStringSlice/[ツ]/1", wantEncoded: true},
		{name: "~literal-prefix", wantEncoded: true},
		{name: strings.Repeat("🙂", identity.MaxCaseNameBytes/4), wantEncoded: true},
	} {
		key, err := identity.CaseNameKey(test.name)
		require.NoError(t, err)
		if test.wantEncoded {
			assert.NotEqual(t, test.name, key)
			assert.True(t, strings.HasPrefix(key, "~"))
		} else {
			assert.Equal(t, test.name, key)
		}
		assert.LessOrEqual(t, len(key), identity.MaxCaseNameKeyBytes)
		got, err := identity.CaseNameFromKey(key)
		require.NoError(t, err)
		assert.Equal(t, test.name, got)
	}

	unicodeKey, err := identity.CaseNameKey("TestTruncateStringSlice/[ツ]/1")
	require.NoError(t, err)
	literalPrefixKey, err := identity.CaseNameKey(unicodeKey)
	require.NoError(t, err)
	assert.NotEqual(t, unicodeKey, literalPrefixKey)

	_, err = identity.CaseNameFromKey("~not!base64")
	assert.Error(t, err)
}

func TestCanonicalizeTargetLabel(t *testing.T) {
	for input, want := range map[string]string{
		"//pkg":         "//pkg:pkg",
		"//pkg:target":  "//pkg:target",
		"@//pkg:target": "//pkg:target",
	} {
		t.Run(input, func(t *testing.T) {
			got, err := identity.CanonicalizeTargetLabel(input)
			require.NoError(t, err)
			assert.Equal(t, want, got)
		})
	}
}

func TestAddressValidation(t *testing.T) {
	packageAddress := identity.PackageAddress{
		Repository: "https://github.com/acme/repo", PackagePath: "pkg/subpkg",
	}
	targetAddress := identity.TargetAddress{
		Repository: packageAddress.Repository, PackagePath: packageAddress.PackagePath, TargetName: "unit_test",
	}
	caseAddress := identity.CaseAddress{TargetAddress: targetAddress, CaseName: "TestCase"}
	require.NoError(t, packageAddress.Validate())
	require.NoError(t, targetAddress.Validate())
	require.NoError(t, caseAddress.Validate())

	assert.Error(t, (identity.PackageAddress{
		Repository: "git@github.com:acme/repo.git", PackagePath: "pkg/subpkg",
	}).Validate())
	assert.Error(t, (identity.TargetAddress{
		Repository: packageAddress.Repository, PackagePath: packageAddress.PackagePath,
	}).Validate())
	assert.Error(t, (identity.CaseAddress{TargetAddress: targetAddress}).Validate())
}

func TestInvalidAddressesAreRejected(t *testing.T) {
	for _, input := range []struct {
		repository string
		target     string
		caseName   string
	}{
		{repository: "", target: "//pkg:test", caseName: "Test"},
		{repository: "https://github.com/acme/repo", target: "pkg:test", caseName: "Test"},
		{repository: "https://github.com/acme/repo", target: "@@repo+1.0//pkg:test", caseName: "Test"},
		{repository: "https://github.com/acme/repo", target: "//pkg:test", caseName: ""},
		{repository: "https://github.com/acme/repo", target: "//pkg:test", caseName: "Test\nNewline"},
		{repository: "https://github.com/acme/repo", target: "//pkg:test", caseName: "Test\tTab"},
	} {
		_, err := identity.CanonicalizeCase(input.repository, input.target, input.caseName)
		assert.Error(t, err, input)
	}

	_, err := identity.CanonicalizeCase(
		"https://github.com/acme/repo", "//pkg:test",
		strings.Repeat("x", identity.MaxCaseNameBytes+1),
	)
	assert.Error(t, err)
}

func TestAddressRenderingIsReadableAndUnambiguous(t *testing.T) {
	address := identity.CaseAddress{
		TargetAddress: identity.TargetAddress{
			Repository: "https://github.com/acme/repo", PackagePath: "pkg", TargetName: "test",
		},
		CaseName: `subtest "quoted"`,
	}
	assert.Equal(t, `"https://github.com/acme/repo":"pkg":"test":"subtest \"quoted\""`, address.String())
	assert.False(t, strings.Contains(address.String(), "\n"))
}
