package ociauth_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ociauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	gcrname "github.com/google/go-containerregistry/pkg/name"
)

func mustRepo(t *testing.T, name string) gcrname.Repository {
	t.Helper()
	repo, err := gcrname.NewRepository(name)
	require.NoError(t, err)
	return repo
}

func TestAuthorizeCacheAccess(t *testing.T) {
	a, err := ociauth.NewAuthenticator()
	require.NoError(t, err)
	ctx := context.Background()
	repo := mustRepo(t, "buildbuddy.io/test")
	creds := &rgpb.Credentials{Username: "user", Password: "pass"}

	proveCalls := 0
	prove := func(ctx context.Context) error {
		proveCalls++
		return nil
	}

	token, err := a.AuthorizeCacheAccess(ctx, repo, creds, prove)
	require.NoError(t, err)
	require.True(t, token.GrantsAccess(repo))
	require.Equal(t, 1, proveCalls)

	// A second authorization with the same repo and creds is served from the
	// proof cache without re-proving.
	token, err = a.AuthorizeCacheAccess(ctx, repo, creds, prove)
	require.NoError(t, err)
	require.True(t, token.GrantsAccess(repo))
	require.Equal(t, 1, proveCalls)

	// Different credentials must re-prove access.
	otherCreds := &rgpb.Credentials{Username: "other", Password: "pass"}
	_, err = a.AuthorizeCacheAccess(ctx, repo, otherCreds, prove)
	require.NoError(t, err)
	require.Equal(t, 2, proveCalls)

	// A different repository must re-prove access.
	otherRepo := mustRepo(t, "buildbuddy.io/other")
	token, err = a.AuthorizeCacheAccess(ctx, otherRepo, creds, prove)
	require.NoError(t, err)
	require.Equal(t, 3, proveCalls)
	require.True(t, token.GrantsAccess(otherRepo))
	require.False(t, token.GrantsAccess(repo))
}

func TestAuthorizeCacheAccessProofFailure(t *testing.T) {
	a, err := ociauth.NewAuthenticator()
	require.NoError(t, err)
	ctx := context.Background()
	repo := mustRepo(t, "buildbuddy.io/test")
	creds := &rgpb.Credentials{Username: "user", Password: "pass"}

	proveCalls := 0
	token, err := a.AuthorizeCacheAccess(ctx, repo, creds, func(ctx context.Context) error {
		proveCalls++
		return status.UnauthenticatedError("bad credentials")
	})
	require.True(t, status.IsUnauthenticatedError(err))
	require.False(t, token.GrantsAccess(repo))
	require.Equal(t, 1, proveCalls)

	// A failed check must not be recorded: the next authorization attempt
	// proves again.
	_, err = a.AuthorizeCacheAccess(ctx, repo, creds, func(ctx context.Context) error {
		proveCalls++
		return status.UnauthenticatedError("bad credentials")
	})
	require.True(t, status.IsUnauthenticatedError(err))
	require.Equal(t, 2, proveCalls)
}

func TestRecordAccess(t *testing.T) {
	a, err := ociauth.NewAuthenticator()
	require.NoError(t, err)
	ctx := context.Background()
	repo := mustRepo(t, "buildbuddy.io/test")
	creds := &rgpb.Credentials{Username: "user", Password: "pass"}

	require.False(t, a.HasRecentAccess(repo, creds))
	token := a.RecordAccess(repo, creds)
	require.True(t, token.GrantsAccess(repo))
	require.True(t, a.HasRecentAccess(repo, creds))
	require.False(t, a.HasRecentAccess(repo, nil))

	// Recorded access satisfies AuthorizeCacheAccess without proving.
	token, err = a.AuthorizeCacheAccess(ctx, repo, creds, func(ctx context.Context) error {
		return status.InternalError("should not be called")
	})
	require.NoError(t, err)
	require.True(t, token.GrantsAccess(repo))
}

func TestTokenScope(t *testing.T) {
	repo := mustRepo(t, "buildbuddy.io/test")
	otherRepo := mustRepo(t, "buildbuddy.io/other")

	var zero ociauth.CacheAccessToken
	require.False(t, zero.GrantsAccess(repo))

	token := ociauth.UncheckedCacheAccess(repo)
	require.True(t, token.GrantsAccess(repo))
	require.False(t, token.GrantsAccess(otherRepo))
}

func TestAuthorizeServerAdminCacheAccessRequiresClaims(t *testing.T) {
	repo := mustRepo(t, "buildbuddy.io/test")
	token, err := ociauth.AuthorizeServerAdminCacheAccess(context.Background(), repo)
	require.Error(t, err)
	require.False(t, token.GrantsAccess(repo))
}
