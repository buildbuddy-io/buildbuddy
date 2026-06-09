package ociauth

import (
	"context"
	"testing"
	"time"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	"github.com/stretchr/testify/require"
)

func TestTokenAuthenticator(t *testing.T) {
	auth := NewTokenAuthenticator[string](TokenAuthenticatorOpts{
		TokenTTL:   time.Minute,
		MaxEntries: 2,
	})

	require.False(t, auth.IsAuthorized("one"))

	auth.Refresh("one")
	require.True(t, auth.IsAuthorized("one"))

	auth.Refresh("two")
	require.True(t, auth.IsAuthorized("one"))

	auth.Refresh("three")
	require.True(t, auth.IsAuthorized("one"), "recently accessed token should be retained")
	require.False(t, auth.IsAuthorized("two"), "least-recently used token should be evicted")
	require.True(t, auth.IsAuthorized("three"))
}

func TestTokenAuthenticatorExpiry(t *testing.T) {
	auth := NewTokenAuthenticator[string](TokenAuthenticatorOpts{
		TokenTTL: -time.Second,
	})

	auth.Refresh("expired")
	require.False(t, auth.IsAuthorized("expired"))
}

func TestTokenAuthenticatorAuthorize(t *testing.T) {
	auth := NewTokenAuthenticator[string](TokenAuthenticatorOpts{
		TokenTTL: time.Minute,
	})

	proveCount := 0
	prove := func(context.Context) error {
		proveCount++
		return nil
	}

	require.NoError(t, auth.Authorize(context.Background(), "token", prove))
	require.NoError(t, auth.Authorize(context.Background(), "token", prove))
	require.Equal(t, 1, proveCount)
}

func TestCacheAccessTokenIsRepoScoped(t *testing.T) {
	repoA := mustRepo(t, "gcr.io/acme/app")
	repoB := mustRepo(t, "gcr.io/acme/other")

	auth := NewCacheAccessAuthenticator(time.Minute, 100)
	token := auth.Refresh(repoA, &rgpb.Credentials{
		Username: "user",
		Password: "pass",
	})

	require.True(t, token.IsValidForRepo(repoA))
	require.False(t, token.IsValidForRepo(repoB))
	require.True(t, BypassCacheAccessToken(repoA).IsValidForRepo(repoA))
	require.False(t, BypassCacheAccessToken(repoA).IsValidForRepo(repoB))
}

func TestCacheAccessAuthenticatorCredentialKeying(t *testing.T) {
	repo := mustRepo(t, "gcr.io/acme/app")
	auth := NewCacheAccessAuthenticator(time.Minute, 100)
	creds := &rgpb.Credentials{
		Username: "user",
		Password: "pass",
	}
	otherCreds := &rgpb.Credentials{
		Username: "other",
		Password: "pass",
	}

	auth.Refresh(repo, creds)

	_, ok := auth.CachedToken(repo, creds)
	require.True(t, ok)
	_, ok = auth.CachedToken(repo, otherCreds)
	require.False(t, ok)
	_, ok = auth.CachedToken(mustRepo(t, "gcr.io/acme/other"), creds)
	require.False(t, ok)
}

func TestCacheAccessAuthenticatorAnonymousCredentialKeying(t *testing.T) {
	repo := mustRepo(t, "gcr.io/acme/app")
	auth := NewCacheAccessAuthenticator(time.Minute, 100)

	nilCredsToken := auth.Refresh(repo, nil)
	emptyCredsToken, ok := auth.CachedToken(repo, &rgpb.Credentials{})

	require.True(t, ok)
	require.Equal(t, nilCredsToken, emptyCredsToken)
}

func mustRepo(t *testing.T, ref string) gcrname.Repository {
	t.Helper()
	repo, err := gcrname.NewRepository(ref)
	require.NoError(t, err)
	return repo
}
