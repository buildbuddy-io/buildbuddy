// Package ociauth issues tokens proving that a caller may read OCI artifacts
// out of the BuildBuddy cache.
//
// Cached OCI blobs are content-addressed and shared across groups, so serving
// one from the cache must be gated on proof that the caller could have
// fetched it from the upstream registry. ociauth makes that proof explicit:
// ocicache.FetchBlobFromCache requires a CacheAccessToken, and tokens can
// only be constructed by this package, via one of:
//
//   - Authenticator.AuthorizeCacheAccess, which runs an access check (or
//     reuses a recent one),
//   - Authenticator.RecordAccess, which must immediately follow a concrete
//     success demonstrating access (e.g. an authenticated fetch from the
//     upstream registry) — call sites must document that success, or
//   - AuthorizeServerAdminCacheAccess, which checks server-admin claims.
//
// There is deliberately no way to mint a token without one of these checks.
package ociauth

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	gcrname "github.com/google/go-containerregistry/pkg/name"
)

const (
	// How long a successful registry access check can be reused before the
	// caller must re-prove access with the upstream registry.
	accessProofTTL        = 15 * time.Minute
	accessProofMaxEntries = 1000
)

// CacheAccessToken proves that a caller recently demonstrated it may access
// an OCI repository, either by authenticating with the upstream registry or
// by holding server-admin credentials. The zero value grants access to
// nothing.
type CacheAccessToken struct {
	repo string
}

// GrantsAccess returns whether the token authorizes reading cached artifacts
// in the given repository. Registry pull authorization is repository-scoped,
// so tokens are scoped to a repository rather than to individual digests.
func (t CacheAccessToken) GrantsAccess(repo gcrname.Repository) bool {
	return t.repo != "" && t.repo == repo.Name()
}

// Authenticator issues CacheAccessTokens, remembering recent successful
// access checks (per repository and credentials) in an LRU so that repeated
// fetches do not have to re-authenticate with the upstream registry every
// time.
type Authenticator struct {
	proofs lru.LRU[struct{}]
}

func NewAuthenticator() (*Authenticator, error) {
	proofs, err := lru.New[struct{}](&lru.Config[struct{}]{
		SizeFn:     func(_ struct{}) int64 { return 1 },
		MaxSize:    accessProofMaxEntries,
		TTL:        accessProofTTL,
		ThreadSafe: true,
	})
	if err != nil {
		return nil, status.InternalErrorf("error initializing access proof cache: %s", err)
	}
	return &Authenticator{proofs: proofs}, nil
}

// AuthorizeCacheAccess returns a token scoped to repo if creds grant access
// to it. If access was proven recently for the same repo and creds, the
// token is issued without invoking prove. Otherwise prove is invoked
// (typically a HEAD request to the upstream registry) and a successful check
// is recorded for subsequent calls.
func (a *Authenticator) AuthorizeCacheAccess(ctx context.Context, repo gcrname.Repository, creds *rgpb.Credentials, prove func(ctx context.Context) error) (CacheAccessToken, error) {
	return a.AuthorizeCacheAccessForCredsKey(ctx, repo, protoCredsKey(creds), prove)
}

// AuthorizeCacheAccessForCredsKey is AuthorizeCacheAccess for callers whose
// credentials are not rgpb.Credentials (e.g. a raw Authorization header).
// credsKey must uniquely identify the credentials; callers should hash any
// secret material rather than passing it directly.
func (a *Authenticator) AuthorizeCacheAccessForCredsKey(ctx context.Context, repo gcrname.Repository, credsKey string, prove func(ctx context.Context) error) (CacheAccessToken, error) {
	key := accessProofKey(repo, credsKey)
	if a.proofs.Contains(key) {
		return CacheAccessToken{repo: repo.Name()}, nil
	}
	if err := prove(ctx); err != nil {
		return CacheAccessToken{}, err
	}
	a.proofs.Add(key, struct{}{})
	return CacheAccessToken{repo: repo.Name()}, nil
}

// RecordAccess records that creds were just proven to grant access to repo
// and returns the corresponding token. It must only be called immediately
// after a concrete success that demonstrates access — for example, an
// authenticated fetch from the upstream registry, or (for trusted tooling) a
// read authorized by the cache's own access controls. Call sites must
// document the success that justifies them.
func (a *Authenticator) RecordAccess(repo gcrname.Repository, creds *rgpb.Credentials) CacheAccessToken {
	return a.RecordAccessForCredsKey(repo, protoCredsKey(creds))
}

// RecordAccessForCredsKey is RecordAccess for callers whose credentials are
// not rgpb.Credentials. See AuthorizeCacheAccessForCredsKey for credsKey
// requirements.
func (a *Authenticator) RecordAccessForCredsKey(repo gcrname.Repository, credsKey string) CacheAccessToken {
	a.proofs.Add(accessProofKey(repo, credsKey), struct{}{})
	return CacheAccessToken{repo: repo.Name()}
}

// HasRecentAccess returns whether access to repo with creds was proven
// recently enough that the proof is still valid.
func (a *Authenticator) HasRecentAccess(repo gcrname.Repository, creds *rgpb.Credentials) bool {
	return a.proofs.Contains(accessProofKey(repo, protoCredsKey(creds)))
}

// protoCredsKey derives a creds key from proto credentials. A nil proto is
// equivalent to empty (anonymous) credentials.
func protoCredsKey(creds *rgpb.Credentials) string {
	return hash.Strings(creds.GetUsername(), creds.GetPassword())
}

// accessProofKey returns the access-proof cache key for the given repository
// and credentials key. Registry pull authorization is repository-scoped, so
// the key is deliberately not specific to any one manifest or blob digest.
func accessProofKey(repo gcrname.Repository, credsKey string) string {
	return hash.Strings(repo.Name(), credsKey)
}

// AuthorizeServerAdminCacheAccess returns a token scoped to repo if the
// caller is a server admin. Server admins may read cached artifacts without
// proving registry access (e.g. when bypassing the registry entirely).
func AuthorizeServerAdminCacheAccess(ctx context.Context, repo gcrname.Repository) (CacheAccessToken, error) {
	if err := claims.AuthorizeServerAdmin(ctx); err != nil {
		return CacheAccessToken{}, err
	}
	return CacheAccessToken{repo: repo.Name()}, nil
}
