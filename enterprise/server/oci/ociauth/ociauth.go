// Package ociauth issues tokens proving that a caller may read OCI artifacts
// out of the BuildBuddy cache.
//
// Cached OCI blobs are content-addressed and shared across groups, so serving
// one from the cache must be gated on proof that the caller could have
// fetched it from the upstream registry. ociauth makes that proof explicit:
// ocicache.FetchBlobFromCache requires a CacheAccessToken, and tokens can
// only be constructed by this package.
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
	key := accessProofKey(repo, creds)
	if a.proofs.Contains(key) {
		return CacheAccessToken{repo: repo.Name()}, nil
	}
	if err := prove(ctx); err != nil {
		return CacheAccessToken{}, err
	}
	a.proofs.Add(key, struct{}{})
	return CacheAccessToken{repo: repo.Name()}, nil
}

// RecordAccess records that creds were just proven to grant access to repo by
// some out-of-band success (e.g. a fetch from the upstream registry) and
// returns the corresponding token.
func (a *Authenticator) RecordAccess(repo gcrname.Repository, creds *rgpb.Credentials) CacheAccessToken {
	a.proofs.Add(accessProofKey(repo, creds), struct{}{})
	return CacheAccessToken{repo: repo.Name()}
}

// HasRecentAccess returns whether access to repo with creds was proven
// recently enough that the proof is still valid.
func (a *Authenticator) HasRecentAccess(repo gcrname.Repository, creds *rgpb.Credentials) bool {
	return a.proofs.Contains(accessProofKey(repo, creds))
}

// accessProofKey returns the access-proof cache key for the given repository
// and credentials. Registry pull authorization is repository-scoped, so the
// key is deliberately not specific to any one manifest or blob digest.
func accessProofKey(repo gcrname.Repository, creds *rgpb.Credentials) string {
	if creds == nil {
		return hash.Strings(repo.Name(), "", "")
	}
	return hash.Strings(repo.Name(), creds.GetUsername(), creds.GetPassword())
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

// UncheckedCacheAccess returns a token scoped to repo without verifying
// registry access. It exists for callers whose access is enforced by other
// means: the ociregistry mirror, which intentionally serves cached content
// without re-authenticating against the upstream registry, and tools like
// replay_action whose cache reads are authorized by the cache itself (via
// API key). New callers should use Authenticator.AuthorizeCacheAccess
// instead.
func UncheckedCacheAccess(repo gcrname.Repository) CacheAccessToken {
	return CacheAccessToken{repo: repo.Name()}
}
