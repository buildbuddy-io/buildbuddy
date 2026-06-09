package ociauth

import (
	"context"
	"time"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	gcrname "github.com/google/go-containerregistry/pkg/name"
)

// CacheAccessToken is a short-lived claim to access blobs cached for a remote
// OCI repository.
type CacheAccessToken struct {
	repo string
	key  string
}

func (t CacheAccessToken) IsValid() bool {
	return t.repo != "" && t.key != ""
}

func (t CacheAccessToken) IsValidForRepo(repo gcrname.Repository) bool {
	return t.IsValid() && t.repo == repo.Name()
}

// CacheAccessAuthenticator grants tokens after registry access has been proven.
type CacheAccessAuthenticator struct {
	cache lru.LRU[struct{}]
}

func NewCacheAccessAuthenticator(ttl time.Duration, maxEntries int) (*CacheAccessAuthenticator, error) {
	cache, err := lru.New[struct{}](&lru.Config[struct{}]{
		SizeFn:     func(_ struct{}) int64 { return 1 },
		MaxSize:    int64(maxEntries),
		TTL:        ttl,
		ThreadSafe: true,
	})
	if err != nil {
		return nil, err
	}
	return &CacheAccessAuthenticator{cache: cache}, nil
}

func (a *CacheAccessAuthenticator) CachedToken(repo gcrname.Repository, creds *rgpb.Credentials) (CacheAccessToken, bool) {
	token := cacheAccessToken(repo, creds)
	if a.cache.Contains(token.key) {
		return token, true
	}
	return CacheAccessToken{}, false
}

func (a *CacheAccessAuthenticator) Authorize(ctx context.Context, repo gcrname.Repository, creds *rgpb.Credentials, prove func(context.Context) error) (CacheAccessToken, error) {
	if token, ok := a.CachedToken(repo, creds); ok {
		return token, nil
	}
	if err := prove(ctx); err != nil {
		return CacheAccessToken{}, err
	}
	return a.Refresh(repo, creds), nil
}

func (a *CacheAccessAuthenticator) Refresh(repo gcrname.Repository, creds *rgpb.Credentials) CacheAccessToken {
	token := cacheAccessToken(repo, creds)
	a.cache.Add(token.key, struct{}{})
	return token
}

// BypassCacheAccessToken returns a token for trusted callers that have already
// authorized access outside of the remote registry proof flow.
func BypassCacheAccessToken(repo gcrname.Repository) CacheAccessToken {
	return CacheAccessToken{
		repo: repo.Name(),
		key:  "bypass:" + repo.Name(),
	}
}

// cacheAccessToken returns the access-proof cache token for the given
// repository and credentials. Registry pull authorization is repository-scoped,
// so the token is deliberately not specific to any one manifest or blob digest.
func cacheAccessToken(repo gcrname.Repository, creds *rgpb.Credentials) CacheAccessToken {
	repoName := repo.Name()
	if creds == nil {
		return CacheAccessToken{repo: repoName, key: hash.Strings(repoName, "", "")}
	}
	return CacheAccessToken{repo: repoName, key: hash.Strings(repoName, creds.GetUsername(), creds.GetPassword())}
}
