package ociauth

import (
	"container/list"
	"context"
	"sync"
	"time"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	"github.com/buildbuddy-io/buildbuddy/server/util/hash"
	gcrname "github.com/google/go-containerregistry/pkg/name"
)

// TokenAuthenticator grants access to short-lived tokens after access has been
// proven against a remote registry.
type TokenAuthenticator[T comparable] struct {
	opts TokenAuthenticatorOpts

	mu      sync.Mutex
	entries map[T]*list.Element
	lru     *list.List
}

type tokenEntry[T comparable] struct {
	token      T
	expireTime time.Time
}

type TokenAuthenticatorOpts struct {
	// TokenTTL controls how long tokens can be used to access locally cached OCI
	// resources until re-authentication with the remote registry is required.
	TokenTTL time.Duration

	// MaxEntries bounds the number of cached tokens. If zero, no size bound is
	// applied.
	MaxEntries int
}

func NewTokenAuthenticator[T comparable](opts TokenAuthenticatorOpts) *TokenAuthenticator[T] {
	return &TokenAuthenticator[T]{
		opts:    opts,
		entries: map[T]*list.Element{},
		lru:     list.New(),
	}
}

func (a *TokenAuthenticator[T]) IsAuthorized(token T) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.purgeExpiredTokens()
	elem, ok := a.entries[token]
	if ok {
		a.lru.MoveToFront(elem)
	}
	return ok
}

func (a *TokenAuthenticator[T]) Refresh(token T) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.purgeExpiredTokens()
	expireTime := time.Now().Add(a.opts.TokenTTL)
	if elem, ok := a.entries[token]; ok {
		elem.Value.(*tokenEntry[T]).expireTime = expireTime
		a.lru.MoveToFront(elem)
		return
	}
	if a.opts.MaxEntries > 0 {
		for len(a.entries) >= a.opts.MaxEntries {
			a.evictOldestToken()
		}
	}
	elem := a.lru.PushFront(&tokenEntry[T]{
		token:      token,
		expireTime: expireTime,
	})
	a.entries[token] = elem
}

func (a *TokenAuthenticator[T]) Authorize(ctx context.Context, token T, prove func(context.Context) error) error {
	if a.IsAuthorized(token) {
		return nil
	}
	if err := prove(ctx); err != nil {
		return err
	}
	a.Refresh(token)
	return nil
}

func (a *TokenAuthenticator[T]) purgeExpiredTokens() {
	now := time.Now()
	for token, elem := range a.entries {
		if now.After(elem.Value.(*tokenEntry[T]).expireTime) {
			a.removeElement(elem)
			delete(a.entries, token)
		}
	}
}

func (a *TokenAuthenticator[T]) evictOldestToken() {
	elem := a.lru.Back()
	if elem != nil {
		a.removeElement(elem)
	}
}

func (a *TokenAuthenticator[T]) removeElement(elem *list.Element) {
	a.lru.Remove(elem)
	delete(a.entries, elem.Value.(*tokenEntry[T]).token)
}

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

// CacheAccessAuthenticator grants cache access tokens after registry access has
// been proven.
type CacheAccessAuthenticator struct {
	auth *TokenAuthenticator[CacheAccessToken]
}

func NewCacheAccessAuthenticator(ttl time.Duration, maxEntries int) *CacheAccessAuthenticator {
	return &CacheAccessAuthenticator{
		auth: NewTokenAuthenticator[CacheAccessToken](TokenAuthenticatorOpts{
			TokenTTL:   ttl,
			MaxEntries: maxEntries,
		}),
	}
}

func (a *CacheAccessAuthenticator) CachedToken(repo gcrname.Repository, creds *rgpb.Credentials) (CacheAccessToken, bool) {
	token := cacheAccessToken(repo, creds)
	if a.auth.IsAuthorized(token) {
		return token, true
	}
	return CacheAccessToken{}, false
}

func (a *CacheAccessAuthenticator) Authorize(ctx context.Context, repo gcrname.Repository, creds *rgpb.Credentials, prove func(context.Context) error) (CacheAccessToken, error) {
	token := cacheAccessToken(repo, creds)
	if err := a.auth.Authorize(ctx, token, prove); err != nil {
		return CacheAccessToken{}, err
	}
	return token, nil
}

func (a *CacheAccessAuthenticator) Refresh(repo gcrname.Repository, creds *rgpb.Credentials) CacheAccessToken {
	token := cacheAccessToken(repo, creds)
	a.auth.Refresh(token)
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
