package crypter_key_cache

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/jonboulle/clockwork"
	"go.uber.org/atomic"

	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	keyTTL = flag.Duration("crypter.key_ttl", 10*time.Minute, "The maximum amount of time a key can be cached without being re-verified before it is considered invalid.")
)

const (
	// How often to check for keys that need to be refreshed.
	defaultKeyRefreshScanFrequency = 10 * time.Second
	// How long to wait after a failed refresh attempt before trying again.
	keyRefreshRetryInterval = 30 * time.Second
	keyRefreshDeadline      = 25 * time.Second
	defaultKeyErrCacheTime  = 10 * time.Second
)

// Note: there are two types of keys in the cache, one with only groupID set
// (encryption) and one with all values set (decryption).
type CacheKey struct {
	GroupID string
	KeyID   string
	Version int64
}

func (ck *CacheKey) String() string {
	if ck.KeyID == "" {
		return ck.GroupID
	} else {
		return fmt.Sprintf("%s/%s/%d", ck.GroupID, ck.KeyID, ck.Version)
	}
}

type cacheEntry struct {
	err error

	mu                 sync.Mutex
	keyMetadata        *sgpb.EncryptionMetadata
	derivedKey         []byte
	lastUse            time.Time
	expiresAfter       time.Time
	lastRefreshAttempt time.Time
}

type KeyCache struct {
	env       environment.Env
	refreshFn func(ctx context.Context, ck CacheKey) ([]byte, *sgpb.EncryptionMetadata, error)
	clock     clockwork.Clock
	sf        singleflight.Group[string, *crypter.DerivedKey]

	data sync.Map

	mu               sync.Mutex
	lastRefreshRun   time.Time
	activeRefreshOps atomic.Int32

	refreshScanFrequency time.Duration
	errCacheTime         time.Duration
}

type Opts struct {
	KeyRefreshScanFrequency time.Duration
	KeyErrCacheTime         time.Duration
}

func New(env environment.Env, refreshFn func(ctx context.Context, ck CacheKey) ([]byte, *sgpb.EncryptionMetadata, error), clock clockwork.Clock) *KeyCache {
	opts := Opts{
		KeyRefreshScanFrequency: defaultKeyRefreshScanFrequency,
		KeyErrCacheTime:         defaultKeyErrCacheTime,
	}
	return NewWithOpts(env, refreshFn, clock, &opts)
}

func NewWithOpts(env environment.Env, refreshFn func(ctx context.Context, ck CacheKey) ([]byte, *sgpb.EncryptionMetadata, error), clock clockwork.Clock, opts *Opts) *KeyCache {
	return &KeyCache{
		env:                  env,
		refreshFn:            refreshFn,
		clock:                clock,
		refreshScanFrequency: opts.KeyRefreshScanFrequency,
		errCacheTime:         opts.KeyErrCacheTime,
	}
}

func (c *KeyCache) checkCacheEntry(ck CacheKey, ce *cacheEntry) {
	ce.mu.Lock()
	defer ce.mu.Unlock()

	// If we reached the expiration and the key was not refreshed, then
	// remove it from the cache.
	if c.clock.Now().After(ce.expiresAfter) {
		for i := range ce.derivedKey {
			ce.derivedKey[i] = 0
		}
		c.data.Delete(ck)
		return
	}

	// If the expiration is far into the future, don't do anything.
	if ce.expiresAfter.Sub(c.clock.Now()) > *keyTTL/2 {
		return
	}

	// Don't try to extend the life of the key if it hasn't been used recently.
	if c.clock.Now().Sub(ce.lastUse) > *keyTTL/2 {
		return
	}

	// Don't try to refresh the key if we already tried recently.
	if c.clock.Since(ce.lastRefreshAttempt) < keyRefreshRetryInterval {
		return
	}

	c.activeRefreshOps.Inc()
	go func() {
		defer c.activeRefreshOps.Dec()
		ctx, cancel := context.WithTimeout(c.env.GetServerContext(), keyRefreshDeadline)
		defer cancel()
		ce.mu.Lock()
		ce.lastRefreshAttempt = c.clock.Now()
		ce.mu.Unlock()
		loadedKey, err := c.refreshKey(ctx, ck, false /*=cacheErr*/)
		if err == nil {
			ce.mu.Lock()
			ce.derivedKey = loadedKey.Key
			ce.keyMetadata = loadedKey.Metadata
			ce.expiresAfter = c.clock.Now().Add(*keyTTL)
			ce.mu.Unlock()
		} else {
			log.Warningf("could not refresh key %q: %s", ck, err)
		}
	}()
}

func (c *KeyCache) StartRefresher(quitChan chan struct{}) {
	// For the sake of testing, create the timer up front before returning
	// from this func. That way tests are guaranteed that the timer will be
	// fired when time is advanced using a fake clock.
	t := c.clock.NewTimer(c.refreshScanFrequency)

	go func() {
		for {
			select {
			case <-quitChan:
				return
			case <-t.Chan():
				// continue with for loop
			}

			c.data.Range(func(key, value any) bool {
				ck := key.(CacheKey)
				ce := value.(*cacheEntry)
				c.checkCacheEntry(ck, ce)
				return true
			})

			c.mu.Lock()
			c.lastRefreshRun = c.clock.Now()
			t.Reset(c.refreshScanFrequency)
			c.mu.Unlock()
		}
	}()
}

func (c *KeyCache) TestGetLastRefreshRun() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastRefreshRun
}

func (c *KeyCache) TestGetActiveRefreshOps() int32 {
	return c.activeRefreshOps.Load()
}

func (c *KeyCache) cacheAdd(ck CacheKey, ce *cacheEntry) {
	ce.lastUse = c.clock.Now()
	c.data.Store(ck, ce)
}

func (c *KeyCache) cacheGet(ck CacheKey) (*cacheEntry, bool) {
	v, ok := c.data.Load(ck)
	if !ok {
		return nil, false
	}
	e := v.(*cacheEntry)
	e.mu.Lock()
	defer e.mu.Unlock()
	e.lastUse = c.clock.Now()
	return e, true
}

func (c *KeyCache) refreshKeyWithRetries(ctx context.Context, ck CacheKey, cacheError bool) (*crypter.DerivedKey, error) {
	var lastErr error
	opts := retry.DefaultOptions()
	opts.Clock = c.clock
	retrier := retry.New(ctx, opts)
	for retrier.Next() {
		key, md, err := c.refreshFn(ctx, ck)
		if err == nil {
			c.cacheAdd(ck, &cacheEntry{
				expiresAfter: c.clock.Now().Add(*keyTTL),
				keyMetadata:  md,
				derivedKey:   key,
			})
		}
		// TODO(vadim): figure out if there are other KMS errors we can treat as immediate failures
		if err == nil || status.IsNotFoundError(err) {
			return &crypter.DerivedKey{Key: key, Metadata: md}, err
		}
		lastErr = err
	}
	if cacheError {
		c.cacheAdd(ck, &cacheEntry{
			err:          lastErr,
			expiresAfter: c.clock.Now().Add(c.errCacheTime),
		})
	}
	return nil, status.UnavailableErrorf("exhausted attempts to refresh key, last error: %s", lastErr)
}

func (c *KeyCache) refreshKey(ctx context.Context, ck CacheKey, cacheError bool) (*crypter.DerivedKey, error) {
	v, _, err := c.sf.Do(ctx, ck.String(), func(ctx context.Context) (*crypter.DerivedKey, error) {
		metrics.EncryptionKeyRefreshCount.Inc()
		k, err := c.refreshKeyWithRetries(ctx, ck, cacheError)
		if err != nil {
			metrics.EncryptionKeyRefreshFailureCount.Inc()
		}
		return k, err
	})
	return v, err
}

func (c *KeyCache) loadKey(ctx context.Context, em *sgpb.EncryptionMetadata) (*crypter.DerivedKey, error) {
	u, err := c.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	var ck CacheKey
	if em != nil {
		if em.GetEncryptionKeyId() == "" {
			return nil, status.FailedPreconditionError("metadata does not contain a valid key ID")
		}
		if em.GetVersion() == 0 {
			return nil, status.FailedPreconditionError("metadata does not contain a valid key version")
		}
		ck = CacheKey{GroupID: u.GetGroupID(), KeyID: em.GetEncryptionKeyId(), Version: em.GetVersion()}
	} else {
		ck = CacheKey{GroupID: u.GetGroupID()}
	}

	e, ok := c.cacheGet(ck)
	if ok {
		e.mu.Lock()
		defer e.mu.Unlock()
		if e.err != nil {
			return nil, e.err
		}
		return &crypter.DerivedKey{Key: e.derivedKey, Metadata: e.keyMetadata}, nil
	}

	// If obtaining the key fails, cache the error.
	loadedKey, err := c.refreshKey(ctx, ck, true /*=cacheErr*/)
	if err != nil {
		log.Warningf("could not refresh key: %s", err)
		return nil, err
	}
	return loadedKey, nil
}

func (c *KeyCache) EncryptionKey(ctx context.Context) (*crypter.DerivedKey, error) {
	return c.loadKey(ctx, nil)
}

func (c *KeyCache) DecryptionKey(ctx context.Context, em *sgpb.EncryptionMetadata) (*crypter.DerivedKey, error) {
	if em == nil {
		return nil, status.FailedPreconditionError("encryption metadata cannot be nil")
	}
	return c.loadKey(ctx, em)
}
