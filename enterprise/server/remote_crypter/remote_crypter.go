package remote_crypter

import (
	"context"
	"flag"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/jonboulle/clockwork"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	remoteCrypterTarget = flag.String("crypter.remote_target", "", "The gRPC target of the remote encryption API.")
)

const (
	keyErrCacheTime = 10 * time.Second
)

// An implementation of interfaces.Crypter that uses encryption keys fetched
// from a remote backend.
type RemoteCrypter struct {
	authenticator interfaces.Authenticator
	cache         *keyCache
}

func Register(env *real_environment.RealEnv) error {
	crypter := RemoteCrypter{
		authenticator: env.GetAuthenticator(),
		cache: &keyCache{
			authenticator: env.GetAuthenticator(),
			clock:         env.GetClock(),
		},
	}
	env.SetCrypter(&crypter)
	return nil
}

// Note: there are two types of keys in the cache, one with only groupID set
// (encryption) and one with all values set (decryption).
type cacheKey struct {
	groupID string
	keyID   string
	version int
}

func (ck *cacheKey) String() string {
	if ck.keyID == "" {
		return ck.groupID
	} else {
		return fmt.Sprintf("%s/%s/%d", ck.groupID, ck.keyID, ck.version)
	}
}

type cacheEntry struct {
	err error

	mu           sync.Mutex
	keyMetadata  *sgpb.EncryptionMetadata
	derivedKey   []byte
	lastUse      time.Time
	expiresAfter time.Time
}

type keyCache struct {
	authenticator interfaces.Authenticator
	clock         clockwork.Clock
	sf            singleflight.Group[string, *crypter.DerivedKey]

	data sync.Map
}

func (c *keyCache) cacheAdd(ck cacheKey, ce *cacheEntry) {
	ce.lastUse = c.clock.Now()
	c.data.Store(ck, ce)
}

func (c *keyCache) cacheGet(ck cacheKey) (*cacheEntry, bool) {
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

func (c *keyCache) refreshKeySingleAttempt(ctx context.Context, ck cacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
	// Make the RPC here
	return nil, nil, nil
}

func (c *keyCache) refreshKeyWithRetries(ctx context.Context, ck cacheKey, cacheError bool) (*crypter.DerivedKey, error) {
	var lastErr error
	opts := retry.DefaultOptions()
	opts.Clock = c.clock
	retrier := retry.New(ctx, opts)
	for retrier.Next() {
		key, md, err := c.refreshKeySingleAttempt(ctx, ck)
		if err == nil || status.IsNotFoundError(err) {
			return &crypter.DerivedKey{Key: key, Metadata: md}, err
		}
		lastErr = err
	}
	if cacheError {
		c.cacheAdd(ck, &cacheEntry{
			err:          lastErr,
			expiresAfter: c.clock.Now().Add(keyErrCacheTime),
		})
	}
	return nil, status.UnavailableErrorf("exhausted attempts to refresh key, last error: %s", lastErr)
}

func (c *keyCache) refreshKey(ctx context.Context, ck cacheKey, cacheError bool) (*crypter.DerivedKey, error) {
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

func (c *keyCache) loadKey(ctx context.Context, em *sgpb.EncryptionMetadata) (*crypter.DerivedKey, error) {
	u, err := c.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	var ck cacheKey
	if em != nil {
		if em.GetEncryptionKeyId() == "" {
			return nil, status.FailedPreconditionError("metadata does not contain a valid key ID")
		}
		if em.GetVersion() == 0 {
			return nil, status.FailedPreconditionError("metadata does not contain a valid key version")
		}
		ck = cacheKey{groupID: u.GetGroupID(), keyID: em.GetEncryptionKeyId(), version: int(em.GetVersion())}
	} else {
		ck = cacheKey{groupID: u.GetGroupID()}
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

func (c *keyCache) encryptionKey(ctx context.Context) (*crypter.DerivedKey, error) {
	return c.loadKey(ctx, nil)
}

func (c *keyCache) decryptionKey(ctx context.Context, em *sgpb.EncryptionMetadata) (*crypter.DerivedKey, error) {
	if em == nil {
		return nil, status.FailedPreconditionError("encryption metadata cannot be nil")
	}
	return c.loadKey(ctx, em)
}

func (c *RemoteCrypter) SetEncryptionConfig(ctx context.Context, req *enpb.SetEncryptionConfigRequest) (*enpb.SetEncryptionConfigResponse, error) {
	return nil, status.UnimplementedError("RemoteCrypter.SetEncryptionConfig() is unsupported")
}

func (c *RemoteCrypter) GetEncryptionConfig(ctx context.Context, req *enpb.GetEncryptionConfigRequest) (*enpb.GetEncryptionConfigResponse, error) {
	return nil, status.UnimplementedError("RemoteCrypter.GetEncryptionConfig() is unsupported")
}

func (c *RemoteCrypter) ActiveKey(ctx context.Context) (*sgpb.EncryptionMetadata, error) {
	loadedKey, err := c.cache.encryptionKey(ctx)
	if err != nil {
		return nil, err
	}
	return loadedKey.Metadata, nil
}

func (c *RemoteCrypter) NewEncryptor(ctx context.Context, digest *repb.Digest, w interfaces.CommittedWriteCloser) (interfaces.Encryptor, error) {
	u, err := c.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	loadedKey, err := c.cache.encryptionKey(ctx)
	if err != nil {
		return nil, err
	}
	return crypter.NewEncryptor(ctx, loadedKey, digest, w, u.GetGroupID(), crypter.PlainTextChunkSize)
}

func (c *RemoteCrypter) NewDecryptor(ctx context.Context, d *repb.Digest, r io.ReadCloser, em *sgpb.EncryptionMetadata) (interfaces.Decryptor, error) {
	return nil, nil
}
