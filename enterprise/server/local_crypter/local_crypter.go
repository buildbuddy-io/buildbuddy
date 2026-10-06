// Package local_crypter provides a crypter that encrypts data using keys
// derived from a key supplied on the command line, rather than keys fetched
// from the app. This lets customer-run cache proxies, which can't fetch
// customer-managed encryption keys, avoid storing data in plaintext.
package local_crypter

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/crypto/hkdf"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const (
	keySize = 32
	keyID   = "local"
)

var (
	localKey        = flag.String("crypter.local_key", "", "A base64-encoded 32-byte key. If set, the cache proxy encrypts data in its local cache for groups with encryption enabled using keys derived from this one, instead of keys fetched from the app. Intended for customer-run cache proxies that cannot fetch customer-managed encryption keys. When changing this key, also increment crypter.local_key_version. Incompatible with crypter.remote_target and with setting crypter.enable_local_cache_encryption to false.", flag.Secret)
	localKeyVersion = flag.Int64("crypter.local_key_version", 1, "The version of crypter.local_key. Increment this to rotate keys: local cache entries written with other versions are treated as cache misses, re-fetched from the app, and re-encrypted with the current key and version.")
)

// Register installs a LocalCrypter in the environment if --crypter.local_key
// is set. It returns an error if the key or version is invalid, if local cache
// encryption is disabled, or if a crypter is already registered.
func Register(env *real_environment.RealEnv) error {
	if *localKey == "" {
		return nil
	}
	if env.GetCrypter() != nil {
		return status.FailedPreconditionError("crypter.local_key is set, but a crypter is already registered")
	}
	if !authutil.LocalCacheEncryptionEnabled() {
		return status.InvalidArgumentError("crypter.local_key cannot be used when crypter.enable_local_cache_encryption is false")
	}
	key, err := base64.StdEncoding.DecodeString(*localKey)
	if err != nil {
		return status.InvalidArgumentErrorf("crypter.local_key is not valid base64: %s", err)
	}
	c, err := New(env.GetAuthenticator(), key, *localKeyVersion)
	if err != nil {
		return err
	}
	env.SetCrypter(c)
	return nil
}

type LocalCrypter struct {
	authenticator interfaces.Authenticator
	key           []byte
	metadata      *sgpb.EncryptionMetadata
}

func New(authenticator interfaces.Authenticator, key []byte, version int64) (*LocalCrypter, error) {
	if len(key) != keySize {
		return nil, status.InvalidArgumentErrorf("crypter.local_key must be %d bytes, got %d", keySize, len(key))
	}
	if version < 1 {
		return nil, status.InvalidArgumentErrorf("crypter.local_key_version must be at least 1, got %d", version)
	}
	return &LocalCrypter{
		authenticator: authenticator,
		key:           key,
		metadata: &sgpb.EncryptionMetadata{
			EncryptionKeyId: keyID,
			Version:         version,
		},
	}, nil
}

// derivedKey returns the key for the caller's group, derived from the local
// key and version, similarly to how crypter_service derives keys from the
// master and group key portions. Only the current version is available, so
// entries written with other versions fail with NotFound and are re-fetched.
func (c *LocalCrypter) derivedKey(ctx context.Context, em *sgpb.EncryptionMetadata) (*crypter.DerivedKey, string, error) {
	if em.GetEncryptionKeyId() != c.metadata.GetEncryptionKeyId() || em.GetVersion() != c.metadata.GetVersion() {
		return nil, "", status.NotFoundErrorf("encryption key %q version %d not available", em.GetEncryptionKeyId(), em.GetVersion())
	}
	u, err := c.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, "", err
	}
	groupID := u.GetGroupID()
	info := append([]byte{crypter.EncryptedDataHeaderVersion}, []byte(groupID)...)
	info = binary.BigEndian.AppendUint64(info, uint64(c.metadata.GetVersion()))
	key := make([]byte, keySize)
	if _, err := io.ReadFull(hkdf.Expand(sha256.New, c.key, info), key); err != nil {
		return nil, "", err
	}
	return &crypter.DerivedKey{Key: key, Metadata: c.metadata.CloneVT()}, groupID, nil
}

func (c *LocalCrypter) SetEncryptionConfig(ctx context.Context, req *enpb.SetEncryptionConfigRequest) (*enpb.SetEncryptionConfigResponse, error) {
	return nil, status.UnimplementedError("LocalCrypter.SetEncryptionConfig() unsupported")
}

func (c *LocalCrypter) GetEncryptionConfig(ctx context.Context, req *enpb.GetEncryptionConfigRequest) (*enpb.GetEncryptionConfigResponse, error) {
	return nil, status.UnimplementedError("LocalCrypter.GetEncryptionConfig() unsupported")
}

func (c *LocalCrypter) ActiveKey(ctx context.Context) (*sgpb.EncryptionMetadata, error) {
	return c.metadata.CloneVT(), nil
}

func (c *LocalCrypter) NewEncryptor(ctx context.Context, d *repb.Digest, w interfaces.CommittedWriteCloser, em *sgpb.EncryptionMetadata) (interfaces.Encryptor, error) {
	key, groupID, err := c.derivedKey(ctx, em)
	if err != nil {
		return nil, err
	}
	return crypter.NewEncryptor(ctx, key, d, w, groupID, crypter.PlainTextChunkSize)
}

func (c *LocalCrypter) NewDecryptor(ctx context.Context, d *repb.Digest, r io.ReadCloser, em *sgpb.EncryptionMetadata) (interfaces.Decryptor, error) {
	key, groupID, err := c.derivedKey(ctx, em)
	if err != nil {
		return nil, err
	}
	return crypter.NewDecryptor(ctx, key, d, r, groupID, crypter.PlainTextChunkSize)
}

func (c *LocalCrypter) GetEncryptionKey(ctx context.Context, req *enpb.GetEncryptionKeyRequest) (*enpb.GetEncryptionKeyResponse, error) {
	return nil, status.UnimplementedError("LocalCrypter.GetEncryptionKey() unsupported")
}
