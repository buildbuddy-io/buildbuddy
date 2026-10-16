package local_crypter

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/kms"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const (
	keyLengthBytes = 32
)

var (
	localKeyFile    = flag.String("crypter.local.key_file", "", "Path to a file containing a base64-encoded 32-byte encryption key. If set, the cache proxy encrypts data in its local cache for all groups with encryption enabled using this key, instead of keys fetched from the remote crypter backend. This is intended for customer-run cache proxies that cannot fetch customer-managed encryption keys. When changing this key, also increment crypter.local.key_version. Incompatible with crypter.local.kms_key_uri, crypter.remote_target, and setting crypter.allow_local_cache_encryption to false.")
	localKMSKeyURI  = flag.String("crypter.local.kms_key_uri", "", "The URI of a KMS HMAC-SHA256 key (e.g. gcp-kms://projects/.../cryptoKeyVersions/1 or aws-kms://arn:aws:kms:...). If set, the local encryption key is computed at startup as the HMAC of a fixed string that includes crypter.local.key_version, and is otherwise used like crypter.local.key_file. KMS credentials are configured with the keystore.* flags. Incompatible with crypter.local.key_file.")
	localKeyVersion = flag.Int64("crypter.local.key_version", 1, "The version of the local encryption key. Increment this to rotate keys: local cache entries written with other versions are treated as cache misses, re-fetched from the app, and re-encrypted with the current key and version.")
)

func Register(env *real_environment.RealEnv) error {
	if *localKeyFile == "" && *localKMSKeyURI == "" {
		return nil
	}
	if *localKeyFile != "" && *localKMSKeyURI != "" {
		return status.InvalidArgumentError("only one of crypter.local.key_file and crypter.local.kms_key_uri can be set")
	}
	if env.GetCrypter() != nil {
		return status.FailedPreconditionError("a local encryption key is configured, but a crypter is already registered")
	}
	if !authutil.AllowLocalCacheEncryption() {
		return status.InvalidArgumentError("a local encryption key cannot be used when crypter.allow_local_cache_encryption is false")
	}
	var key []byte
	var err error
	if *localKMSKeyURI != "" {
		key, err = keyFromKMS(env.GetServerContext(), *localKMSKeyURI, *localKeyVersion)
	} else {
		key, err = keyFromFile(*localKeyFile)
	}
	if err != nil {
		return err
	}
	c, err := New(env.GetAuthenticator(), key, *localKeyVersion)
	if err != nil {
		return err
	}
	env.SetCrypter(c)
	return nil
}

func keyFromFile(path string) ([]byte, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not read crypter.local.key_file: %s", err)
	}
	key, err := base64.StdEncoding.DecodeString(strings.TrimSpace(string(contents)))
	if err != nil {
		return nil, status.InvalidArgumentErrorf("crypter.local.key_file does not contain valid base64: %s", err)
	}
	return key, nil
}

// keyFromKMS computes the key as an HMAC with a KMS key, so the key is never
// stored and can only be recomputed with access to the KMS key. The version is
// part of the HMAC input so that incrementing it produces a new key. Changing
// this input makes existing local cache entries unreadable.
func keyFromKMS(ctx context.Context, keyURI string, version int64) ([]byte, error) {
	k, err := kms.NewWithKeyURI(ctx, keyURI)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not initialize KMS for crypter.local.kms_key_uri: %s", err)
	}
	key, err := k.MAC(ctx, keyURI, []byte(fmt.Sprintf("buildbuddy-cache-proxy-local-key-v%d", version)))
	if err != nil {
		return nil, status.UnavailableErrorf("could not compute local key with crypter.local.kms_key_uri: %s", err)
	}
	return key, nil
}

type LocalCrypter struct {
	authenticator interfaces.Authenticator
	key           []byte
	metadata      *sgpb.EncryptionMetadata
}

func New(authenticator interfaces.Authenticator, key []byte, version int64) (*LocalCrypter, error) {
	if len(key) != keyLengthBytes {
		return nil, status.InvalidArgumentErrorf("the local encryption key must be %d bytes, got %d", keyLengthBytes, len(key))
	}
	if version < 1 {
		return nil, status.InvalidArgumentErrorf("crypter.local.key_version must be at least 1, got %d", version)
	}
	return &LocalCrypter{
		authenticator: authenticator,
		key:           key,
		// The version is included in the key ID because some caches key entries
		// by key ID alone. Otherwise, entries written with an old version would
		// still be found after a rotation, but couldn't be decrypted.
		metadata: &sgpb.EncryptionMetadata{
			EncryptionKeyId: fmt.Sprintf("local-v%d", version),
			Version:         version,
		},
	}, nil
}

// keyForGroup returns the local key and the caller's group ID. The local key is
// used directly for all groups; the encryptor authenticates each chunk with the
// group ID so groups can't read each other's data. Only the current key version
// is available, so entries written with other versions fail with NotFound and
// are re-fetched.
func (c *LocalCrypter) keyForGroup(ctx context.Context, em *sgpb.EncryptionMetadata) (*crypter.DerivedKey, string, error) {
	if em.GetEncryptionKeyId() != c.metadata.GetEncryptionKeyId() || em.GetVersion() != c.metadata.GetVersion() {
		return nil, "", status.NotFoundErrorf("encryption key %q version %d not available", em.GetEncryptionKeyId(), em.GetVersion())
	}
	u, err := c.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, "", err
	}
	return &crypter.DerivedKey{Key: c.key, Metadata: c.metadata}, u.GetGroupID(), nil
}

func (c *LocalCrypter) SetEncryptionConfig(ctx context.Context, req *enpb.SetEncryptionConfigRequest) (*enpb.SetEncryptionConfigResponse, error) {
	return nil, status.UnimplementedError("LocalCrypter.SetEncryptionConfig() unsupported")
}

func (c *LocalCrypter) GetEncryptionConfig(ctx context.Context, req *enpb.GetEncryptionConfigRequest) (*enpb.GetEncryptionConfigResponse, error) {
	return nil, status.UnimplementedError("LocalCrypter.GetEncryptionConfig() unsupported")
}

func (c *LocalCrypter) ActiveKey(ctx context.Context) (*sgpb.EncryptionMetadata, error) {
	return c.metadata, nil
}

func (c *LocalCrypter) NewEncryptor(ctx context.Context, d *repb.Digest, w interfaces.CommittedWriteCloser, em *sgpb.EncryptionMetadata) (interfaces.Encryptor, error) {
	key, groupID, err := c.keyForGroup(ctx, em)
	if err != nil {
		return nil, err
	}
	return crypter.NewEncryptor(ctx, key, d, w, groupID, crypter.PlainTextChunkSize)
}

func (c *LocalCrypter) NewDecryptor(ctx context.Context, d *repb.Digest, r io.ReadCloser, em *sgpb.EncryptionMetadata) (interfaces.Decryptor, error) {
	key, groupID, err := c.keyForGroup(ctx, em)
	if err != nil {
		return nil, err
	}
	return crypter.NewDecryptor(ctx, key, d, r, groupID, crypter.PlainTextChunkSize)
}

func (c *LocalCrypter) GetEncryptionKey(ctx context.Context, req *enpb.GetEncryptionKeyRequest) (*enpb.GetEncryptionKeyResponse, error) {
	return nil, status.UnimplementedError("LocalCrypter.GetEncryptionKey() unsupported")
}
