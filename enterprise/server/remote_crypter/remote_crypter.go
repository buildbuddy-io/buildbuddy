package remote_crypter

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/clientidentity"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_key_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"golang.org/x/crypto/hkdf"
	"google.golang.org/grpc"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

const (
	remoteEncryptionEnabled = "crypter.remote_encryption_enabled"

	localKeySize = 32
	localKeyID   = "local"
)

var (
	target          = flag.String("crypter.remote_target", "", "The gRPC target of the remote encryption API.")
	localKey        = flag.String("crypter.local_key", "", "A base64-encoded 32-byte key. If set, the cache proxy encrypts data in its local cache for groups with encryption enabled using keys derived from this one, instead of keys fetched from the app. Intended for customer-run cache proxies that cannot fetch customer-managed encryption keys. When changing this key, also increment crypter.local_key_version. Incompatible with crypter.remote_target and with setting crypter.enable_local_cache_encryption to false.", flag.Secret)
	localKeyVersion = flag.Int64("crypter.local_key_version", 1, "The version of crypter.local_key. Increment this to rotate keys: local cache entries written with other versions are treated as cache misses, re-fetched from the app, and re-encrypted with the current key and version.")
)

type RemoteCrypter struct {
	authenticator         interfaces.Authenticator
	client                enpb.EncryptionServiceClient
	cache                 *crypter_key_cache.KeyCache
	clientIdentityService interfaces.ClientIdentityService
}

func SupportsEncryption(env environment.Env) func(ctx context.Context) bool {
	return func(ctx context.Context) bool {
		if env.GetCrypter() == nil {
			return false
		}
		if *localKey != "" {
			return true
		}
		if env.GetExperimentFlagProvider() == nil {
			return false
		}
		return env.GetExperimentFlagProvider().Boolean(ctx, remoteEncryptionEnabled, false)
	}
}

func Register(env *real_environment.RealEnv) error {
	if *localKey != "" {
		return registerWithLocalKey(env)
	}
	if *target == "" {
		return nil
	}

	// Installing the client identity service in the environment causes it to
	// parse the incoming client identity for all incoming RPCs and set a
	// client identity for all outgoing RPCs. We don't want that in the Proxy,
	// so, create a new client identity service here for populating the client
	// identity just for GetEncryptionKey RPCs.
	clientIdentityService, err := clientidentity.New(env.GetClock())
	if err != nil {
		return err
	}
	conn, err := grpc_client.DialSimpleWithoutPooling(*target)
	if err != nil {
		return err
	}
	crypter := New(env, env.GetAuthenticator(), clientIdentityService, env.GetClock(), conn)
	env.SetCrypter(crypter)
	return nil
}

func registerWithLocalKey(env *real_environment.RealEnv) error {
	if *target != "" {
		return status.InvalidArgumentError("crypter.remote_target and crypter.local_key are mutually exclusive")
	}
	if !authutil.LocalCacheEncryptionEnabled() {
		return status.InvalidArgumentError("crypter.local_key cannot be used when crypter.enable_local_cache_encryption is false")
	}
	key, err := base64.StdEncoding.DecodeString(*localKey)
	if err != nil {
		return status.InvalidArgumentErrorf("crypter.local_key is not valid base64: %s", err)
	}
	if len(key) != localKeySize {
		return status.InvalidArgumentErrorf("crypter.local_key must be %d bytes, got %d", localKeySize, len(key))
	}
	version := *localKeyVersion
	if version < 1 {
		return status.InvalidArgumentErrorf("crypter.local_key_version must be at least 1, got %d", version)
	}
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		return deriveLocalKey(ck, key, version)
	}
	env.SetCrypter(&RemoteCrypter{
		authenticator: env.GetAuthenticator(),
		cache:         crypter_key_cache.New(env, refreshFn, env.GetClock()),
	})
	return nil
}

func New(env environment.Env, authenticator interfaces.Authenticator, clientIdentityService interfaces.ClientIdentityService, clock clockwork.Clock, conn grpc.ClientConnInterface) *RemoteCrypter {
	client := enpb.NewEncryptionServiceClient(conn)
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		return refreshKey(ctx, ck, client, clientIdentityService)
	}

	// Don't start the asynchronous key refresher. Instead, rely on refetching
	// encryption keys synchronously when needed.
	cache := crypter_key_cache.New(env, refreshFn, clock)

	return &RemoteCrypter{
		authenticator: authenticator,
		client:        client,
		cache:         cache,
	}
}

// deriveLocalKey derives the key for the group in ck from the local key and
// version, similarly to how crypter_service derives keys from the master and
// group key portions. Only the current version is available, so entries
// written with other versions fail to decrypt with NotFound and are re-fetched.
func deriveLocalKey(ck crypter_key_cache.CacheKey, localKey []byte, version int64) ([]byte, *sgpb.EncryptionMetadata, error) {
	if ck.KeyID != "" && (ck.KeyID != localKeyID || ck.Version != version) {
		return nil, nil, status.NotFoundErrorf("encryption key %q version %d not available", ck.KeyID, ck.Version)
	}
	info := append([]byte{crypter.EncryptedDataHeaderVersion}, []byte(ck.GroupID)...)
	info = binary.BigEndian.AppendUint64(info, uint64(version))
	derivedKey := make([]byte, localKeySize)
	if _, err := io.ReadFull(hkdf.Expand(sha256.New, localKey, info), derivedKey); err != nil {
		return nil, nil, err
	}
	return derivedKey, &sgpb.EncryptionMetadata{EncryptionKeyId: localKeyID, Version: version}, nil
}

func refreshKey(ctx context.Context, ck crypter_key_cache.CacheKey, client enpb.EncryptionServiceClient, clientIdentityService interfaces.ClientIdentityService) ([]byte, *sgpb.EncryptionMetadata, error) {
	// The GetEncryptionKey RPC is only permitted for certain clients, so we
	// don't want to use the callers identity (or lack thereof) for this RPC.
	// Clear it here and set this server's identity, if present, because there
	// can only be one client identity per RPC.
	ctx = clientidentity.ClearIdentity(ctx)
	ctx, err := clientIdentityService.AddIdentityToContext(ctx)
	if err != nil {
		return nil, nil, err
	}

	req := &enpb.GetEncryptionKeyRequest{}
	if ck.KeyID != "" {
		req = &enpb.GetEncryptionKeyRequest{
			Metadata: &enpb.EncryptionKeyMetadata{
				Id:      ck.KeyID,
				Version: ck.Version,
			},
		}
	}

	resp, err := client.GetEncryptionKey(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	md := &sgpb.EncryptionMetadata{
		EncryptionKeyId: resp.GetKey().GetMetadata().GetId(),
		Version:         resp.GetKey().GetMetadata().GetVersion(),
	}

	return resp.GetKey().GetKey(), md, nil
}

func (c *RemoteCrypter) SetEncryptionConfig(ctx context.Context, req *enpb.SetEncryptionConfigRequest) (*enpb.SetEncryptionConfigResponse, error) {
	return nil, status.UnimplementedError("RemoteCrypter.SetEncryptionConfig() unsupported")
}

func (c *RemoteCrypter) GetEncryptionConfig(ctx context.Context, req *enpb.GetEncryptionConfigRequest) (*enpb.GetEncryptionConfigResponse, error) {
	return nil, status.UnimplementedError("RemoteCrypter.GetEncryptionConfig() unsupported")
}

func (c *RemoteCrypter) ActiveKey(ctx context.Context) (*sgpb.EncryptionMetadata, error) {
	loadedKey, err := c.cache.ActiveEncryptionKey(ctx)
	if err != nil {
		return nil, err
	}
	return loadedKey.Metadata, nil
}

// NewEncryptor creates an encryptor using the specified key metadata.
func (c *RemoteCrypter) NewEncryptor(ctx context.Context, d *repb.Digest, w interfaces.CommittedWriteCloser, em *sgpb.EncryptionMetadata) (interfaces.Encryptor, error) {
	u, err := c.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	loadedKey, err := c.cache.EncryptionKeyForMetadata(ctx, em)
	if err != nil {
		return nil, err
	}
	return crypter.NewEncryptor(ctx, loadedKey, d, w, u.GetGroupID(), crypter.PlainTextChunkSize)
}

func (c *RemoteCrypter) NewDecryptor(ctx context.Context, d *repb.Digest, r io.ReadCloser, em *sgpb.EncryptionMetadata) (interfaces.Decryptor, error) {
	u, err := c.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	loadedKey, err := c.cache.EncryptionKeyForMetadata(ctx, em)
	if err != nil {
		return nil, err
	}
	return crypter.NewDecryptor(ctx, loadedKey, d, r, u.GetGroupID(), crypter.PlainTextChunkSize)
}

func (c *RemoteCrypter) GetEncryptionKey(ctx context.Context, req *enpb.GetEncryptionKeyRequest) (*enpb.GetEncryptionKeyResponse, error) {
	return nil, status.UnimplementedError("RemoteCrypter.GetEncryptionKey() unsupported")
}
