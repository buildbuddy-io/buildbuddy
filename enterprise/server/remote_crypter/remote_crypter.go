package remote_crypter

import (
	"context"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_key_cache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jonboulle/clockwork"
	"google.golang.org/grpc"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	target = flag.String("crypter.remote_target", "", "The gRPC target of the remote encryption API.")
)

type RemoteCrypter struct {
	authenticator interfaces.Authenticator
	client        enpb.EncryptionServiceClient
	cache         *crypter_key_cache.KeyCache
}

func Register(env *real_environment.RealEnv) error {
	if *target == "" {
		return nil
	}

	conn, err := grpc_client.DialSimple(*target)
	if err != nil {
		return err
	}
	crypter := New(env, env.GetClock(), conn)
	env.SetCrypter(crypter)
	return nil
}

func New(env environment.Env, clock clockwork.Clock, conn grpc.ClientConnInterface) *RemoteCrypter {
	client := enpb.NewEncryptionServiceClient(conn)
	refreshFn := func(ctx context.Context, ck crypter_key_cache.CacheKey) ([]byte, *sgpb.EncryptionMetadata, error) {
		return refreshKey(ctx, ck, client)
	}

	cache := crypter_key_cache.New(env, refreshFn, clock)
	quitChan := make(chan struct{})
	cache.StartRefresher(quitChan)

	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		close(quitChan)
		return nil
	})

	return &RemoteCrypter{
		authenticator: env.GetAuthenticator(),
		client:        client,
		cache:         cache,
	}
}

func refreshKey(ctx context.Context, ck crypter_key_cache.CacheKey, client enpb.EncryptionServiceClient) ([]byte, *sgpb.EncryptionMetadata, error) {
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
	loadedKey, err := c.cache.EncryptionKey(ctx)
	if err != nil {
		return nil, err
	}
	return loadedKey.Metadata, nil
}

func (c *RemoteCrypter) NewEncryptor(ctx context.Context, d *repb.Digest, w interfaces.CommittedWriteCloser) (interfaces.Encryptor, error) {
	u, err := c.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	loadedKey, err := c.cache.EncryptionKey(ctx)
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
	loadedKey, err := c.cache.DecryptionKey(ctx, em)
	if err != nil {
		return nil, err
	}
	return crypter.NewDecryptor(ctx, loadedKey, d, r, em, u.GetGroupID(), crypter.PlainTextChunkSize)
}

func (c *RemoteCrypter) GetEncryptionKey(ctx context.Context, req *enpb.GetEncryptionKeyRequest) (*enpb.GetEncryptionKeyResponse, error) {
	return nil, status.UnimplementedError("RemoteCrypter.GetEncryptionKey() unsupported")
}
