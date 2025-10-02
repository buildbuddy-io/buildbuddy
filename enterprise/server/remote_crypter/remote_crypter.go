package remote_crypter

import (
	"context"
	"flag"
	"io"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/crypter_key_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	enpb "github.com/buildbuddy-io/buildbuddy/proto/encryption"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sgpb "github.com/buildbuddy-io/buildbuddy/proto/storage"
)

var (
	target = flag.String("crypter.remote_target", "", "The gRPC target of the remote encryption API.")
)

type RemoteCrypter struct {
	cache *crypter_key_cache.KeyCache
}

func Register(env *real_environment.RealEnv) {
	crypter := &RemoteCrypter{}
	env.SetCrypter(crypter)
}

func (c *RemoteCrypter) SetEncryptionConfig(ctx context.Context, req *enpb.SetEncryptionConfigRequest) (*enpb.SetEncryptionConfigResponse, error) {
	return nil, status.UnimplementedError("RemoteCrypter.SetEncryptionConfig unsupported")
}

func (c *RemoteCrypter) GetEncryptionConfig(ctx context.Context, req *enpb.GetEncryptionConfigRequest) (*enpb.GetEncryptionConfigResponse, error) {
	return nil, status.UnimplementedError("RemoteCrypter.GetEncryptionConfig unsupported")
}

func (c *RemoteCrypter) ActiveKey(ctx context.Context) (*sgpb.EncryptionMetadata, error) {
	return nil, status.UnimplementedError("unimplemented")
}

func (c *RemoteCrypter) NewEncryptor(ctx context.Context, d *repb.Digest, w interfaces.CommittedWriteCloser) (interfaces.Encryptor, error) {
	return nil, status.UnimplementedError("unimplemented")
}

func (c *RemoteCrypter) NewDecryptor(ctx context.Context, d *repb.Digest, r io.ReadCloser, em *sgpb.EncryptionMetadata) (interfaces.Decryptor, error) {
	return nil, status.UnimplementedError("unimplemented")
}

func (c *RemoteCrypter) GetEncryptionKey(ctx context.Context, req *enpb.GetEncryptionKeyRequest) (*enpb.GetEncryptionKeyResponse, error) {
	return nil, status.UnimplementedError("unimplemented")
}
