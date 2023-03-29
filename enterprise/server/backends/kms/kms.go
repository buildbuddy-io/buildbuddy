package kms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"flag"
	"os"
	"path/filepath"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/tink/go/core/registry"
	"github.com/google/tink/go/integration/gcpkms"
	"github.com/google/tink/go/tink"
	"google.golang.org/api/option"
)

const (
	gcpKMSPrefix           = "gcp-kms://"
	localInsecureKMSPrefix = "local-insecure-kms://"
)

var (
	masterKeyURI              = flag.String("keystore.master_key_uri", "", "The master key URI (see tink docs for example)")
	enableGCPClient           = flag.Bool("keystore.gcp.enabled", false, "Whether GCP KMS support should be enabled. Implicitly enabled if the master key URI references a GCP KMS URI.")
	gcpCredentialsFile        = flag.String("keystore.gcp.credentials_file", "", "A path to a gcp JSON credentials file that will be used to authenticate.")
	localInsecureKMSDirectory = flag.String("keystore.local_insecure_kms_directory", "", "For development only. If set, keys in format local-insecure-kms://[id] are read from this directory.")
)

type KMS struct {
	gcpClient              registry.KMSClient
	localInsecureKMSClient registry.KMSClient
}

func New(ctx context.Context) (*KMS, error) {
	kms := &KMS{}
	if err := kms.initGCPClient(ctx); err != nil {
		return nil, err
	}
	if err := kms.initLocalInsecureKMSClient(ctx); err != nil {
		return nil, err
	}
	_, err := kms.clientForURI(*masterKeyURI)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("master key URI not supported")
	}
	return kms, nil
}

func Register(env environment.Env) error {
	if *masterKeyURI == "" {
		return nil
	}
	kms, err := New(context.TODO())
	if err != nil {
		return err
	}
	env.SetKMS(kms)
	return nil
}

func (k *KMS) initGCPClient(ctx context.Context) error {
	if !*enableGCPClient && !strings.HasPrefix(*masterKeyURI, gcpKMSPrefix) {
		return nil
	}

	opts := make([]option.ClientOption, 0)
	if *gcpCredentialsFile != "" {
		log.Debugf("KMS: using credentials file: %q", *gcpCredentialsFile)
		opts = append(opts, option.WithCredentialsFile(*gcpCredentialsFile))
	}
	client, err := gcpkms.NewClientWithOptions(ctx, gcpKMSPrefix, opts...)
	if err != nil {
		return err
	}
	k.gcpClient = client
	return nil
}

func (k *KMS) initLocalInsecureKMSClient(ctx context.Context) error {
	if *localInsecureKMSDirectory == "" {
		return nil
	}
	client := &LocalInsecureKMS{root: *localInsecureKMSDirectory}
	k.localInsecureKMSClient = client
	return nil
}

func (k *KMS) clientForURI(uri string) (registry.KMSClient, error) {
	if strings.HasPrefix(uri, gcpKMSPrefix) && k.gcpClient != nil {
		return k.gcpClient, nil
	} else if strings.HasPrefix(uri, localInsecureKMSPrefix) && k.localInsecureKMSClient != nil {
		return k.localInsecureKMSClient, nil
	}
	log.Warningf("no matching client for URI %q", uri)
	return nil, status.InvalidArgumentError("no matching client for key URI")
}

func (k *KMS) FetchMasterKey() (interfaces.AEAD, error) {
	return k.FetchKey(*masterKeyURI)
}

func (k *KMS) FetchKey(uri string) (interfaces.AEAD, error) {
	c, err := k.clientForURI(uri)
	if err != nil {
		return nil, status.NotFoundErrorf("no handler available for KMS URI")
	}
	return c.GetAEAD(uri)
}

type gcmAESAEAD struct {
	ciph cipher.AEAD
}

func (g *gcmAESAEAD) Encrypt(plaintext, associatedData []byte) ([]byte, error) {
	nonce := make([]byte, g.ciph.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	out := g.ciph.Seal(nil, nonce, plaintext, associatedData)
	return append(nonce, out...), nil
}

func (g *gcmAESAEAD) Decrypt(ciphertext, associatedData []byte) ([]byte, error) {
	if len(ciphertext) < g.ciph.NonceSize() {
		return nil, status.InvalidArgumentErrorf("input ciphertext too short")
	}
	nonce := ciphertext[:g.ciph.NonceSize()]
	return g.ciph.Open(nil, nonce, ciphertext[g.ciph.NonceSize():], associatedData)
}

// LocalInsecureKMS is a KMS client that reads unencrypted keys from a local
// directory.
// Useful for testing encryption related functionality w/o depending on a real
// KMS. Not suitable for production use.
type LocalInsecureKMS struct {
	root string
}

func (l *LocalInsecureKMS) Supported(keyURI string) bool {
	return strings.HasPrefix(keyURI, localInsecureKMSPrefix)
}

func (l *LocalInsecureKMS) GetAEAD(keyURI string) (tink.AEAD, error) {
	id := strings.TrimPrefix(keyURI, localInsecureKMSPrefix)
	path := filepath.Join(l.root, id)
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	bc, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	g, err := cipher.NewGCM(bc)
	if err != nil {
		return nil, err
	}
	return &gcmAESAEAD{g}, nil
}
