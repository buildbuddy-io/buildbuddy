package kms

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/tink-crypto/tink-go-awskms/v2/integration/awskms"
	"github.com/tink-crypto/tink-go-gcpkms/v2/integration/gcpkms"
	"github.com/tink-crypto/tink-go/v2/core/registry"
	"github.com/tink-crypto/tink-go/v2/tink"
	"google.golang.org/api/option"

	awscreds "github.com/aws/aws-sdk-go-v2/credentials"
	awssdkkms "github.com/aws/aws-sdk-go-v2/service/kms"
)

const (
	gcpKMSPrefix           = "gcp-kms://"
	awsKMSPrefix           = "aws-kms://"
	localInsecureKMSPrefix = "local-insecure-kms://"
)

var (
	masterKeyURI              = flag.String("keystore.master_key_uri", "", "The master key URI (see tink docs for example)")
	enableGCPClient           = flag.Bool("keystore.gcp.enabled", false, "Whether GCP KMS support should be enabled. Implicitly enabled if the master key URI references a GCP KMS URI.")
	gcpCredentialsFile        = flag.String("keystore.gcp.credentials_file", "", "A path to a gcp JSON credentials file that will be used to authenticate.")
	gcpCredentials            = flag.String("keystore.gcp.credentials", "", "GCP JSON credentials that will be used to authenticate.", flag.Secret)
	enableAWSClient           = flag.Bool("keystore.aws.enabled", false, "Whether AWS KMS support should be enabled. Implicitly enabled if the master key URI references an AWS KMS URI.")
	awsCredentialsFile        = flag.String("keystore.aws.credentials_file", "", "A path to a AWS CSV credentials file that will be used to authenticate. If not specified, credentials will be retrieved as described by https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html")
	awsCredentials            = flag.String("keystore.aws.credentials", "", "AWS CSV credentials that will be used to authenticate. If not specified, credentials will be retrieved as described by https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html", flag.Secret)
	localInsecureKMSDirectory = flag.String("keystore.local_insecure_kms_directory", "", "For development only. If set, keys in format local-insecure-kms://[id] are read from this directory.")
)

type KMS struct {
	// May be nil if GCP integration is not enabled.
	gcpClient registry.KMSClient

	// AWS clients are regional and we create them on-demand.
	awsClientsMu sync.Mutex
	// May be nil if AWS integration is not enabled.
	awsClients map[string]registry.KMSClient

	// May be nil if local development integration is not enabled.
	localInsecureKMSClient registry.KMSClient
}

func New(ctx context.Context) (*KMS, error) {
	kms := &KMS{}
	if err := kms.initGCPClient(ctx); err != nil {
		return nil, err
	}
	if err := kms.initAWSClient(ctx); err != nil {
		return nil, err
	}
	if err := kms.initLocalInsecureKMSClient(ctx); err != nil {
		return nil, err
	}
	_, err := kms.clientForURI(ctx, *masterKeyURI)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("master key URI not supported")
	}
	return kms, nil
}

func Register(env *real_environment.RealEnv) error {
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
	if *gcpCredentialsFile != "" && *gcpCredentials != "" {
		return status.FailedPreconditionError("GCP KMS credentials should be specified either via file or directly, but not both")
	}
	if *gcpCredentialsFile != "" {
		log.Debugf("KMS: using GCP credentials file: %q", *gcpCredentialsFile)
		opts = append(opts, option.WithCredentialsFile(*gcpCredentialsFile))
	} else if *gcpCredentials != "" {
		opts = append(opts, option.WithCredentialsJSON([]byte(*gcpCredentials)))
	}
	client, err := gcpkms.NewClientWithOptions(ctx, gcpKMSPrefix, opts...)
	if err != nil {
		return err
	}
	k.gcpClient = client
	return nil
}

type awsKMSARN struct {
	partition string
	region    string
	// full prefix of the URI that includes the partition and the region
	uriLocationPrefix string
}

func parseAWSARN(keyURI string) (*awsKMSARN, error) {
	// http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html.
	re, err := regexp.Compile(`aws-kms://arn:(aws[a-zA-Z0-9-_]*):kms:([a-z0-9-]+):`)
	if err != nil {
		return nil, err
	}
	m := re.FindStringSubmatch(keyURI)
	if len(m) != 3 {
		return nil, status.FailedPreconditionErrorf("could not parse AWS ARN %q", keyURI)
	}
	return &awsKMSARN{
		partition:         m[1],
		region:            m[2],
		uriLocationPrefix: m[0],
	}, nil
}

func (k *KMS) initAWSClient(ctx context.Context) error {
	if !*enableAWSClient && !strings.HasPrefix(*masterKeyURI, awsKMSPrefix) {
		return nil
	}

	k.awsClients = make(map[string]registry.KMSClient)
	if *awsCredentialsFile != "" && *awsCredentials != "" {
		return status.FailedPreconditionError("AWS KMS credentials should be specified either via file or directly, but not both")
	}
	if *awsCredentialsFile != "" || *awsCredentials != "" {
		if *awsCredentialsFile != "" {
			log.Debugf("KMS: using AWS credentials file: %q", *awsCredentialsFile)
		}
		// Verify the credential file format is valid.
		_, err := loadAWSCreds()
		if err != nil {
			return status.FailedPreconditionErrorf("AWS credentials file not valid: %s", err)
		}
	}
	if strings.HasPrefix(*masterKeyURI, awsKMSPrefix) {
		_, err := k.clientForURI(ctx, *masterKeyURI)
		return status.FailedPreconditionErrorf("could not initialize KMS client for master key: %s", err)
	}
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

func loadAWSCreds() (*aws.Credentials, error) {
	var credsData []byte
	if *awsCredentials != "" {
		credsData = []byte(*awsCredentials)
	} else {
		data, err := os.ReadFile(*awsCredentialsFile)
		if err != nil {
			return nil, err
		}
		credsData = data
	}
	rs, err := csv.NewReader(bytes.NewReader(credsData)).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rs) != 2 {
		return nil, status.FailedPreconditionErrorf("credentials file not in valid format (expected 2 rows, got %d)", len(rs))
	}
	if len(rs[1]) != 2 {
		return nil, status.FailedPreconditionErrorf("credential file not in valid format (expected 2 columns, got %d", len(rs[0]))
	}
	return &aws.Credentials{
		AccessKeyID:     rs[1][0],
		SecretAccessKey: rs[1][1],
	}, nil
}

// For some reason, the tink library does not support aws credential files with
// 2 columns so we manually create the AWS SDK client.
func createAWSClientWithCreds(ctx context.Context, arn *awsKMSARN) (registry.KMSClient, error) {
	credValue, err := loadAWSCreds()
	if err != nil {
		return nil, status.UnknownErrorf("could not load credentials file: %s", err)
	}
	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(arn.region),
		config.WithCredentialsProvider(
			awscreds.StaticCredentialsProvider{Value: *credValue},
		),
	)
	if err != nil {
		return nil, status.UnknownErrorf("could not create config: %s", err)
	}
	return awskms.NewClientWithKMS(ctx, arn.uriLocationPrefix, awssdkkms.NewFromConfig(cfg))
}

func (k *KMS) clientForURI(ctx context.Context, uri string) (registry.KMSClient, error) {
	if strings.HasPrefix(uri, gcpKMSPrefix) && k.gcpClient != nil {
		return k.gcpClient, nil
	} else if strings.HasPrefix(uri, awsKMSPrefix) && k.awsClients != nil {
		arn, err := parseAWSARN(uri)
		if err != nil {
			return nil, err
		}
		k.awsClientsMu.Lock()
		defer k.awsClientsMu.Unlock()
		key := fmt.Sprintf("%s-%s", arn.partition, arn.region)
		client, ok := k.awsClients[key]
		if ok {
			return client, nil
		}
		if *awsCredentialsFile != "" || *awsCredentials != "" {
			client, err = createAWSClientWithCreds(ctx, arn)
		} else {
			client, err = awskms.NewClientWithOptions(ctx, arn.uriLocationPrefix)
		}
		if err != nil {
			return nil, status.FailedPreconditionErrorf("could not create AWS KMS client: %s", err)
		}
		k.awsClients[key] = client
		return client, nil
	} else if strings.HasPrefix(uri, localInsecureKMSPrefix) && k.localInsecureKMSClient != nil {
		return k.localInsecureKMSClient, nil
	}
	log.Warningf("no matching client for URI %q", uri)
	return nil, status.InvalidArgumentError("no matching client for key URI")
}

func (k *KMS) FetchMasterKey(ctx context.Context) (interfaces.AEAD, error) {
	return k.FetchKey(ctx, *masterKeyURI)
}

func (k *KMS) FetchKey(ctx context.Context, uri string) (interfaces.AEAD, error) {
	c, err := k.clientForURI(ctx, uri)
	if err != nil {
		return nil, status.NotFoundErrorf("no handler available for KMS URI: %s", err)
	}
	return c.GetAEAD(uri)
}

func (k *KMS) SupportedTypes() []interfaces.KMSType {
	types := make([]interfaces.KMSType, 0)
	if k.localInsecureKMSClient != nil {
		types = append(types, interfaces.KMSTypeLocalInsecure)
	}
	if k.gcpClient != nil {
		types = append(types, interfaces.KMSTypeGCP)
	}
	if k.awsClients != nil {
		types = append(types, interfaces.KMSTypeAWS)
	}
	return types
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
