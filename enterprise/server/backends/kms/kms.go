package kms

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/tink/go/core/registry"
	"github.com/google/tink/go/integration/awskms"
	"github.com/google/tink/go/integration/gcpkms"
	"github.com/google/tink/go/tink"
	"google.golang.org/api/option"

	awscreds "github.com/aws/aws-sdk-go/aws/credentials"
	awssession "github.com/aws/aws-sdk-go/aws/session"
	awssdkkms "github.com/aws/aws-sdk-go/service/kms"
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
	enableAWSClient           = flag.Bool("keystore.aws.enabled", false, "Whether AWS KMS support should be enabled. Implicitly enabled if the master key URI references an AWS KMS URI.")
	awsCredentialsFile        = flag.String("keystore.aws.credentials_file", "", "A path to a AWS CSV credentials file that will be used to authenticate. If not specified, credentials will be retrieved as described by https://docs.aws.amazon.com/sdkref/latest/guide/standardized-credentials.html")
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
		log.Debugf("KMS: using GCP credentials file: %q", *gcpCredentialsFile)
		opts = append(opts, option.WithCredentialsFile(*gcpCredentialsFile))
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
	if *awsCredentialsFile != "" {
		log.Debugf("KMS: using AWS credentials file: %q", *awsCredentialsFile)
		// Verify the credential file format is valid.
		_, err := loadAWSCreds(*awsCredentialsFile)
		if err != nil {
			return status.FailedPreconditionErrorf("AWS credentials file not valid: %s", err)
		}
	}
	if strings.HasPrefix(*masterKeyURI, awsKMSPrefix) {
		_, err := k.clientForURI(*masterKeyURI)
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

func loadAWSCreds(file string) (*awscreds.Value, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	rs, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rs) != 2 {
		return nil, status.FailedPreconditionErrorf("credentials file not in valid format (expected 2 rows, got %d)", len(rs))
	}
	if len(rs[1]) != 2 {
		return nil, status.FailedPreconditionErrorf("credential file not in valid format (expected 2 columns, got %d", len(rs[0]))
	}
	return &awscreds.Value{
		AccessKeyID:     rs[1][0],
		SecretAccessKey: rs[1][1],
	}, nil
}

// For some reason, the tink library does not support aws credential files with
// 2 columns so we manually create the AWS SDK client.
func createAWSClientWithCreds(arn *awsKMSARN, credsFile string) (registry.KMSClient, error) {
	credValue, err := loadAWSCreds(credsFile)
	if err != nil {
		return nil, status.UnknownErrorf("could not load credentials file: %s", err)
	}
	creds := awscreds.NewStaticCredentialsFromCreds(*credValue)
	sess, err := awssession.NewSession(&aws.Config{
		Credentials: creds,
		Region:      aws.String(arn.region),
	})
	if err != nil {
		return nil, status.UnknownErrorf("could not create session: %s", err)
	}
	return awskms.NewClientWithKMS(arn.uriLocationPrefix, awssdkkms.New(sess))
}

func (k *KMS) clientForURI(uri string) (registry.KMSClient, error) {
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
		if *awsCredentialsFile != "" {
			client, err = createAWSClientWithCreds(arn, *awsCredentialsFile)
		} else {
			client, err = awskms.NewClient(arn.uriLocationPrefix)
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

func (k *KMS) FetchMasterKey() (interfaces.AEAD, error) {
	return k.FetchKey(*masterKeyURI)
}

func (k *KMS) FetchKey(uri string) (interfaces.AEAD, error) {
	c, err := k.clientForURI(uri)
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
