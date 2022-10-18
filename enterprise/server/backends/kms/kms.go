package kms

import (
	"context"
	"flag"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/tink/go/core/registry"
	"github.com/google/tink/go/integration/gcpkms"
	"google.golang.org/api/option"
)

var (
	masterKeyURI       = flag.String("keystore.master_key_uri", "", "The master key URI (see tink docs for example)")
	gcpCredentialsFile = flag.String("keystore.gcp.credentials_file", "", "A path to a gcp JSON credentials file that will be used to authenticate.")
)

type KMS struct {
	client registry.KMSClient
}

func New(ctx context.Context) (*KMS, error) {
	kms := &KMS{}
	var err error
	switch {
	case strings.HasPrefix(*masterKeyURI, "gcp-kms://"):
		err = kms.initGCPClient(ctx)
	case strings.HasPrefix(*masterKeyURI, "aws-kms://"):
		err = status.UnimplementedError("AWS KMS not yet implemented")
	}
	if err != nil {
		return nil, err
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
	opts := make([]option.ClientOption, 0)
	if *gcpCredentialsFile != "" {
		log.Debugf("KMS: using credentials file: %q", *gcpCredentialsFile)
		opts = append(opts, option.WithCredentialsFile(*gcpCredentialsFile))
	}
	client, err := gcpkms.NewClientWithOptions(ctx, *masterKeyURI, opts...)
	if err != nil {
		return err
	}
	registry.RegisterKMSClient(client)
	k.client = client
	return nil
}

func (k *KMS) FetchMasterKey() (interfaces.AEAD, error) {
	return k.client.GetAEAD(*masterKeyURI)
}
