package configsecrets

import (
	"context"
	"flag"
	"fmt"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil/yaml"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
)

var (
	configSecretProvider = flag.String("config_secrets.provider", "", "Secrets provider to use for config variable substitution. Currently only 'gcp' is supported.")

	configSecretsGCPProject = flag.String("config_secrets.gcp.project_id", "", "GCP project from which secrets will be loaded.")
)

type gcpProvider struct {
	client *secretmanager.Client
}

func (gcp *gcpProvider) GetSecret(ctx context.Context, name string) ([]byte, error) {
	s, err := gcp.client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{
		Name: fmt.Sprintf("projects/%s/secrets/%s/versions/latest", *configSecretsGCPProject, name),
	})
	if err != nil {
		return nil, err
	}
	return s.GetPayload().GetData(), nil
}

func Configure() error {
	if *configSecretProvider == "" {
		return nil
	}

	var provider interfaces.ConfigSecretProvider
	if *configSecretProvider == "gcp" {
		if *configSecretsGCPProject == "" {
			return status.InvalidArgumentErrorf("GCP project not specified for external secrets")
		}

		creds, err := google.FindDefaultCredentials(context.Background())
		if err != nil {
			return err
		}
		client, err := secretmanager.NewClient(context.Background(), option.WithCredentials(creds))
		if err != nil {
			return err
		}
		provider = &gcpProvider{client: client}
	} else {
		return status.InvalidArgumentErrorf("unknown config secrets provider %q", *configSecretProvider)
	}

	yaml.SecretProvider = provider

	return nil
}
