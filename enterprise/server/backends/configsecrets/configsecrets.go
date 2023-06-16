// Package configsecrets adds external secret support to configs.
// Any placeholders in a loaded config in format ${SECRET:foo} are substituted
// with the contents of the secret "foo" retrieved from the configured secrets
// provider.
package configsecrets

import (
	"context"
	"fmt"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
)

var (
	// Note that all the flags here must be set on the command line and not via
	// a config file since config file parsing depends on secret integration
	// being already configured.

	configSecretProvider = flag.String("config_secrets.provider", "", "Secrets provider to use for config variable substitution. Currently only 'gcp' is supported.", flag.YAMLIgnore)

	// GCP flags.
	configSecretsGCPProject         = flag.String("config_secrets.gcp.project_id", "", "GCP project from which secrets will be loaded.", flag.YAMLIgnore)
	configSecretsGCPCredentialsFile = flag.String("config_secrets.gcp.credentials_file", "", "Credentials to use when communicating with the secrets store. If not specified, Application Default Credentials are used.", flag.YAMLIgnore)
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

		var credOption option.ClientOption
		if *configSecretsGCPCredentialsFile != "" {
			credOption = option.WithCredentialsFile(*configSecretsGCPCredentialsFile)
		} else {
			creds, err := google.FindDefaultCredentials(context.Background())
			if err != nil {
				return err
			}
			credOption = option.WithCredentials(creds)
		}
		client, err := secretmanager.NewClient(context.Background(), credOption)
		if err != nil {
			return err
		}
		provider = &gcpProvider{client: client}
	} else {
		return status.InvalidArgumentErrorf("unknown config secrets provider %q", *configSecretProvider)
	}

	config.SecretProvider = provider

	return nil
}
