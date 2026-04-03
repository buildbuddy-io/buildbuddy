package secrets

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/nacl/box"
	"google.golang.org/grpc"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	skpb "github.com/buildbuddy-io/buildbuddy/proto/secrets"
)

type fakeBBClient struct {
	bbspb.BuildBuddyServiceClient

	publicKey string
	secrets   map[string]string
}

func (c *fakeBBClient) GetPublicKey(ctx context.Context, req *skpb.GetPublicKeyRequest, opts ...grpc.CallOption) (*skpb.GetPublicKeyResponse, error) {
	return &skpb.GetPublicKeyResponse{
		PublicKey: &skpb.PublicKey{Value: c.publicKey},
	}, nil
}

func (c *fakeBBClient) ListSecrets(ctx context.Context, req *skpb.ListSecretsRequest, opts ...grpc.CallOption) (*skpb.ListSecretsResponse, error) {
	names := make([]string, 0, len(c.secrets))
	for name := range c.secrets {
		names = append(names, name)
	}

	rsp := &skpb.ListSecretsResponse{}
	for _, name := range names {
		rsp.Secret = append(rsp.Secret, &skpb.Secret{Name: name})
	}
	return rsp, nil
}

func (c *fakeBBClient) UpdateSecret(ctx context.Context, req *skpb.UpdateSecretRequest, opts ...grpc.CallOption) (*skpb.UpdateSecretResponse, error) {
	if c.secrets == nil {
		c.secrets = make(map[string]string)
	}
	c.secrets[req.GetSecret().GetName()] = req.GetSecret().GetValue()
	return &skpb.UpdateSecretResponse{}, nil
}

func (c *fakeBBClient) DeleteSecret(ctx context.Context, req *skpb.DeleteSecretRequest, opts ...grpc.CallOption) (*skpb.DeleteSecretResponse, error) {
	delete(c.secrets, req.GetSecret().GetName())
	return &skpb.DeleteSecretResponse{}, nil
}

func TestRunSubcommandCreateEncryptsAndStoresSecret(t *testing.T) {
	pub, priv, err := box.GenerateKey(rand.Reader)
	require.NoError(t, err)

	client := &fakeBBClient{
		publicKey: base64.StdEncoding.EncodeToString(pub[:]),
		secrets:   map[string]string{},
	}
	secretPath := t.TempDir() + "/secret.txt"
	require.NoError(t, os.WriteFile(secretPath, []byte("super-secret"), 0600))

	var out bytes.Buffer
	err = runSubcommand(context.Background(), client, &out, "create", []string{"API_TOKEN", "--file=" + secretPath})
	require.NoError(t, err)
	require.Equal(t, "Created secret \"API_TOKEN\"\n", out.String())

	ciphertext := client.secrets["API_TOKEN"]
	require.NotEmpty(t, ciphertext)
	require.NotEqual(t, "super-secret", ciphertext)

	rawCiphertext, err := base64.StdEncoding.DecodeString(ciphertext)
	require.NoError(t, err)
	plaintext, ok := box.OpenAnonymous(nil, rawCiphertext, pub, priv)
	require.True(t, ok)
	require.Equal(t, "super-secret", string(plaintext))
}

func TestRunSubcommandCreateRequiresDataFile(t *testing.T) {
	client := &fakeBBClient{
		secrets: map[string]string{},
	}
	err := runSubcommand(context.Background(), client, &bytes.Buffer{}, "create", []string{"API_TOKEN", "super-secret"})
	require.ErrorContains(t, err, "--file")
}

func TestRunSubcommandUpdateRequiresExistingSecret(t *testing.T) {
	pub, _, err := box.GenerateKey(rand.Reader)
	require.NoError(t, err)

	client := &fakeBBClient{
		publicKey: base64.StdEncoding.EncodeToString(pub[:]),
		secrets:   map[string]string{},
	}
	secretPath := t.TempDir() + "/update-secret.txt"
	require.NoError(t, os.WriteFile(secretPath, []byte("new-value"), 0600))

	err = runSubcommand(context.Background(), client, &bytes.Buffer{}, "update", []string{"MISSING", "--file=" + secretPath})
	require.ErrorContains(t, err, "not found")

	client.secrets["EXISTING"] = "previous"
	err = runSubcommand(context.Background(), client, &bytes.Buffer{}, "update", []string{"EXISTING", "--file=" + secretPath})
	require.NoError(t, err)
	require.NotEqual(t, "previous", client.secrets["EXISTING"])
}

func TestRunSubcommandUpdateRequiresDataFile(t *testing.T) {
	client := &fakeBBClient{
		secrets: map[string]string{
			"EXISTING": "cipher",
		},
	}
	err := runSubcommand(context.Background(), client, &bytes.Buffer{}, "update", []string{"EXISTING", "new-value"})
	require.ErrorContains(t, err, "--file")
}

func TestRunSubcommandListAndDelete(t *testing.T) {
	client := &fakeBBClient{
		secrets: map[string]string{
			"B_SECRET": "cipher-b",
			"A_SECRET": "cipher-a",
		},
	}

	var listOut bytes.Buffer
	err := runSubcommand(context.Background(), client, &listOut, "list", nil)
	require.NoError(t, err)
	require.Equal(t, "A_SECRET\nB_SECRET\n", listOut.String())

	var deleteOut bytes.Buffer
	err = runSubcommand(context.Background(), client, &deleteOut, "delete", []string{"A_SECRET"})
	require.NoError(t, err)
	require.Equal(t, "Deleted secret \"A_SECRET\"\n", deleteOut.String())
	_, ok := client.secrets["A_SECRET"]
	require.False(t, ok)
}
