package auth_service

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testgrpc"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/golang-jwt/jwt/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"

	authpb "github.com/buildbuddy-io/buildbuddy/proto/auth"
)

type rsaKeyPair struct {
	privateKeyPEM string
	publicKeyPEM  string
}

func generateRSAKeyPair(t *testing.T) *rsaKeyPair {
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	privateKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(privateKey),
	})

	publicKeyBytes, err := x509.MarshalPKIXPublicKey(&privateKey.PublicKey)
	require.NoError(t, err)
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	return &rsaKeyPair{
		privateKeyPEM: string(privateKeyPEM),
		publicKeyPEM:  string(publicKeyPEM),
	}
}

func contextWithApiKey(t *testing.T, key string) context.Context {
	ctx := metadata.AppendToOutgoingContext(t.Context(), authutil.APIKeyHeader, key)
	return testgrpc.OutgoingToIncomingContext(t, ctx)
}

func TestAuthenticateNoCreds(t *testing.T) {
	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}
	_, err := service.Authenticate(t.Context(), &authpb.AuthenticateRequest{})
	assert.True(t, status.IsUnauthenticatedError(err))
}

func TestAuthenticate(t *testing.T) {
	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}
	resp, err := service.Authenticate(contextWithApiKey(t, "foo"), &authpb.AuthenticateRequest{})
	assert.NoError(t, err)
	assert.NotEqual(t, 0, len(*resp.Jwt))
}

func TestAuthenticate_HS256SigningMethod(t *testing.T) {
	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}
	resp, err := service.Authenticate(contextWithApiKey(t, "foo"),
		&authpb.AuthenticateRequest{})
	assert.NoError(t, err)
	assert.NotEqual(t, 0, len(*resp.Jwt))
	expectedJwt := resp.Jwt

	resp, err = service.Authenticate(contextWithApiKey(t, "foo"),
		&authpb.AuthenticateRequest{
			JwtSigningMethod: authpb.JWTSigningMethod_HS256.Enum(),
		})
	assert.NoError(t, err)
	assert.NotEqual(t, 0, len(*resp.Jwt))
	assert.Equal(t, expectedJwt, resp.Jwt)
}

func TestAuthenticate_RS256SigningMethod(t *testing.T) {
	keyPair := generateRSAKeyPair(t)
	flags.Set(t, "auth.jwt_rsa_private_key", keyPair.privateKeyPEM)
	flags.Set(t, "auth.jwt_rsa_public_key", keyPair.publicKeyPEM)

	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}

	authResp, err := service.Authenticate(contextWithApiKey(t, "foo"),
		&authpb.AuthenticateRequest{
			JwtSigningMethod: authpb.JWTSigningMethod_RS256.Enum(),
		})
	require.NoError(t, err)
	require.NotEmpty(t, authResp.GetJwt())

	// Get the public key and confirm the JWT can be verified with it
	keysResp, err := service.GetPublicKeys(t.Context(), &authpb.GetPublicKeysRequest{})
	require.NoError(t, err)
	require.Len(t, keysResp.PublicKeys, 1)
	rsaPublicKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(keysResp.PublicKeys[0].GetKey()))
	require.NoError(t, err)
	token, err := jwt.Parse(authResp.GetJwt(), func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			t.Fatalf("unexpected signing method: %v", token.Header["alg"])
		}
		return rsaPublicKey, nil
	})
	require.NoError(t, err)
	assert.True(t, token.Valid)
}

func TestAuthenticateWrongCreds(t *testing.T) {
	service := AuthService{authenticator: testauth.NewTestAuthenticator(testauth.TestUsers("foo", "bar"))}
	_, err := service.Authenticate(contextWithApiKey(t, "baz"), &authpb.AuthenticateRequest{})
	assert.True(t, status.IsUnauthenticatedError(err))
}

func TestGetPublicKeys_NoKeys(t *testing.T) {
	service := AuthService{}
	resp, err := service.GetPublicKeys(t.Context(), &authpb.GetPublicKeysRequest{})
	require.NoError(t, err)
	assert.Empty(t, resp.PublicKeys)
}

func TestGetPublicKeys_OnlyOldKey(t *testing.T) {
	flags.Set(t, "auth.jwt_rsa_public_key", "old-public-key")
	service := AuthService{}
	resp, err := service.GetPublicKeys(t.Context(), &authpb.GetPublicKeysRequest{})
	require.NoError(t, err)
	require.Len(t, resp.PublicKeys, 1)
	assert.Equal(t, "old-public-key", resp.PublicKeys[0].GetKey())
}

func TestGetPublicKeys_OnlyNewKey(t *testing.T) {
	flags.Set(t, "auth.new_jwt_rsa_public_key", "new-public-key")
	service := AuthService{}
	resp, err := service.GetPublicKeys(t.Context(), &authpb.GetPublicKeysRequest{})
	require.NoError(t, err)
	require.Len(t, resp.PublicKeys, 1)
	assert.Equal(t, "new-public-key", resp.PublicKeys[0].GetKey())
}

func TestGetPublicKeys_BothKeys(t *testing.T) {
	flags.Set(t, "auth.jwt_rsa_public_key", "old-public-key")
	flags.Set(t, "auth.new_jwt_rsa_public_key", "new-public-key")
	service := AuthService{}
	resp, err := service.GetPublicKeys(t.Context(), &authpb.GetPublicKeysRequest{})
	require.NoError(t, err)
	require.Len(t, resp.PublicKeys, 2)
	// New key should come first
	assert.Equal(t, "new-public-key", resp.PublicKeys[0].GetKey())
	assert.Equal(t, "old-public-key", resp.PublicKeys[1].GetKey())
}
