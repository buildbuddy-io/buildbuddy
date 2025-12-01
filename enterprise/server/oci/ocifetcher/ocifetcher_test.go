package ocifetcher_test

import (
	"context"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

func allowLocalhostIPs(t *testing.T) []*net.IPNet {
	_, ipv4Net, err := net.ParseCIDR("127.0.0.0/8")
	require.NoError(t, err)
	_, ipv6Net, err := net.ParseCIDR("::1/128")
	require.NoError(t, err)
	return []*net.IPNet{ipv4Net, ipv6Net}
}

func TestFetchManifestMetadata_NoAuth(t *testing.T) {
	reg := testregistry.Run(t, testregistry.Opts{})
	imageName, img := reg.PushNamedImage(t, "test-image")

	client := ocifetcher.NewClient(allowLocalhostIPs(t), nil)
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
	})
	require.NoError(t, err)

	digest, err := img.Digest()
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.NotZero(t, resp.GetSize())
	require.NotEmpty(t, resp.GetMediaType())
}

func TestFetchManifestMetadata_WithValidCredentials(t *testing.T) {
	reg := testregistry.Run(t, testregistry.Opts{
		Auth: &testregistry.AuthConfig{
			Username: "testuser",
			Password: "testpass",
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image")

	client := ocifetcher.NewClient(allowLocalhostIPs(t), nil)
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "testuser",
			Password: "testpass",
		},
	})
	require.NoError(t, err)

	digest, err := img.Digest()
	require.NoError(t, err)
	require.Equal(t, digest.String(), resp.GetDigest())
	require.NotZero(t, resp.GetSize())
	require.NotEmpty(t, resp.GetMediaType())
}

func TestFetchManifestMetadata_WithInvalidCredentials(t *testing.T) {
	reg := testregistry.Run(t, testregistry.Opts{
		Auth: &testregistry.AuthConfig{
			Username: "testuser",
			Password: "testpass",
		},
	})
	imageName, _ := reg.PushNamedImage(t, "test-image")

	client := ocifetcher.NewClient(allowLocalhostIPs(t), nil)
	_, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "wronguser",
			Password: "wrongpass",
		},
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)
}

func TestFetchManifestMetadata_MissingCredentials(t *testing.T) {
	reg := testregistry.Run(t, testregistry.Opts{
		Auth: &testregistry.AuthConfig{
			Username: "testuser",
			Password: "testpass",
		},
	})
	imageName, _ := reg.PushNamedImage(t, "test-image")

	client := ocifetcher.NewClient(allowLocalhostIPs(t), nil)
	_, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		// No credentials provided
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)
}
