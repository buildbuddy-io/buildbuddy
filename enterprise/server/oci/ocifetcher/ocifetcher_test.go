package ocifetcher_test

import (
	"context"
	"net"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

func TestFetchManifestMetadata_NoAuth(t *testing.T) {
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", nil)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	counter.Reset()
	client := ocifetcher.NewClient(localhostIPs(t), nil)
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.NoError(t, err)

	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                                1,
		http.MethodHead + " /v2/test-image/manifests/latest":    1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

func TestFetchManifestMetadata_WithValidCredentials(t *testing.T) {
	counter := testhttp.NewRequestCounter()
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	reg := testregistry.Run(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, img := reg.PushNamedImage(t, "test-image", creds)
	digest, err := img.Digest()
	require.NoError(t, err)
	size, err := img.Size()
	require.NoError(t, err)
	mediaType, err := img.MediaType()
	require.NoError(t, err)

	counter.Reset()
	client := ocifetcher.NewClient(localhostIPs(t), nil)
	resp, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "testuser",
			Password: "testpass",
		},
	})
	require.NoError(t, err)

	require.Equal(t, digest.String(), resp.GetDigest())
	require.Equal(t, size, resp.GetSize())
	require.Equal(t, string(mediaType), resp.GetMediaType())

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

func TestFetchManifestMetadata_WithInvalidCredentials(t *testing.T) {
	counter := testhttp.NewRequestCounter()
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	reg := testregistry.Run(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, _ := reg.PushNamedImage(t, "test-image", creds)

	counter.Reset()
	client := ocifetcher.NewClient(localhostIPs(t), nil)
	_, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{
		Ref: imageName,
		Credentials: &rgpb.Credentials{
			Username: "wronguser",
			Password: "wrongpass",
		},
	})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

func TestFetchManifestMetadata_MissingCredentials(t *testing.T) {
	counter := testhttp.NewRequestCounter()
	creds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	reg := testregistry.Run(t, testregistry.Opts{
		Creds: creds,
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageName, _ := reg.PushNamedImage(t, "test-image", creds)

	counter.Reset()
	client := ocifetcher.NewClient(localhostIPs(t), nil)
	_, err := client.FetchManifestMetadata(context.Background(), &ofpb.FetchManifestMetadataRequest{Ref: imageName})
	require.Error(t, err)
	require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDenied error, got: %v", err)

	expected := map[string]int{
		http.MethodGet + " /v2/":                             1,
		http.MethodHead + " /v2/test-image/manifests/latest": 1,
	}
	require.Empty(t, cmp.Diff(expected, counter.Snapshot()))
}

func localhostIPs(t *testing.T) []*net.IPNet {
	_, ipv4Net, err := net.ParseCIDR("127.0.0.0/8")
	require.NoError(t, err)
	_, ipv6Net, err := net.ParseCIDR("::1/128")
	require.NoError(t, err)
	return []*net.IPNet{ipv4Net, ipv6Net}
}
