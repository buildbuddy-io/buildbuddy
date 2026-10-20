package ocifetcher_server_proxy

import (
	"context"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testhttp"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/testing/protocmp"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
	gproto "google.golang.org/protobuf/proto"
)

func TestNew_MissingOCIFetcherClient(t *testing.T) {
	env := testenv.GetTestEnv(t)
	env.SetLocalByteStreamClient(setupLocalBSClient(t))
	// Don't set OCIFetcherClient

	_, err := New(env)
	require.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition, got: %v", err)
}

func TestNew_MissingLocalBSClient(t *testing.T) {
	env := testenv.GetTestEnv(t)
	// Set a dummy OCIFetcherClient but no LocalByteStreamClient
	ctx := context.Background()
	_, bsClient, acClient := setupCacheEnv(t)
	env.SetOCIFetcherClient(runOCIFetcherServer(ctx, t, bsClient, acClient))

	_, err := New(env)
	require.True(t, status.IsFailedPreconditionError(err), "expected FailedPrecondition, got: %v", err)
}

// TestHappyPath tests successful FetchBlob, FetchBlobMetadata, FetchManifest,
// FetchManifestMetadata calls with no credentials and with credentials.
func TestHappyPath(t *testing.T) {
	for _, tc := range []struct {
		name      string
		withCreds bool
	}{
		{"NoCreds", false},
		{"WithCreds", true},
	} {
		t.Run(tc.name+"/FetchManifest", func(t *testing.T) {
			ctx := context.Background()

			var creds *testregistry.BasicAuthCreds
			var reqCreds *rgpb.Credentials
			if tc.withCreds {
				creds = &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
				reqCreds = &rgpb.Credentials{Username: "testuser", Password: "testpass"}
			}

			reg := setupTestRegistry(t, creds)
			imageName, img := reg.PushNamedImage(t, "test-image", creds)
			expectedDigest, expectedSize, expectedMediaType := imageMetadata(t, img)
			expectedManifest, err := img.RawManifest()
			require.NoError(t, err)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			resp, err := proxyClient.FetchManifest(ctx, &ofpb.FetchManifestRequest{
				Ref:         imageName,
				Credentials: reqCreds,
			})

			require.NoError(t, err)
			require.Equal(t, expectedDigest, resp.GetDigest())
			require.Equal(t, expectedSize, resp.GetSize())
			require.Equal(t, expectedMediaType, resp.GetMediaType())
			require.Equal(t, expectedManifest, resp.GetManifest())
		})

		t.Run(tc.name+"/FetchManifestMetadata", func(t *testing.T) {
			ctx := context.Background()

			var creds *testregistry.BasicAuthCreds
			var reqCreds *rgpb.Credentials
			if tc.withCreds {
				creds = &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
				reqCreds = &rgpb.Credentials{Username: "testuser", Password: "testpass"}
			}

			reg := setupTestRegistry(t, creds)
			imageName, img := reg.PushNamedImage(t, "test-image", creds)
			expectedDigest, expectedSize, expectedMediaType := imageMetadata(t, img)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			resp, err := proxyClient.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{
				Ref:         imageName,
				Credentials: reqCreds,
			})

			require.NoError(t, err)
			require.Equal(t, expectedDigest, resp.GetDigest())
			require.Equal(t, expectedSize, resp.GetSize())
			require.Equal(t, expectedMediaType, resp.GetMediaType())
		})

		t.Run(tc.name+"/FetchBlobMetadata", func(t *testing.T) {
			ctx := context.Background()

			var creds *testregistry.BasicAuthCreds
			var reqCreds *rgpb.Credentials
			if tc.withCreds {
				creds = &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
				reqCreds = &rgpb.Credentials{Username: "testuser", Password: "testpass"}
			}

			reg := setupTestRegistry(t, creds)
			imageName, img := reg.PushNamedImage(t, "test-image", creds)

			layers, err := img.Layers()
			require.NoError(t, err)
			require.NotEmpty(t, layers)
			layer := layers[0]
			digest, err := layer.Digest()
			require.NoError(t, err)
			expectedSize, expectedMediaType := layerMetadata(t, layer)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			resp, err := proxyClient.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
				Ref:         imageName + "@" + digest.String(),
				Credentials: reqCreds,
			})

			require.NoError(t, err)
			require.Equal(t, expectedSize, resp.GetSize())
			require.Equal(t, expectedMediaType, resp.GetMediaType())
		})

		t.Run(tc.name+"/FetchBlob", func(t *testing.T) {
			ctx := context.Background()

			var creds *testregistry.BasicAuthCreds
			var reqCreds *rgpb.Credentials
			if tc.withCreds {
				creds = &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
				reqCreds = &rgpb.Credentials{Username: "testuser", Password: "testpass"}
			}

			reg := setupTestRegistry(t, creds)
			imageName, img := reg.PushNamedImage(t, "test-image", creds)

			layers, err := img.Layers()
			require.NoError(t, err)
			require.NotEmpty(t, layers)
			layer := layers[0]
			digest, err := layer.Digest()
			require.NoError(t, err)
			expectedData := layerData(t, layer)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
				Ref:         imageName + "@" + digest.String(),
				Credentials: reqCreds,
			})
			require.NoError(t, err)

			data := collectBlobData(t, stream)
			require.Equal(t, expectedData, data)
		})
	}
}

func TestFetchBlob_ForwardsRequestUnchanged(t *testing.T) {
	const ref = "example.com/repo@sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	blobData := []byte("hello")

	for _, tc := range []struct {
		name      string
		size      *int64
		mediaType *string
	}{
		{name: "NoSizeOrMediaType"},
		{name: "SizeOnly", size: gproto.Int64(12345)},
		{name: "MediaTypeOnly", mediaType: new("application/vnd.example.layer.v1.tar+gzip")},
		{name: "SizeAndMediaType", size: gproto.Int64(12345), mediaType: new("application/vnd.example.layer.v1.tar+gzip")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			remoteClient := &recordingOCIFetcherClient{}
			proxyClient := runOCIFetcherProxy(ctx, t, remoteClient)
			req := &ofpb.FetchBlobRequest{
				Ref:            ref,
				Credentials:    &rgpb.Credentials{Username: "testuser", Password: "testpass"},
				BypassRegistry: false,
				Size:           tc.size,
				MediaType:      tc.mediaType,
			}

			stream, err := proxyClient.FetchBlob(ctx, req)
			require.NoError(t, err)
			require.Equal(t, blobData, collectBlobData(t, stream))
			require.NotNil(t, remoteClient.fetchBlobRequest)
			require.Empty(t, cmp.Diff(req, remoteClient.fetchBlobRequest, protocmp.Transform()))
		})
	}
}

// TestFetchBlob_LocalCacheWriteThrough verifies that after a FetchBlob call,
// the blob is written to the proxy's local BS cache and can be served from
// there on a subsequent request.
func TestFetchBlob_LocalCacheWriteThrough(t *testing.T) {
	ctx := context.Background()

	reg := setupTestRegistry(t, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	digest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)

	_, bsClient, acClient := setupCacheEnv(t)
	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)

	proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

	ref := imageName + "@" + digest.String()

	// First fetch: should go to upstream and write to local cache.
	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{Ref: ref})
	require.NoError(t, err)
	data := collectBlobData(t, stream)
	require.Equal(t, expectedData, data)

	// Shut down the test registry so upstream cannot serve blobs.
	err = reg.Shutdown()
	require.NoError(t, err)

	// Second fetch: access was proven on the first fetch, so this is served
	// from the local BS cache without contacting the (now-stopped) registry.
	stream2, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{Ref: ref})
	require.NoError(t, err)
	data2 := collectBlobData(t, stream2)
	require.Equal(t, expectedData, data2)
}

// TestBypassRegistry tests the bypass_registry flag crossed with server admin claims
// for all Fetch methods.
func TestBypassRegistry(t *testing.T) {
	const adminGroupID = "GR123"

	adminUser := &claims.Claims{
		UserID:        "US1",
		GroupID:       adminGroupID,
		AllowedGroups: []string{adminGroupID},
		GroupMemberships: []*interfaces.GroupMembership{
			{
				GroupID:      adminGroupID,
				Capabilities: []cappb.Capability{cappb.Capability_ORG_ADMIN},
			},
		},
	}

	for _, tc := range []struct {
		name           string
		bypassRegistry bool
		user           *claims.Claims
		checkError     func(error) bool
	}{
		{
			name:           "BypassTrue_Admin",
			bypassRegistry: true,
			user:           adminUser,
			checkError:     status.IsNotFoundError, // Cache miss returns NotFound when bypassing
		},
		{
			name:           "BypassTrue_NonAdmin",
			bypassRegistry: true,
			user:           nil,
			checkError:     status.IsPermissionDeniedError,
		},
		{
			name:           "BypassFalse_Admin",
			bypassRegistry: false,
			user:           adminUser,
			checkError:     nil, // Should succeed
		},
		{
			name:           "BypassFalse_NonAdmin",
			bypassRegistry: false,
			user:           nil,
			checkError:     nil, // Should succeed
		},
	} {
		// FetchManifestMetadata - bypass_registry always returns NotFound even for admins
		t.Run("FetchManifestMetadata/"+tc.name, func(t *testing.T) {
			flags.Set(t, "auth.admin_group_id", adminGroupID)

			ctx := context.Background()
			if tc.user != nil {
				ctx = testauth.WithAuthenticatedUserInfo(ctx, tc.user)
			}

			reg := setupTestRegistry(t, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", nil)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			resp, err := proxyClient.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{
				Ref:            imageName,
				BypassRegistry: tc.bypassRegistry,
			})

			if tc.bypassRegistry {
				// FetchManifestMetadata always errors with bypass_registry=true
				require.Error(t, err)
				if tc.user != nil {
					// Admin gets NotFound
					require.True(t, status.IsNotFoundError(err), "expected NotFoundError for admin, got: %v", err)
				} else {
					// Non-admin gets PermissionDenied
					require.True(t, status.IsPermissionDeniedError(err), "expected PermissionDeniedError for non-admin, got: %v", err)
				}
			} else {
				// No bypass - should succeed
				require.NoError(t, err)
				expectedDigest, expectedSize, expectedMediaType := imageMetadata(t, img)
				require.Equal(t, expectedDigest, resp.GetDigest())
				require.Equal(t, expectedSize, resp.GetSize())
				require.Equal(t, expectedMediaType, resp.GetMediaType())
			}
		})

		// FetchManifest
		t.Run("FetchManifest/"+tc.name, func(t *testing.T) {
			flags.Set(t, "auth.admin_group_id", adminGroupID)

			ctx := context.Background()
			if tc.user != nil {
				ctx = testauth.WithAuthenticatedUserInfo(ctx, tc.user)
			}

			reg := setupTestRegistry(t, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", nil)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			resp, err := proxyClient.FetchManifest(ctx, &ofpb.FetchManifestRequest{
				Ref:            imageName,
				BypassRegistry: tc.bypassRegistry,
			})

			if tc.checkError != nil {
				require.Error(t, err)
				require.True(t, tc.checkError(err), "unexpected error type: %v", err)
			} else {
				require.NoError(t, err)
				expectedDigest, expectedSize, expectedMediaType := imageMetadata(t, img)
				expectedManifest, err := img.RawManifest()
				require.NoError(t, err)
				require.Equal(t, expectedDigest, resp.GetDigest())
				require.Equal(t, expectedSize, resp.GetSize())
				require.Equal(t, expectedMediaType, resp.GetMediaType())
				require.Equal(t, expectedManifest, resp.GetManifest())
			}
		})

		// FetchBlobMetadata
		t.Run("FetchBlobMetadata/"+tc.name, func(t *testing.T) {
			flags.Set(t, "auth.admin_group_id", adminGroupID)

			ctx := context.Background()
			if tc.user != nil {
				ctx = testauth.WithAuthenticatedUserInfo(ctx, tc.user)
			}

			reg := setupTestRegistry(t, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", nil)

			layers, err := img.Layers()
			require.NoError(t, err)
			require.NotEmpty(t, layers)
			layer := layers[0]
			digest, err := layer.Digest()
			require.NoError(t, err)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			resp, err := proxyClient.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
				Ref:            imageName + "@" + digest.String(),
				BypassRegistry: tc.bypassRegistry,
			})

			if tc.checkError != nil {
				require.Error(t, err)
				require.True(t, tc.checkError(err), "unexpected error type: %v", err)
			} else {
				require.NoError(t, err)
				expectedSize, expectedMediaType := layerMetadata(t, layer)
				require.Equal(t, expectedSize, resp.GetSize())
				require.Equal(t, expectedMediaType, resp.GetMediaType())
			}
		})

		// FetchBlob
		t.Run("FetchBlob/"+tc.name, func(t *testing.T) {
			flags.Set(t, "auth.admin_group_id", adminGroupID)

			ctx := context.Background()
			if tc.user != nil {
				ctx = testauth.WithAuthenticatedUserInfo(ctx, tc.user)
			}

			reg := setupTestRegistry(t, nil)
			imageName, img := reg.PushNamedImage(t, "test-image", nil)

			layers, err := img.Layers()
			require.NoError(t, err)
			require.NotEmpty(t, layers)
			layer := layers[0]
			digest, err := layer.Digest()
			require.NoError(t, err)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
				Ref:            imageName + "@" + digest.String(),
				BypassRegistry: tc.bypassRegistry,
			})

			if tc.checkError != nil {
				if err != nil {
					// Error on initial call
					require.True(t, tc.checkError(err), "unexpected error type: %v", err)
				} else {
					// Error might come during streaming
					_, recvErr := stream.Recv()
					require.Error(t, recvErr)
					require.True(t, tc.checkError(recvErr), "unexpected error type: %v", recvErr)
				}
			} else {
				require.NoError(t, err)
				expectedData := layerData(t, layer)
				data := collectBlobData(t, stream)
				require.Equal(t, expectedData, data)
			}
		})
	}
}

// TestInvalidOrMissingCredentials tests all Fetch methods with invalid and
// missing credentials to verify auth errors are propagated correctly.
func TestInvalidOrMissingCredentials(t *testing.T) {
	for _, tc := range []struct {
		name  string
		creds *rgpb.Credentials // nil for missing, wrong values for invalid
	}{
		{"InvalidCreds", &rgpb.Credentials{Username: "wrong", Password: "wrong"}},
		{"MissingCreds", nil},
	} {
		t.Run(tc.name+"/FetchManifest", func(t *testing.T) {
			ctx := context.Background()

			// Setup registry with basic auth required
			registryCreds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
			reg := setupTestRegistry(t, registryCreds)
			imageName, _ := reg.PushNamedImage(t, "test-image", registryCreds)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			_, err := proxyClient.FetchManifest(ctx, &ofpb.FetchManifestRequest{
				Ref:         imageName,
				Credentials: tc.creds,
			})

			require.Error(t, err)
			require.True(t, status.IsUnauthenticatedError(err), "expected UnauthenticatedError, got: %v", err)
		})

		t.Run(tc.name+"/FetchManifestMetadata", func(t *testing.T) {
			ctx := context.Background()

			// Setup registry with basic auth required
			registryCreds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
			reg := setupTestRegistry(t, registryCreds)
			imageName, _ := reg.PushNamedImage(t, "test-image", registryCreds)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			_, err := proxyClient.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{
				Ref:         imageName,
				Credentials: tc.creds,
			})

			require.Error(t, err)
			require.True(t, status.IsUnauthenticatedError(err), "expected UnauthenticatedError, got: %v", err)
		})

		t.Run(tc.name+"/FetchBlobMetadata", func(t *testing.T) {
			ctx := context.Background()

			// Setup registry with basic auth required
			registryCreds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
			reg := setupTestRegistry(t, registryCreds)
			imageName, img := reg.PushNamedImage(t, "test-image", registryCreds)

			layers, err := img.Layers()
			require.NoError(t, err)
			require.NotEmpty(t, layers)
			layer := layers[0]
			digest, err := layer.Digest()
			require.NoError(t, err)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			_, err = proxyClient.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
				Ref:         imageName + "@" + digest.String(),
				Credentials: tc.creds,
			})

			require.Error(t, err)
			require.True(t, status.IsUnauthenticatedError(err), "expected UnauthenticatedError, got: %v", err)
		})

		t.Run(tc.name+"/FetchBlob", func(t *testing.T) {
			ctx := context.Background()

			// Setup registry with basic auth required
			registryCreds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
			reg := setupTestRegistry(t, registryCreds)
			imageName, img := reg.PushNamedImage(t, "test-image", registryCreds)

			layers, err := img.Layers()
			require.NoError(t, err)
			require.NotEmpty(t, layers)
			layer := layers[0]
			digest, err := layer.Digest()
			require.NoError(t, err)

			_, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

			stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
				Ref:         imageName + "@" + digest.String(),
				Credentials: tc.creds,
			})

			if err != nil {
				// Error on initial call
				require.True(t, status.IsUnauthenticatedError(err), "expected UnauthenticatedError, got: %v", err)
			} else {
				// Error might come during streaming
				_, recvErr := stream.Recv()
				require.Error(t, recvErr)
				require.True(t, status.IsUnauthenticatedError(recvErr), "expected UnauthenticatedError, got: %v", recvErr)
			}
		})
	}
}

func TestPrivateRegistryCacheHitsRequireValidCredentials(t *testing.T) {
	ctx := context.Background()
	registryCreds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	validRequestCreds := &rgpb.Credentials{Username: "testuser", Password: "testpass"}

	reg := setupTestRegistry(t, registryCreds)
	imageName, img := reg.PushNamedImage(t, "test-image", registryCreds)
	manifestDigest, _, _ := imageMetadata(t, img)
	expectedManifest, err := img.RawManifest()
	require.NoError(t, err)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	expectedBlob := layerData(t, layer)

	_, bsClient, acClient := setupCacheEnv(t)
	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
	proxyClient := runOCIFetcherProxy(ctx, t, ociFetcherClient)

	// Populate the upstream OCIFetcher manifest/blob/blob-metadata caches and
	// the proxy's local blob cache using credentials that are valid for the
	// private registry.
	manifestResp, err := proxyClient.FetchManifest(ctx, &ofpb.FetchManifestRequest{
		Ref:         imageName + "@" + manifestDigest,
		Credentials: validRequestCreds,
	})
	require.NoError(t, err)
	require.Equal(t, expectedManifest, manifestResp.GetManifest())

	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref:         imageName + "@" + layerDigest.String(),
		Credentials: validRequestCreds,
	})
	require.NoError(t, err)
	require.Equal(t, expectedBlob, collectBlobData(t, stream))

	for _, tc := range []struct {
		name  string
		creds *rgpb.Credentials
	}{
		{
			name:  "MissingCredentials",
			creds: nil,
		},
		{
			name:  "InvalidCredentials",
			creds: &rgpb.Credentials{Username: "wrong", Password: "wrong"},
		},
	} {
		t.Run("FetchManifestMetadata/"+tc.name, func(t *testing.T) {
			_, err := proxyClient.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{
				Ref:         imageName + "@" + manifestDigest,
				Credentials: tc.creds,
			})
			require.Error(t, err)
			require.True(t, status.IsUnauthenticatedError(err), "FetchManifestMetadata: expected Unauthenticated, got: %v", err)
		})

		t.Run("FetchManifest/"+tc.name, func(t *testing.T) {
			_, err := proxyClient.FetchManifest(ctx, &ofpb.FetchManifestRequest{
				Ref:         imageName + "@" + manifestDigest,
				Credentials: tc.creds,
			})
			require.Error(t, err)
			require.True(t, status.IsUnauthenticatedError(err), "FetchManifest: expected Unauthenticated, got: %v", err)
		})

		t.Run("FetchBlobMetadata/"+tc.name, func(t *testing.T) {
			_, err := proxyClient.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{
				Ref:         imageName + "@" + layerDigest.String(),
				Credentials: tc.creds,
			})
			require.Error(t, err)
			require.True(t, status.IsUnauthenticatedError(err), "FetchBlobMetadata: expected Unauthenticated, got: %v", err)
		})

		t.Run("FetchBlob/"+tc.name, func(t *testing.T) {
			stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
				Ref:         imageName + "@" + layerDigest.String(),
				Credentials: tc.creds,
			})
			if err != nil {
				require.True(t, status.IsUnauthenticatedError(err), "FetchBlob: expected Unauthenticated, got: %v", err)
				return
			}
			_, recvErr := stream.Recv()
			require.Error(t, recvErr)
			require.True(t, status.IsUnauthenticatedError(recvErr), "FetchBlob: expected Unauthenticated, got: %v", recvErr)
		})
	}
}

// TestFetchBlob_Singleflight verifies that concurrent FetchBlob requests for
// the same blob are deduplicated: only one upstream FetchBlob RPC is made and
// all callers receive the correct data.
func TestFetchBlob_Singleflight(t *testing.T) {
	ctx := context.Background()

	reg := setupTestRegistry(t, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	digest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)

	_, bsClient, acClient := setupCacheEnv(t)
	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
	counter := &countingOCIFetcherClient{inner: ociFetcherClient}
	proxyClient := runOCIFetcherProxy(ctx, t, counter)

	ref := imageName + "@" + digest.String()
	const numClients = 5

	var wg sync.WaitGroup
	errs := make([]error, numClients)
	results := make([][]byte, numClients)

	for i := range numClients {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{Ref: ref})
			if err != nil {
				errs[idx] = err
				return
			}
			var data []byte
			for {
				resp, err := stream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					errs[idx] = err
					return
				}
				data = append(data, resp.GetData()...)
			}
			results[idx] = data
		}(i)
	}
	wg.Wait()

	for i := range numClients {
		require.NoError(t, errs[i], "client %d got error", i)
		require.Equal(t, expectedData, results[i], "client %d got wrong data", i)
	}

	require.Equal(t, 1, counter.callCounts()["FetchBlob"],
		"expected exactly 1 upstream FetchBlob call due to singleflight deduplication")
}

// These tests exercise the full chain:
// Test Client -> Proxy -> OCIFetcher Server -> Test Registry
// with real cache infrastructure (ByteStream + ActionCache)

// setupTestRegistry creates a test registry with optional auth.
func setupTestRegistry(t *testing.T, creds *testregistry.BasicAuthCreds) *testregistry.Registry {
	return testregistry.Run(t, testregistry.Opts{
		Creds: creds,
	})
}

// imageMetadata extracts digest, size, and media type from an image.
func imageMetadata(t *testing.T, img ctr.Image) (digest string, size int64, mediaType string) {
	d, err := img.Digest()
	require.NoError(t, err)
	s, err := img.Size()
	require.NoError(t, err)
	m, err := img.MediaType()
	require.NoError(t, err)
	return d.String(), s, string(m)
}

// setupCacheEnv creates ByteStream and ActionCache clients for caching.
func setupCacheEnv(t *testing.T) (*testenv.TestEnv, bspb.ByteStreamClient, repb.ActionCacheClient) {
	te := testenv.GetTestEnv(t)
	enterprise_testenv.AddClientIdentity(t, te, interfaces.ClientIdentityApp)
	_, runServer, localGRPClis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, localGRPClis)
	go runServer()
	return te, te.GetByteStreamClient(), te.GetActionCacheClient()
}

// setupLocalBSClient creates a standalone local BS cache env and returns
// a ByteStream client connected to it.
func setupLocalBSClient(t *testing.T) bspb.ByteStreamClient {
	te := testenv.GetTestEnv(t)
	enterprise_testenv.AddClientIdentity(t, te, interfaces.ClientIdentityApp)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, te)
	testcache.Setup(t, te, lis)
	go runServer()
	return te.GetByteStreamClient()
}

// runOCIFetcherServer creates an OCIFetcher server and returns a client connected to it.
func runOCIFetcherServer(ctx context.Context, t *testing.T, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient) ofpb.OCIFetcherClient {
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.0/8", "::1/128"})
	server, err := ocifetcher.NewServer(bsClient, acClient)
	require.NoError(t, err)

	env := testenv.GetTestEnv(t)
	// Use TestAuthenticator to enable JWT parsing for bypass_registry tests
	env.SetAuthenticator(testauth.NewTestAuthenticator(t, nil))

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	ofpb.RegisterOCIFetcherServer(grpcServer, server)
	go runFunc()
	t.Cleanup(func() { grpcServer.GracefulStop() })

	conn, err := testenv.LocalGRPCConn(t, ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return ofpb.NewOCIFetcherClient(conn)
}

// runOCIFetcherProxy sets up the proxy server connecting to the remote client
// and returns a client connected to the proxy.
func runOCIFetcherProxy(ctx context.Context, t *testing.T, remoteClient ofpb.OCIFetcherClient) ofpb.OCIFetcherClient {
	env := testenv.GetTestEnv(t)
	env.SetOCIFetcherClient(remoteClient)
	env.SetLocalByteStreamClient(setupLocalBSClient(t))

	proxy, err := New(env)
	require.NoError(t, err)
	return serveOCIFetcherProxy(ctx, t, env, proxy)
}

// serveOCIFetcherProxy serves proxy and returns a client connected to it.
func serveOCIFetcherProxy(ctx context.Context, t *testing.T, env *testenv.TestEnv, proxy *OCIFetcherServerProxy) ofpb.OCIFetcherClient {
	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	ofpb.RegisterOCIFetcherServer(grpcServer, proxy)
	go runFunc()
	t.Cleanup(func() { grpcServer.GracefulStop() })

	conn, err := testenv.LocalGRPCConn(t, ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return ofpb.NewOCIFetcherClient(conn)
}

// layerMetadata extracts size and media type from an image layer.
func layerMetadata(t *testing.T, layer ctr.Layer) (size int64, mediaType string) {
	s, err := layer.Size()
	require.NoError(t, err)
	mt, err := layer.MediaType()
	require.NoError(t, err)
	return s, string(mt)
}

// layerData reads the compressed data from an image layer.
func layerData(t *testing.T, layer ctr.Layer) []byte {
	rc, err := layer.Compressed()
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	return data
}

// countingOCIFetcherClient wraps an OCIFetcherClient and counts calls by
// method.
type countingOCIFetcherClient struct {
	inner  ofpb.OCIFetcherClient
	mu     sync.Mutex
	counts map[string]int
}

func (c *countingOCIFetcherClient) count(method string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.counts == nil {
		c.counts = map[string]int{}
	}
	c.counts[method]++
}

func (c *countingOCIFetcherClient) callCounts() map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return maps.Clone(c.counts)
}

func (c *countingOCIFetcherClient) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestResponse, error) {
	c.count("FetchManifest")
	return c.inner.FetchManifest(ctx, req, opts...)
}

func (c *countingOCIFetcherClient) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestMetadataResponse, error) {
	c.count("FetchManifestMetadata")
	return c.inner.FetchManifestMetadata(ctx, req, opts...)
}

func (c *countingOCIFetcherClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (ofpb.OCIFetcher_FetchBlobClient, error) {
	c.count("FetchBlob")
	return c.inner.FetchBlob(ctx, req, opts...)
}

func (c *countingOCIFetcherClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	c.count("FetchBlobMetadata")
	return c.inner.FetchBlobMetadata(ctx, req, opts...)
}

type recordingOCIFetcherClient struct {
	fetchBlobRequest *ofpb.FetchBlobRequest
}

func (c *recordingOCIFetcherClient) FetchManifest(ctx context.Context, req *ofpb.FetchManifestRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestResponse, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (c *recordingOCIFetcherClient) FetchManifestMetadata(ctx context.Context, req *ofpb.FetchManifestMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchManifestMetadataResponse, error) {
	return nil, status.UnimplementedError("not implemented")
}

func (c *recordingOCIFetcherClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (ofpb.OCIFetcher_FetchBlobClient, error) {
	c.fetchBlobRequest = gproto.Clone(req).(*ofpb.FetchBlobRequest)
	return &fakeFetchBlobClient{data: []byte("hello")}, nil
}

func (c *recordingOCIFetcherClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	return &ofpb.FetchBlobMetadataResponse{Size: 5}, nil
}

type fakeFetchBlobClient struct {
	grpc.ClientStream
	data []byte
	sent bool
}

func (c *fakeFetchBlobClient) Recv() (*ofpb.FetchBlobResponse, error) {
	if !c.sent {
		c.sent = true
		return &ofpb.FetchBlobResponse{Data: c.data}, nil
	}
	return nil, io.EOF
}

// collectBlobData reads all data from a FetchBlob stream.
func collectBlobData(t *testing.T, stream ofpb.OCIFetcher_FetchBlobClient) []byte {
	var data []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		data = append(data, resp.GetData()...)
	}
	return data
}

// TestPrivateImageManifestCacheRequiresCredentials confirms the cached
// manifest is not served through the proxy to a caller without valid creds.
func TestPrivateImageManifestCacheRequiresCredentials(t *testing.T) {
	registryCreds := &testregistry.BasicAuthCreds{Username: "good", Password: "secret"}
	validRequestCreds := &rgpb.Credentials{Username: "good", Password: "secret"}
	wrongRequestCreds := &rgpb.Credentials{Username: "good", Password: "wrong"}

	for _, ac := range []struct {
		name  string
		creds *rgpb.Credentials
	}{
		{name: "NoCredentials", creds: nil},
		{name: "WrongCredentials", creds: wrongRequestCreds},
	} {
		t.Run(ac.name, func(t *testing.T) {
			ctx := context.Background()
			counter := testhttp.NewRequestCounter()
			reg := testregistry.Run(t, testregistry.Opts{
				Creds: registryCreds,
				HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
					counter.Inc(r)
					return true
				},
			})
			imageName, img := reg.PushNamedImage(t, "private-image", registryCreds)
			digest, _, _ := imageMetadata(t, img)
			ref := imageName + "@" + digest
			manifestHead := http.MethodHead + " /v2/private-image/manifests/" + digest

			_, bsClient, acClient := setupCacheEnv(t)
			upstream := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, upstream)

			_, err := proxyClient.FetchManifest(ctx, &ofpb.FetchManifestRequest{
				Ref:         ref,
				Credentials: validRequestCreds,
			})
			require.NoError(t, err)

			counter.Reset()
			_, err = proxyClient.FetchManifest(ctx, &ofpb.FetchManifestRequest{
				Ref:         ref,
				Credentials: ac.creds,
			})
			require.True(t, status.IsUnauthenticatedError(err), "expected Unauthenticated, got: %v", err)
			require.Empty(t, cmp.Diff(map[string]int{
				http.MethodGet + " /v2/": 2,
				manifestHead:             2,
			}, counter.Snapshot()))
		})
	}
}

// runRegistryFetchingProxy returns a proxy that can fetch from registries
// itself if enabled, a client for it, and its upstream (the apps), which
// fetches from registries too. If registryFetcher is set, it replaces the
// proxy's own.
func runRegistryFetchingProxy(t *testing.T, enabled bool, registryFetcher ofpb.OCIFetcherServer) (*OCIFetcherServerProxy, ofpb.OCIFetcherClient, *countingOCIFetcherClient) {
	ctx := context.Background()
	flags.Set(t, "cache_proxy.oci_fetch_from_registry", enabled)
	_, appsBSClient, appsACClient := setupCacheEnv(t)
	apps := &countingOCIFetcherClient{inner: runOCIFetcherServer(ctx, t, appsBSClient, appsACClient)}

	_, bsClient, acClient := setupCacheEnv(t)
	env := testenv.GetTestEnv(t)
	env.SetOCIFetcherClient(apps)
	env.SetLocalByteStreamClient(bsClient)
	env.SetLocalActionCacheClient(acClient)
	proxy, err := New(env)
	require.NoError(t, err)
	require.NotNil(t, proxy.registryFetcher)
	if registryFetcher != nil {
		proxy.registryFetcher = registryFetcher
	}
	return proxy, serveOCIFetcherProxy(ctx, t, env, proxy), apps
}

// pushTestImage pushes an image and returns its ref, its only layer's ref, and
// that layer's data.
func pushTestImage(t *testing.T, reg *testregistry.Registry, creds *testregistry.BasicAuthCreds) (imageRef, blobRef string, blob []byte) {
	imageName, img := reg.PushNamedImage(t, "test-image", creds)
	layers, err := img.Layers()
	require.NoError(t, err)
	digest, err := layers[0].Digest()
	require.NoError(t, err)
	return imageName, imageName + "@" + digest.String(), layerData(t, layers[0])
}

// fetchAll makes each OCIFetcher RPC, and returns their errors by method, the
// unary RPCs' responses by method, and the blob's data.
func fetchAll(client ofpb.OCIFetcherClient, imageRef, blobRef string, creds *rgpb.Credentials, bypassRegistry bool) (map[string]error, map[string]gproto.Message, []byte) {
	ctx := context.Background()
	errs := map[string]error{}
	resps := map[string]gproto.Message{}
	resps["FetchManifest"], errs["FetchManifest"] = client.FetchManifest(ctx, &ofpb.FetchManifestRequest{Ref: imageRef, Credentials: creds, BypassRegistry: bypassRegistry})
	resps["FetchManifestMetadata"], errs["FetchManifestMetadata"] = client.FetchManifestMetadata(ctx, &ofpb.FetchManifestMetadataRequest{Ref: imageRef, Credentials: creds, BypassRegistry: bypassRegistry})
	resps["FetchBlobMetadata"], errs["FetchBlobMetadata"] = client.FetchBlobMetadata(ctx, &ofpb.FetchBlobMetadataRequest{Ref: blobRef, Credentials: creds, BypassRegistry: bypassRegistry})
	var blob []byte
	stream, err := client.FetchBlob(ctx, &ofpb.FetchBlobRequest{Ref: blobRef, Credentials: creds, BypassRegistry: bypassRegistry})
	for err == nil {
		var resp *ofpb.FetchBlobResponse
		resp, err = stream.Recv()
		blob = append(blob, resp.GetData()...)
	}
	if err == io.EOF {
		err = nil
	}
	errs["FetchBlob"] = err
	return errs, resps, blob
}

// fallbackCount returns the number of fallbacks to the apps recorded for the
// given method, ref, registry error code and apps result code.
func fallbackCount(method, ref string, registryCode, appsCode codes.Code) float64 {
	return testutil.ToFloat64(metrics.OCIFetcherProxyFallbackCount.With(prometheus.Labels{
		metrics.OCIFetcherMethodLabel:         method,
		metrics.ImageFetchRegistryLabel:       registryLabel(ref),
		metrics.StatusHumanReadableLabel:      registryCode.String(),
		metrics.OCIFetcherFallbackResultLabel: appsCode.String(),
	}))
}

// fakeRegistryFetcher fails every request with err. If err is nil, it fails
// the test if it's called at all.
type fakeRegistryFetcher struct {
	t   *testing.T
	err error
}

func (f *fakeRegistryFetcher) fail() error {
	if f.err == nil {
		f.t.Error("unexpected registry fetch")
		return status.InternalError("unexpected registry fetch")
	}
	return f.err
}

func (f *fakeRegistryFetcher) FetchManifest(context.Context, *ofpb.FetchManifestRequest) (*ofpb.FetchManifestResponse, error) {
	return nil, f.fail()
}

func (f *fakeRegistryFetcher) FetchManifestMetadata(context.Context, *ofpb.FetchManifestMetadataRequest) (*ofpb.FetchManifestMetadataResponse, error) {
	return nil, f.fail()
}

func (f *fakeRegistryFetcher) FetchBlobMetadata(context.Context, *ofpb.FetchBlobMetadataRequest) (*ofpb.FetchBlobMetadataResponse, error) {
	return nil, f.fail()
}

func (f *fakeRegistryFetcher) FetchBlob(*ofpb.FetchBlobRequest, ofpb.OCIFetcher_FetchBlobServer) error {
	return f.fail()
}

func TestFetchFromRegistry(t *testing.T) {
	counter := testhttp.NewRequestCounter()
	reg := testregistry.Run(t, testregistry.Opts{
		HttpInterceptor: func(w http.ResponseWriter, r *http.Request) bool {
			counter.Inc(r)
			return true
		},
	})
	imageRef, blobRef, want := pushTestImage(t, reg, nil)
	counter.Reset()
	_, client, apps := runRegistryFetchingProxy(t, true, nil)

	for range 2 {
		errs, _, got := fetchAll(client, imageRef, blobRef, nil, false)
		require.Equal(t, map[string]error{"FetchManifest": nil, "FetchManifestMetadata": nil, "FetchBlobMetadata": nil, "FetchBlob": nil}, errs)
		require.Equal(t, want, got)
	}

	require.Empty(t, apps.callCounts())
	// The second round is served from the proxy's cache.
	_, digest, _ := strings.Cut(blobRef, "@")
	snapshot := counter.Snapshot()
	require.Equal(t, 1, snapshot[http.MethodGet+" /v2/test-image/manifests/latest"], "requests: %v", snapshot)
	require.Equal(t, 1, snapshot[http.MethodGet+" /v2/test-image/blobs/"+digest], "requests: %v", snapshot)
}

func TestFetchFromRegistryGatedOff(t *testing.T) {
	for _, tc := range []struct {
		name           string
		enabled        bool
		bypassRegistry bool
		code           codes.Code
	}{
		{name: "ExperimentOff", enabled: false, code: codes.OK},
		// The apps reject bypassing the registry, as the caller isn't an admin.
		{name: "BypassRegistry", enabled: true, bypassRegistry: true, code: codes.PermissionDenied},
	} {
		t.Run(tc.name, func(t *testing.T) {
			imageRef, blobRef, blob := pushTestImage(t, setupTestRegistry(t, nil), nil)
			_, client, apps := runRegistryFetchingProxy(t, tc.enabled, &fakeRegistryFetcher{t: t})
			// What the apps return when called directly (not counted).
			_, wantResps, _ := fetchAll(apps.inner, imageRef, blobRef, nil, tc.bypassRegistry)

			errs, resps, got := fetchAll(client, imageRef, blobRef, nil, tc.bypassRegistry)
			for method, err := range errs {
				require.Equal(t, tc.code, gstatus.Code(err), "%s: %v", method, err)
			}
			require.Empty(t, cmp.Diff(wantResps, resps, protocmp.Transform()))
			// FetchBlob checks the blob's metadata with the apps first, and
			// stops there if that fails.
			wantCalls := map[string]int{"FetchManifest": 1, "FetchManifestMetadata": 1, "FetchBlobMetadata": 2}
			if tc.code == codes.OK {
				require.NotEmpty(t, resps["FetchManifest"].(*ofpb.FetchManifestResponse).GetManifest())
				require.Equal(t, blob, got)
				wantCalls["FetchBlob"] = 1
			}
			require.Equal(t, wantCalls, apps.callCounts())
		})
	}
}

func TestFetchFromRegistryFallsBackToApps(t *testing.T) {
	for _, tc := range []struct {
		code     codes.Code
		fallBack bool
	}{
		{codes.Unavailable, true},
		{codes.ResourceExhausted, true},
		{codes.Unauthenticated, true},
		{codes.PermissionDenied, true},
		{codes.NotFound, false},
		{codes.Internal, false},
	} {
		t.Run(tc.code.String(), func(t *testing.T) {
			imageRef, blobRef, want := pushTestImage(t, setupTestRegistry(t, nil), nil)
			registryErr := gstatus.Error(tc.code, "registry error")
			_, client, apps := runRegistryFetchingProxy(t, true, &fakeRegistryFetcher{t: t, err: registryErr})
			refs := map[string]string{"FetchManifest": imageRef, "FetchManifestMetadata": imageRef, "FetchBlobMetadata": blobRef, "FetchBlob": blobRef}
			before := map[string]float64{}
			for method, ref := range refs {
				before[method] = fallbackCount(method, ref, tc.code, codes.OK)
			}

			errs, _, got := fetchAll(client, imageRef, blobRef, nil, false)

			for method, err := range errs {
				if tc.fallBack {
					require.NoError(t, err, method)
					require.Equal(t, before[method]+1, fallbackCount(method, refs[method], tc.code, codes.OK), method)
				} else {
					require.Equal(t, tc.code, gstatus.Code(err), "%s: %v", method, err)
				}
			}
			if tc.fallBack {
				require.Equal(t, want, got)
				require.Equal(t, map[string]int{"FetchManifest": 1, "FetchManifestMetadata": 1, "FetchBlobMetadata": 2, "FetchBlob": 1}, apps.callCounts())
			} else {
				require.Empty(t, apps.callCounts())
			}
		})
	}
}

// partialBlobFetcher sends part of a blob, then fails with an error that
// would otherwise cause a fallback to the apps.
type partialBlobFetcher struct {
	ofpb.UnimplementedOCIFetcherServer
}

func (partialBlobFetcher) FetchBlob(req *ofpb.FetchBlobRequest, stream ofpb.OCIFetcher_FetchBlobServer) error {
	if err := stream.Send(&ofpb.FetchBlobResponse{Data: []byte("partial")}); err != nil {
		return err
	}
	return status.UnavailableError("registry connection reset")
}

// failingSendStream is a FetchBlob stream whose client is gone.
type failingSendStream struct {
	ofpb.OCIFetcher_FetchBlobServer
}

func (failingSendStream) Context() context.Context {
	return context.Background()
}

func (failingSendStream) Send(*ofpb.FetchBlobResponse) error {
	return status.UnavailableError("client is gone")
}

func TestFetchFromRegistryDoesNotFallBackAfterFailedSend(t *testing.T) {
	proxy, _, apps := runRegistryFetchingProxy(t, true, partialBlobFetcher{})

	err := proxy.FetchBlob(&ofpb.FetchBlobRequest{Ref: "localhost/repo@sha256:" + strings.Repeat("a", 64)}, failingSendStream{})
	require.True(t, status.IsUnavailableError(err), "unexpected error: %v", err)
	require.Empty(t, apps.callCounts())
}

func TestFetchFromRegistryDoesNotFallBackAfterPartialBlob(t *testing.T) {
	_, client, apps := runRegistryFetchingProxy(t, true, partialBlobFetcher{})

	stream, err := client.FetchBlob(context.Background(), &ofpb.FetchBlobRequest{Ref: "localhost/repo@sha256:" + strings.Repeat("a", 64)})
	require.NoError(t, err)
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte("partial"), resp.GetData())
	_, err = stream.Recv()
	require.True(t, status.IsUnavailableError(err), "unexpected error: %v", err)
	require.Empty(t, apps.callCounts())
}

func TestFetchFromRegistryWarmCacheRequiresValidCredentials(t *testing.T) {
	registryCreds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	imageRef, blobRef, want := pushTestImage(t, setupTestRegistry(t, registryCreds), registryCreds)
	_, client, apps := runRegistryFetchingProxy(t, true, nil)

	// Warm the proxy's cache with valid credentials.
	errs, _, got := fetchAll(client, imageRef, blobRef, &rgpb.Credentials{Username: "testuser", Password: "testpass"}, false)
	for method, err := range errs {
		require.NoError(t, err, method)
	}
	require.Equal(t, want, got)
	require.Empty(t, apps.callCounts())

	// The registry rejects the proxy, so it falls back to the apps, which
	// reject the credentials too.
	refs := map[string]string{"FetchManifest": imageRef, "FetchManifestMetadata": imageRef, "FetchBlobMetadata": blobRef, "FetchBlob": blobRef}
	before := map[string]float64{}
	for method, ref := range refs {
		before[method] = fallbackCount(method, ref, codes.Unauthenticated, codes.Unauthenticated)
	}
	errs, _, got = fetchAll(client, imageRef, blobRef, &rgpb.Credentials{Username: "testuser", Password: "wrong"}, false)
	for method, err := range errs {
		require.True(t, status.IsUnauthenticatedError(err), "%s: %v", method, err)
		require.Equal(t, before[method]+1, fallbackCount(method, refs[method], codes.Unauthenticated, codes.Unauthenticated), method)
	}
	require.Empty(t, got)
	require.Equal(t, map[string]int{"FetchManifest": 1, "FetchManifestMetadata": 1, "FetchBlobMetadata": 2}, apps.callCounts())
}

func TestFetchFromRegistryFallbackMetricRecordsAppsResult(t *testing.T) {
	registryCreds := &testregistry.BasicAuthCreds{Username: "testuser", Password: "testpass"}
	imageRef, blobRef, _ := pushTestImage(t, setupTestRegistry(t, registryCreds), registryCreds)
	registryErr := status.UnavailableError("registry error")
	_, client, apps := runRegistryFetchingProxy(t, true, &fakeRegistryFetcher{t: t, err: registryErr})

	// The proxy's registry fetch fails with Unavailable, so it falls back to
	// the apps, which reject the credentials with Unauthenticated.
	refs := map[string]string{"FetchManifest": imageRef, "FetchManifestMetadata": imageRef, "FetchBlobMetadata": blobRef, "FetchBlob": blobRef}
	before := map[string]float64{}
	for method, ref := range refs {
		before[method] = fallbackCount(method, ref, codes.Unavailable, codes.Unauthenticated)
	}
	errs, _, got := fetchAll(client, imageRef, blobRef, &rgpb.Credentials{Username: "testuser", Password: "wrong"}, false)
	for method, err := range errs {
		require.True(t, status.IsUnauthenticatedError(err), "%s: %v", method, err)
		require.Equal(t, before[method]+1, fallbackCount(method, refs[method], codes.Unavailable, codes.Unauthenticated), method)
	}
	require.Empty(t, got)
	require.Equal(t, map[string]int{"FetchManifest": 1, "FetchManifestMetadata": 1, "FetchBlobMetadata": 2}, apps.callCounts())
}

func TestFetchFromRegistryFetcherCreationFails(t *testing.T) {
	_, bsClient, acClient := setupCacheEnv(t)
	env := testenv.GetTestEnv(t)
	env.SetOCIFetcherClient(&countingOCIFetcherClient{})
	env.SetLocalByteStreamClient(bsClient)
	env.SetLocalActionCacheClient(acClient)
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"not a CIDR"})

	// The proxy starts, and forwards all requests to the apps.
	proxy, err := New(env)
	require.NoError(t, err)
	require.Nil(t, proxy.registryFetcher)
}
