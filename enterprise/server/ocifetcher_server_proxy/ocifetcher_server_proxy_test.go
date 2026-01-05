package ocifetcher_server_proxy

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
	ofpb "github.com/buildbuddy-io/buildbuddy/proto/oci_fetcher"
	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func TestNew_MissingClient(t *testing.T) {
	env := testenv.GetTestEnv(t)
	// Don't set OCIFetcherClient

	_, err := New(env)
	require.Error(t, err)
	require.Contains(t, err.Error(), "OCIFetcherClient is required")
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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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

			te, bsClient, acClient := setupCacheEnv(t)
			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			proxyClient := runOCIFetcherProxy(ctx, t, te, ociFetcherClient)

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
func imageMetadata(t *testing.T, img v1.Image) (digest string, size int64, mediaType string) {
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

// runOCIFetcherServer creates an OCIFetcher server and returns a client connected to it.
func runOCIFetcherServer(ctx context.Context, t *testing.T, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient) ofpb.OCIFetcherClient {
	flags.Set(t, "executor.container_registry_allowed_private_ips", []string{"127.0.0.0/8", "::1/128"})
	server, err := ocifetcher.NewServer(bsClient, acClient)
	require.NoError(t, err)

	env := testenv.GetTestEnv(t)
	// Use TestAuthenticator to enable JWT parsing for bypass_registry tests
	env.SetAuthenticator(testauth.NewTestAuthenticator(nil))

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	ofpb.RegisterOCIFetcherServer(grpcServer, server)
	go runFunc()
	t.Cleanup(func() { grpcServer.GracefulStop() })

	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return ofpb.NewOCIFetcherClient(conn)
}

// runOCIFetcherProxy sets up the proxy server connecting to the remote client
// and returns a client connected to the proxy. It uses the cacheEnv to share
// the cache with the proxy.
func runOCIFetcherProxy(ctx context.Context, t *testing.T, cacheEnv *testenv.TestEnv, remoteClient ofpb.OCIFetcherClient) ofpb.OCIFetcherClient {
	// Create a new test env for the proxy, sharing the cache from cacheEnv
	env := testenv.GetTestEnv(t)
	env.SetCache(cacheEnv.GetCache())
	env.SetOCIFetcherClient(remoteClient)

	// Set up local ByteStreamServer for the proxy
	localBSS, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	env.SetLocalByteStreamServer(localBSS)

	proxy, err := New(env)
	require.NoError(t, err)

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	ofpb.RegisterOCIFetcherServer(grpcServer, proxy)
	go runFunc()
	t.Cleanup(func() { grpcServer.GracefulStop() })

	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return ofpb.NewOCIFetcherClient(conn)
}

// layerMetadata extracts size and media type from an image layer.
func layerMetadata(t *testing.T, layer v1.Layer) (size int64, mediaType string) {
	s, err := layer.Size()
	require.NoError(t, err)
	mt, err := layer.MediaType()
	require.NoError(t, err)
	return s, string(mt)
}

// layerData reads the compressed data from an image layer.
func layerData(t *testing.T, layer v1.Layer) []byte {
	rc, err := layer.Compressed()
	require.NoError(t, err)
	defer rc.Close()
	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	return data
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

// TestFetchBlob_LocalCacheHit tests that when the blob is present in the local
// CAS, FetchBlob serves it from there without calling remote.FetchBlob.
func TestFetchBlob_LocalCacheHit(t *testing.T) {
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

	te, bsClient, acClient := setupCacheEnv(t)

	// Pre-populate local CAS with the blob data
	seedLocalCAS(ctx, t, te, digest.Hex, expectedData)

	// Wrap the OCI fetcher client to track whether FetchBlob was called
	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
	tracker := &fetchBlobTracker{OCIFetcherClient: ociFetcherClient}
	proxyClient := runOCIFetcherProxy(ctx, t, te, tracker)

	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + digest.String(),
	})
	require.NoError(t, err)

	data := collectBlobData(t, stream)
	require.Equal(t, expectedData, data)
	// Verify remote.FetchBlob was NOT called (data served from local)
	require.False(t, tracker.fetchBlobCalled, "expected blob to be served from local cache, but remote.FetchBlob was called")
}

// TestFetchBlob_LocalCacheMiss tests that when the blob is NOT in the local
// CAS, FetchBlob falls back to the remote and serves data from there.
func TestFetchBlob_LocalCacheMiss(t *testing.T) {
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

	te, bsClient, acClient := setupCacheEnv(t)

	// Do NOT pre-populate local CAS - cache miss scenario

	// Wrap the OCI fetcher client to track whether FetchBlob was called
	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
	tracker := &fetchBlobTracker{OCIFetcherClient: ociFetcherClient}
	proxyClient := runOCIFetcherProxy(ctx, t, te, tracker)

	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + digest.String(),
	})
	require.NoError(t, err)

	data := collectBlobData(t, stream)
	require.Equal(t, expectedData, data)
	// Verify remote.FetchBlob WAS called (fallback from local cache miss)
	require.True(t, tracker.fetchBlobCalled, "expected remote.FetchBlob to be called on cache miss")
}

// TestFetchBlob_MetadataError tests behavior when FetchBlobMetadata fails.
func TestFetchBlob_MetadataError(t *testing.T) {
	for _, tc := range []struct {
		name         string
		seedLocalCAS bool
	}{
		{"LocalPresent", true},
		{"LocalMissing", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

			te, bsClient, acClient := setupCacheEnv(t)

			if tc.seedLocalCAS {
				seedLocalCAS(ctx, t, te, digest.Hex, expectedData)
			}

			ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
			// Wrap to inject metadata errors
			errorClient := &metadataErrorClient{
				OCIFetcherClient: ociFetcherClient,
				metadataErr:      status.NotFoundError("simulated metadata error"),
			}
			proxyClient := runOCIFetcherProxy(ctx, t, te, errorClient)

			stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
				Ref: imageName + "@" + digest.String(),
			})
			require.NoError(t, err)

			// When metadata fails, should fallback to remote.FetchBlob
			data := collectBlobData(t, stream)
			require.Equal(t, expectedData, data)
		})
	}
}

// TestFetchBlob_MidStreamRemoteFailure tests that errors occurring mid-stream
// from the remote are properly propagated to the client.
func TestFetchBlob_MidStreamRemoteFailure(t *testing.T) {
	ctx := context.Background()

	reg := setupTestRegistry(t, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)

	te, bsClient, acClient := setupCacheEnv(t)

	// Do NOT seed local CAS - forces remote fetch

	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
	// Wrap to inject mid-stream errors - fail after receiving first response
	midStreamErr := status.UnavailableError("simulated mid-stream failure")
	errorClient := &midStreamErrorClient{
		OCIFetcherClient:  ociFetcherClient,
		failAfterMessages: 1, // Fail after first message
		err:               midStreamErr,
	}
	proxyClient := runOCIFetcherProxy(ctx, t, te, errorClient)

	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + layerDigest.String(),
	})
	require.NoError(t, err)

	// Read until error
	var receivedData []byte
	var streamErr error
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			streamErr = err
			break
		}
		receivedData = append(receivedData, resp.GetData()...)
	}

	// For small blobs that arrive in a single message, we'll receive all data
	// before the error. For larger blobs, we'd see partial data.
	// The key behavior is that the error is propagated.
	require.NotEmpty(t, receivedData, "expected to receive some data before failure")
	require.Equal(t, expectedData, receivedData, "expected to receive blob data before error")
	// Should have received an error after data
	require.Error(t, streamErr)
	require.True(t, status.IsUnavailableError(streamErr), "expected UnavailableError, got: %v", streamErr)
}

// TestFetchBlob_MidStreamLocalFailure tests that when the local CAS read fails,
// the proxy falls back to the remote.
func TestFetchBlob_MidStreamLocalFailure(t *testing.T) {
	ctx := context.Background()

	reg := setupTestRegistry(t, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)

	te, bsClient, acClient := setupCacheEnv(t)

	// Seed local CAS with correct data
	seedLocalCAS(ctx, t, te, layerDigest.Hex, expectedData)

	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)
	tracker := &fetchBlobTracker{OCIFetcherClient: ociFetcherClient}

	// Create proxy with a local BSS that fails immediately on first Send
	proxyClient := runOCIFetcherProxyWithFailingLocalBSS(ctx, t, te, tracker)

	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + layerDigest.String(),
	})
	require.NoError(t, err)

	// The proxy should fallback to remote when local fails
	data := collectBlobData(t, stream)
	require.Equal(t, expectedData, data)
	// Verify remote.FetchBlob WAS called (fallback from local failure)
	require.True(t, tracker.fetchBlobCalled, "expected remote.FetchBlob to be called on local failure")
}

// seedLocalCAS writes blob data to the local CAS with the given hash using ByteStream.
// Uses zstd compression to match how ocicache stores blobs.
func seedLocalCAS(ctx context.Context, t *testing.T, te *testenv.TestEnv, hash string, data []byte) {
	bsClient := te.GetByteStreamClient()
	require.NotNil(t, bsClient)

	d := &repb.Digest{
		Hash:      hash,
		SizeBytes: int64(len(data)),
	}
	rn := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)
	rn.SetCompressor(repb.Compressor_ZSTD)

	// Use cachetools.UploadFromReader which handles zstd compression properly
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, rn, bytes.NewReader(data))
	require.NoError(t, err)
}

// fetchBlobTracker wraps an OCIFetcherClient to track if FetchBlob was called.
type fetchBlobTracker struct {
	ofpb.OCIFetcherClient
	fetchBlobCalled bool
}

func (t *fetchBlobTracker) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (ofpb.OCIFetcher_FetchBlobClient, error) {
	t.fetchBlobCalled = true
	return t.OCIFetcherClient.FetchBlob(ctx, req, opts...)
}

// metadataErrorClient wraps an OCIFetcherClient to return errors from FetchBlobMetadata.
type metadataErrorClient struct {
	ofpb.OCIFetcherClient
	metadataErr error
}

func (c *metadataErrorClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	return nil, c.metadataErr
}

// midStreamErrorClient wraps an OCIFetcherClient to fail mid-stream during FetchBlob.
type midStreamErrorClient struct {
	ofpb.OCIFetcherClient
	failAfterMessages int // Fail after this many messages have been received
	err               error
}

func (c *midStreamErrorClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (ofpb.OCIFetcher_FetchBlobClient, error) {
	stream, err := c.OCIFetcherClient.FetchBlob(ctx, req, opts...)
	if err != nil {
		return nil, err
	}
	return &midStreamErrorStream{
		OCIFetcher_FetchBlobClient: stream,
		failAfterMessages:          c.failAfterMessages,
		err:                        c.err,
	}, nil
}

type midStreamErrorStream struct {
	ofpb.OCIFetcher_FetchBlobClient
	failAfterMessages int
	messagesReceived  int
	err               error
}

func (s *midStreamErrorStream) Recv() (*ofpb.FetchBlobResponse, error) {
	if s.messagesReceived >= s.failAfterMessages {
		return nil, s.err
	}
	resp, err := s.OCIFetcher_FetchBlobClient.Recv()
	if err != nil {
		return nil, err
	}
	s.messagesReceived++
	return resp, nil
}

// runOCIFetcherProxyWithFailingLocalBSS creates a proxy with a local BSS that fails on read.
func runOCIFetcherProxyWithFailingLocalBSS(ctx context.Context, t *testing.T, cacheEnv *testenv.TestEnv, remoteClient ofpb.OCIFetcherClient) ofpb.OCIFetcherClient {
	env := testenv.GetTestEnv(t)
	env.SetCache(cacheEnv.GetCache())
	env.SetOCIFetcherClient(remoteClient)

	// Set up local ByteStreamServer with error injection
	realBSS, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	failingBSS := &failingByteStreamServer{
		ByteStreamServer: realBSS,
	}
	env.SetLocalByteStreamServer(failingBSS)

	proxy, err := New(env)
	require.NoError(t, err)

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	ofpb.RegisterOCIFetcherServer(grpcServer, proxy)
	go runFunc()
	t.Cleanup(func() { grpcServer.GracefulStop() })

	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return ofpb.NewOCIFetcherClient(conn)
}

// failingByteStreamServer wraps a ByteStreamServer to fail on read.
type failingByteStreamServer struct {
	interfaces.ByteStreamServer
}

func (s *failingByteStreamServer) ReadCASResource(ctx context.Context, rn *digest.CASResourceName, offset, limit int64, stream bspb.ByteStream_ReadServer) error {
	// Return an error immediately to simulate local read failure
	return status.UnavailableError("simulated local read failure")
}
