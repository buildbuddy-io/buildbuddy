package ocifetcher_server_proxy

import (
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/stretchr/testify/require"

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

	// Set up local ActionCacheServer for the proxy
	localACS, err := action_cache_server.NewActionCacheServer(env)
	require.NoError(t, err)
	env.SetLocalActionCacheServer(localACS)

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
