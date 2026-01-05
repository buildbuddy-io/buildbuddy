package ocifetcher_server_proxy

import (
	"context"
	"io"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/oci/ocifetcher"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testregistry"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
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
	"google.golang.org/grpc"
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
// and returns a client connected to the proxy.
func runOCIFetcherProxy(ctx context.Context, t *testing.T, remoteClient ofpb.OCIFetcherClient) ofpb.OCIFetcherClient {
	env := testenv.GetTestEnv(t)
	env.SetOCIFetcherClient(remoteClient)

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

// runOCIFetcherProxyWithLocalCache sets up the proxy server with a local ByteStream server
// for caching and returns a client connected to the proxy.
func runOCIFetcherProxyWithLocalCache(ctx context.Context, t *testing.T, remoteClient ofpb.OCIFetcherClient, localBSServer interfaces.ByteStreamServer) ofpb.OCIFetcherClient {
	env := testenv.GetTestEnv(t)
	env.SetOCIFetcherClient(remoteClient)
	env.SetLocalByteStreamServer(localBSServer)

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

// createLocalByteStreamServer creates a local ByteStreamServer backed by an in-memory cache.
func createLocalByteStreamServer(t *testing.T) interfaces.ByteStreamServer {
	env := testenv.GetTestEnv(t)
	mc, err := memory_cache.NewMemoryCache(1e9) // 1GB
	require.NoError(t, err)
	env.SetCache(mc)

	bs, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	return bs
}

// readFromLocalCache reads a blob from the local ByteStream server.
func readFromLocalCache(ctx context.Context, t *testing.T, bsServer interfaces.ByteStreamServer, hash v1.Hash, size int64) []byte {
	d := &repb.Digest{Hash: hash.Hex, SizeBytes: size}
	rn := digest.NewCASResourceName(d, "", repb.DigestFunction_SHA256)

	req := &bspb.ReadRequest{
		ResourceName: rn.DownloadString(),
	}

	reader := &testReadStream{ctx: ctx, data: make([]byte, 0)}
	err := bsServer.Read(req, reader)
	require.NoError(t, err)
	return reader.data
}

// testReadStream is a mock ByteStream_ReadServer for reading from local cache.
type testReadStream struct {
	bspb.ByteStream_ReadServer
	ctx  context.Context
	data []byte
}

func (s *testReadStream) Send(resp *bspb.ReadResponse) error {
	s.data = append(s.data, resp.GetData()...)
	return nil
}

func (s *testReadStream) Context() context.Context { return s.ctx }

// failingByteStreamServer is a mock that fails on Write after consuming all data.
// It still calls Recv() to drive the data flow (which sends data to the client),
// but returns an error at the end.
type failingByteStreamServer struct {
	interfaces.ByteStreamServer
}

func (f *failingByteStreamServer) Write(stream bspb.ByteStream_WriteServer) error {
	// Consume all data from the stream (this drives data to the client)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if req.GetFinishWrite() {
			break
		}
	}
	// Fail after all data has been sent to client
	return status.InternalError("simulated write failure")
}

// failingOCIFetcherClient is a mock OCI fetcher client that returns an error mid-stream.
type failingOCIFetcherClient struct {
	ofpb.OCIFetcherClient
	realClient ofpb.OCIFetcherClient
}

// metadataFailingOCIFetcherClient fails on FetchBlobMetadata but succeeds on FetchBlob.
type metadataFailingOCIFetcherClient struct {
	ofpb.OCIFetcherClient
	realClient ofpb.OCIFetcherClient
}

func (m *metadataFailingOCIFetcherClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	return nil, status.InternalError("simulated metadata failure")
}

func (m *metadataFailingOCIFetcherClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ofpb.FetchBlobResponse], error) {
	return m.realClient.FetchBlob(ctx, req, opts...)
}

// immediateFailingOCIFetcherClient fails immediately on FetchBlob call.
type immediateFailingOCIFetcherClient struct {
	ofpb.OCIFetcherClient
	realClient ofpb.OCIFetcherClient
}

func (i *immediateFailingOCIFetcherClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	return i.realClient.FetchBlobMetadata(ctx, req, opts...)
}

func (i *immediateFailingOCIFetcherClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ofpb.FetchBlobResponse], error) {
	return nil, status.InternalError("simulated immediate fetch failure")
}

func (f *failingOCIFetcherClient) FetchBlob(ctx context.Context, req *ofpb.FetchBlobRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ofpb.FetchBlobResponse], error) {
	// Return a stream that fails after sending some data
	return &failingFetchBlobStream{ctx: ctx}, nil
}

func (f *failingOCIFetcherClient) FetchBlobMetadata(ctx context.Context, req *ofpb.FetchBlobMetadataRequest, opts ...grpc.CallOption) (*ofpb.FetchBlobMetadataResponse, error) {
	return f.realClient.FetchBlobMetadata(ctx, req)
}

type failingFetchBlobStream struct {
	ofpb.OCIFetcher_FetchBlobClient
	ctx       context.Context
	callCount int
}

func (s *failingFetchBlobStream) Recv() (*ofpb.FetchBlobResponse, error) {
	s.callCount++
	if s.callCount == 1 {
		// Send some data first
		return &ofpb.FetchBlobResponse{Data: []byte("partial data")}, nil
	}
	// Then fail
	return nil, status.InternalError("simulated remote read failure")
}

func (s *failingFetchBlobStream) Context() context.Context { return s.ctx }

// TestFetchBlob_WritesToLocalCache tests that FetchBlob writes blobs to the local cache.
func TestFetchBlob_WritesToLocalCache(t *testing.T) {
	ctx := context.Background()

	// Setup registry and push image
	reg := setupTestRegistry(t, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)
	layerSize, _ := layerMetadata(t, layer)

	// Setup remote OCI fetcher
	_, bsClient, acClient := setupCacheEnv(t)
	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)

	// Setup local ByteStream server for proxy
	localBSServer := createLocalByteStreamServer(t)

	// Run proxy with local cache
	proxyClient := runOCIFetcherProxyWithLocalCache(ctx, t, ociFetcherClient, localBSServer)

	// Fetch blob
	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + layerDigest.String(),
	})
	require.NoError(t, err)

	data := collectBlobData(t, stream)
	require.Equal(t, expectedData, data)

	// Verify blob is in local cache
	cachedData := readFromLocalCache(ctx, t, localBSServer, layerDigest, layerSize)
	require.Equal(t, expectedData, cachedData)
}

// TestFetchBlob_BlobAlreadyInLocalCache tests that FetchBlob works when blob is already cached.
func TestFetchBlob_BlobAlreadyInLocalCache(t *testing.T) {
	ctx := context.Background()

	// Setup registry and push image
	reg := setupTestRegistry(t, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)

	// Setup remote OCI fetcher
	_, bsClient, acClient := setupCacheEnv(t)
	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)

	// Setup local ByteStream server for proxy
	localBSServer := createLocalByteStreamServer(t)

	// Run proxy with local cache
	proxyClient := runOCIFetcherProxyWithLocalCache(ctx, t, ociFetcherClient, localBSServer)

	// Fetch blob FIRST time to populate cache
	stream1, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + layerDigest.String(),
	})
	require.NoError(t, err)
	data1 := collectBlobData(t, stream1)
	require.Equal(t, expectedData, data1)

	// Fetch blob SECOND time - should still work even though blob is already in cache
	stream2, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + layerDigest.String(),
	})
	require.NoError(t, err)
	data2 := collectBlobData(t, stream2)
	require.Equal(t, expectedData, data2)
}

// TestFetchBlob_LocalWriteFailure tests that FetchBlob still returns data even when
// writing to the local cache fails.
func TestFetchBlob_LocalWriteFailure(t *testing.T) {
	ctx := context.Background()

	// Setup registry and push image
	reg := setupTestRegistry(t, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)

	// Setup remote OCI fetcher
	_, bsClient, acClient := setupCacheEnv(t)
	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)

	// Use a failing ByteStream server
	failingBSServer := &failingByteStreamServer{}

	// Run proxy with failing local cache
	proxyClient := runOCIFetcherProxyWithLocalCache(ctx, t, ociFetcherClient, failingBSServer)

	// Fetch blob - should still succeed even though local cache write fails
	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + layerDigest.String(),
	})
	require.NoError(t, err)

	data := collectBlobData(t, stream)
	require.Equal(t, expectedData, data)
}

// TestFetchBlob_RemoteReadFailure tests that FetchBlob returns an error when
// reading from the remote fails.
func TestFetchBlob_RemoteReadFailure(t *testing.T) {
	ctx := context.Background()

	// Setup registry and push image
	reg := setupTestRegistry(t, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)

	// Setup real remote OCI fetcher (for FetchBlobMetadata)
	_, bsClient, acClient := setupCacheEnv(t)
	realOCIFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)

	// Wrap it with a failing client
	failingClient := &failingOCIFetcherClient{realClient: realOCIFetcherClient}

	// Setup local ByteStream server for proxy
	localBSServer := createLocalByteStreamServer(t)

	// Run proxy with failing remote
	proxyClient := runOCIFetcherProxyWithLocalCache(ctx, t, failingClient, localBSServer)

	// Fetch blob - should fail with an error
	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + layerDigest.String(),
	})
	require.NoError(t, err) // Initial call succeeds

	// Error should come during streaming
	for {
		_, recvErr := stream.Recv()
		if recvErr == io.EOF {
			break
		}
		if recvErr != nil {
			// Expected error
			require.True(t, status.IsInternalError(recvErr), "expected InternalError, got: %v", recvErr)
			return
		}
	}
	t.Fatal("expected error during streaming, but got none")
}

// TestFetchBlob_InvalidRefFormat tests that FetchBlob fails when given an invalid ref.
func TestFetchBlob_InvalidRefFormat(t *testing.T) {
	ctx := context.Background()

	// Setup remote OCI fetcher
	_, bsClient, acClient := setupCacheEnv(t)
	ociFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)

	// Setup local ByteStream server for proxy
	localBSServer := createLocalByteStreamServer(t)

	// Run proxy with local cache
	proxyClient := runOCIFetcherProxyWithLocalCache(ctx, t, ociFetcherClient, localBSServer)

	// Try to fetch with invalid ref (not a digest ref)
	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: "not-a-valid-ref",
	})
	if err != nil {
		// Error on initial call is acceptable
		return
	}

	// Error might come during streaming
	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
}

// TestFetchBlob_MetadataFailure tests that FetchBlob still returns data when
// FetchBlobMetadata fails (falls back to passthrough without caching).
func TestFetchBlob_MetadataFailure(t *testing.T) {
	ctx := context.Background()

	// Setup registry and push image
	reg := setupTestRegistry(t, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)
	expectedData := layerData(t, layer)

	// Setup real remote OCI fetcher
	_, bsClient, acClient := setupCacheEnv(t)
	realOCIFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)

	// Wrap it with a metadata-failing client
	metadataFailingClient := &metadataFailingOCIFetcherClient{realClient: realOCIFetcherClient}

	// Setup local ByteStream server for proxy
	localBSServer := createLocalByteStreamServer(t)

	// Run proxy with metadata-failing remote
	proxyClient := runOCIFetcherProxyWithLocalCache(ctx, t, metadataFailingClient, localBSServer)

	// Fetch blob - should succeed via passthrough (metadata failure causes cache skip)
	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + layerDigest.String(),
	})
	require.NoError(t, err)

	data := collectBlobData(t, stream)
	require.Equal(t, expectedData, data)
}

// TestFetchBlob_RemoteImmediateFailure tests that FetchBlob returns an error when
// the remote FetchBlob call fails immediately (before streaming starts).
func TestFetchBlob_RemoteImmediateFailure(t *testing.T) {
	ctx := context.Background()

	// Setup registry and push image to get a valid ref
	reg := setupTestRegistry(t, nil)
	imageName, img := reg.PushNamedImage(t, "test-image", nil)

	layers, err := img.Layers()
	require.NoError(t, err)
	require.NotEmpty(t, layers)
	layer := layers[0]
	layerDigest, err := layer.Digest()
	require.NoError(t, err)

	// Setup real remote OCI fetcher (for FetchBlobMetadata)
	_, bsClient, acClient := setupCacheEnv(t)
	realOCIFetcherClient := runOCIFetcherServer(ctx, t, bsClient, acClient)

	// Use an immediate-failing client
	immediateFailingClient := &immediateFailingOCIFetcherClient{realClient: realOCIFetcherClient}

	// Setup local ByteStream server for proxy
	localBSServer := createLocalByteStreamServer(t)

	// Run proxy with immediate-failing remote
	proxyClient := runOCIFetcherProxyWithLocalCache(ctx, t, immediateFailingClient, localBSServer)

	// Fetch blob - should fail with error
	stream, err := proxyClient.FetchBlob(ctx, &ofpb.FetchBlobRequest{
		Ref: imageName + "@" + layerDigest.String(),
	})
	if err != nil {
		// Error on initial call
		require.True(t, status.IsInternalError(err), "expected InternalError, got: %v", err)
		return
	}

	// Error might come during streaming
	_, recvErr := stream.Recv()
	require.Error(t, recvErr)
	require.True(t, status.IsInternalError(recvErr), "expected InternalError, got: %v", recvErr)
}
