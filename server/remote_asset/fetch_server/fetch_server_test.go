package fetch_server_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/remote_asset/fetch_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

func runFetchServer(ctx context.Context, env *testenv.TestEnv, t *testing.T) *grpc.ClientConn {
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	fetchServer, err := fetch_server.NewFetchServer(env)
	require.NoError(t, err)

	grpcServer, runFunc := testenv.RegisterLocalGRPCServer(env)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	rapb.RegisterFetchServer(grpcServer, fetchServer)

	go runFunc()
	t.Cleanup(func() { grpcServer.GracefulStop() })

	clientConn, err := testenv.LocalGRPCConn(ctx, env)
	require.NoError(t, err)

	env.SetByteStreamClient(bspb.NewByteStreamClient(clientConn))
	return clientConn
}

func checksumQualifierFromContent(t *testing.T, contentHash string, digestFunc repb.DigestFunction_Value) string {
	h, err := hex.DecodeString(contentHash)
	require.NoError(t, err)
	base64hash := base64.StdEncoding.EncodeToString(h)

	var prefix string
	if digestFunc == repb.DigestFunction_UNKNOWN {
		prefix = "sha256"
	} else {
		prefix = strings.ToLower(digestFunc.String())
	}

	return fmt.Sprintf("%s-%s", prefix, base64hash)
}

func TestFetchBlob(t *testing.T) {
	for _, tc := range []struct {
		name       string
		content    string
		digestFunc repb.DigestFunction_Value
	}{
		{
			name:       "default_digest_func",
			content:    "default",
			digestFunc: repb.DigestFunction_UNKNOWN,
		},
		{
			name:       "sha256_content",
			content:    "sha256",
			digestFunc: repb.DigestFunction_SHA256,
		},
		{
			name:       "blake3_content",
			content:    "blake3",
			digestFunc: repb.DigestFunction_BLAKE3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			clientConn := runFetchServer(ctx, te, t)
			fetchClient := rapb.NewFetchClient(clientConn)

			contentDigest, err := digest.Compute(bytes.NewReader([]byte(tc.content)), tc.digestFunc)
			require.NoError(t, err)

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, tc.content)
			}))
			defer ts.Close()

			resp, err := fetchClient.FetchBlob(ctx, &rapb.FetchBlobRequest{
				Uris: []string{ts.URL},
				Qualifiers: []*rapb.Qualifier{
					{
						Name:  fetch_server.ChecksumQualifier,
						Value: checksumQualifierFromContent(t, contentDigest.GetHash(), tc.digestFunc),
					},
				},
				DigestFunction: tc.digestFunc,
			})
			assert.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, int32(0), resp.GetStatus().Code)
			assert.Equal(t, "", resp.GetStatus().Message)
			assert.Contains(t, resp.GetUri(), ts.URL)
			assert.Equal(t, contentDigest.GetHash(), resp.GetBlobDigest().GetHash())
			assert.Equal(t, contentDigest.GetSizeBytes(), resp.GetBlobDigest().GetSizeBytes())
		})
	}
}

// Precompute content to be used in the following tests
const (
	content     = "content"
	contentSize = int64(7)

	contentSHA256 = "ed7002b439e9ac845f22357d822bac1444730fbdb6016d3ec9432297b9ec9f73"
	contentBLAKE3 = "3fba5250be9ac259c56e7250c526bc83bacb4be825f2799d3d59e5b4878dd74e"

	// see checksumQualifierFromContent for logic on recreating these values
	sha256CRI = "sha256-7XACtDnprIRfIjV9giusFERzD722AW0+yUMil7nsn3M="
	blake3CRI = "blake3-P7pSUL6awlnFbnJQxSa8g7rLS+gl8nmdPVnltIeN104="
)

// TestFetchBlobWithCache verifies that the blob in cache is prioritized
// over the externaly fetching from HTTP upstream.
// Also test that if the checksum qualifier and request use different
// hash algorithms, properly replicate the cached blob from the checksum
// digest function to the request's digest function.
func TestFetchBlobWithCache(t *testing.T) {
	for _, tc := range []struct {
		name         string
		contentHash  string
		checksumFunc repb.DigestFunction_Value
		checksumCRI  string
		storageFunc  repb.DigestFunction_Value
	}{
		{
			name:         "checksum_SHA256__storage_SHA256",
			contentHash:  contentSHA256,
			checksumFunc: repb.DigestFunction_SHA256,
			checksumCRI:  sha256CRI,
			storageFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:         "checksum_BLAKE3__storage_BLAKE3",
			contentHash:  contentBLAKE3,
			checksumFunc: repb.DigestFunction_BLAKE3,
			checksumCRI:  blake3CRI,
			storageFunc:  repb.DigestFunction_BLAKE3,
		},
		{
			name:         "checksum_SHA256__storage_BLAKE3",
			contentHash:  contentSHA256,
			checksumFunc: repb.DigestFunction_SHA256,
			checksumCRI:  sha256CRI,
			storageFunc:  repb.DigestFunction_BLAKE3,
		},
		{
			name:         "checksum_BLAKE3__storage_SHA256",
			contentHash:  contentBLAKE3,
			checksumFunc: repb.DigestFunction_BLAKE3,
			checksumCRI:  blake3CRI,
			storageFunc:  repb.DigestFunction_SHA256,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			require.NoError(t, scratchspace.Init())
			clientConn := runFetchServer(ctx, te, t)
			fetchClient := rapb.NewFetchClient(clientConn)

			ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
			require.NoError(t, err)

			err = te.GetCache().Set(ctx, digest.NewResourceName(&repb.Digest{
				Hash:      tc.contentHash,
				SizeBytes: contentSize,
			}, "", resource.CacheType_CAS, tc.checksumFunc).ToProto(), []byte("content"))
			require.NoError(t, err)

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "should not request this", http.StatusForbidden)
			}))
			defer ts.Close()

			resp, err := fetchClient.FetchBlob(ctx, &rapb.FetchBlobRequest{
				Uris: []string{ts.URL},
				Qualifiers: []*rapb.Qualifier{
					{
						Name:  fetch_server.ChecksumQualifier,
						Value: tc.checksumCRI,
					},
				},
				DigestFunction: tc.storageFunc,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, int32(0), resp.GetStatus().Code)

			exist, err := te.GetCache().Contains(ctx, digest.NewResourceName(&repb.Digest{
				Hash:      resp.GetBlobDigest().GetHash(),
				SizeBytes: resp.GetBlobDigest().GetSizeBytes(),
			}, "", resource.CacheType_CAS, tc.storageFunc).ToProto())
			require.NoError(t, err)
			require.True(t, exist)
		})
	}
}

func TestFetchBlobMismatch(t *testing.T) {
	content := "content"
	for _, tc := range []struct {
		name                string
		checksumQualifier   string
		requestedDigestFunc repb.DigestFunction_Value
		expectedDigestFunc  repb.DigestFunction_Value
		expectedHash        string
	}{
		{
			name:                "default_digest_func__sri_sha256",
			checksumQualifier:   sha256CRI,
			requestedDigestFunc: repb.DigestFunction_UNKNOWN,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
			expectedHash:        contentSHA256,
		},
		{
			name:                "default_digest_func__sri_blake3",
			checksumQualifier:   blake3CRI,
			requestedDigestFunc: repb.DigestFunction_UNKNOWN,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
			expectedHash:        contentSHA256,
		},
		{
			name:                "default_digest_func__no_sri",
			checksumQualifier:   "",
			requestedDigestFunc: repb.DigestFunction_UNKNOWN,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
			expectedHash:        contentSHA256,
		},
		{
			name:                "sha256_digest_func__sri_blake3",
			checksumQualifier:   blake3CRI,
			requestedDigestFunc: repb.DigestFunction_SHA256,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
			expectedHash:        contentSHA256,
		},
		{
			name:                "sha256_digest_func__no_sri",
			checksumQualifier:   "",
			requestedDigestFunc: repb.DigestFunction_SHA256,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
			expectedHash:        contentSHA256,
		},
		{
			name:                "blake3_digest_func__sri_sha256",
			checksumQualifier:   sha256CRI,
			requestedDigestFunc: repb.DigestFunction_BLAKE3,
			expectedDigestFunc:  repb.DigestFunction_BLAKE3,
			expectedHash:        contentBLAKE3,
		},
		{
			name:                "blake3_digest_func__no_sri",
			checksumQualifier:   "",
			requestedDigestFunc: repb.DigestFunction_BLAKE3,
			expectedDigestFunc:  repb.DigestFunction_BLAKE3,
			expectedHash:        contentBLAKE3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			require.NoError(t, scratchspace.Init())
			clientConn := runFetchServer(ctx, te, t)
			fetchClient := rapb.NewFetchClient(clientConn)

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, content)
			}))
			defer ts.Close()

			request := &rapb.FetchBlobRequest{
				Uris:           []string{ts.URL},
				DigestFunction: tc.requestedDigestFunc,
			}
			if tc.checksumQualifier != "" {
				request.Qualifiers = []*rapb.Qualifier{
					{
						Name:  fetch_server.ChecksumQualifier,
						Value: tc.checksumQualifier,
					},
				}
			}
			resp, err := fetchClient.FetchBlob(ctx, request)

			assert.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, int32(0), resp.GetStatus().Code)
			assert.Equal(t, "", resp.GetStatus().Message)
			assert.Contains(t, resp.GetUri(), ts.URL)
			assert.Equal(t, tc.expectedHash, resp.GetBlobDigest().GetHash())
		})
	}
}

func TestFetchDirectory(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runFetchServer(ctx, te, t)
	fetchClient := rapb.NewFetchClient(clientConn)

	resp, err := fetchClient.FetchDirectory(ctx, &rapb.FetchDirectoryRequest{})
	assert.EqualError(t, err, "rpc error: code = Unimplemented desc = FetchDirectory is not yet implemented")
	assert.Nil(t, resp)
}
