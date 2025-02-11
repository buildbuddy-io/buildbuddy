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

func runFetchServer(ctx context.Context, t *testing.T, env *testenv.TestEnv) *grpc.ClientConn {
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	fetchServer, err := fetch_server.NewFetchServer(env)
	require.NoError(t, err)

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	rapb.RegisterFetchServer(grpcServer, fetchServer)

	go runFunc()

	clientConn, err := testenv.LocalGRPCConn(ctx, lis)
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
			name:       "sha1_content",
			content:    "sha1",
			digestFunc: repb.DigestFunction_SHA1,
		},
		{
			name:       "sha256_content",
			content:    "sha256",
			digestFunc: repb.DigestFunction_SHA256,
		},
		{
			name:       "sha512_content",
			content:    "sha512",
			digestFunc: repb.DigestFunction_SHA512,
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
			clientConn := runFetchServer(ctx, t, te)
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
	content = "content"

	// see checksumQualifierFromContent for logic on recreating these values
	sha1CRI   = "sha1-BA8G/XdAkkeNRQd09bowxdp4rMg="
	sha256CRI = "sha256-7XACtDnprIRfIjV9giusFERzD722AW0+yUMil7nsn3M="
	sha512CRI = "sha512-stHShbUZnIX5iNA2ScN+RP093gHl1pxQ/vkGUZYvSBEOk0C2DUmkecTAtT9fB9aQaG3YfSSBk3pRLouF7nxhfw=="
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
		checksumFunc repb.DigestFunction_Value
		storageFunc  repb.DigestFunction_Value
	}{
		{
			name:         "checksum_SHA256__storage_SHA256",
			checksumFunc: repb.DigestFunction_SHA256,
			storageFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:         "checksum_BLAKE3__storage_BLAKE3",
			checksumFunc: repb.DigestFunction_BLAKE3,
			storageFunc:  repb.DigestFunction_BLAKE3,
		},
		{
			name:         "checksum_SHA256__storage_BLAKE3",
			checksumFunc: repb.DigestFunction_SHA256,
			storageFunc:  repb.DigestFunction_BLAKE3,
		},
		{
			name:         "checksum_BLAKE3__storage_SHA256",
			checksumFunc: repb.DigestFunction_BLAKE3,
			storageFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:         "checksum_SHA1__storage_SHA256",
			checksumFunc: repb.DigestFunction_SHA1,
			storageFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:         "checksum_SHA512__storage_SHA256",
			checksumFunc: repb.DigestFunction_SHA512,
			storageFunc:  repb.DigestFunction_SHA256,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			require.NoError(t, scratchspace.Init())
			clientConn := runFetchServer(ctx, t, te)
			fetchClient := rapb.NewFetchClient(clientConn)

			ctx, err := prefix.AttachUserPrefixToContext(ctx, te)
			require.NoError(t, err)

			checksumDigest, err := digest.Compute(bytes.NewReader([]byte(content)), tc.checksumFunc)
			require.NoError(t, err)
			err = te.GetCache().Set(ctx, digest.NewResourceName(checksumDigest, "", resource.CacheType_CAS, tc.checksumFunc).ToProto(), []byte(content))
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
						Value: checksumQualifierFromContent(t, checksumDigest.GetHash(), tc.checksumFunc),
					},
				},
				DigestFunction: tc.storageFunc,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, int32(0), resp.GetStatus().Code)
			assert.Equal(t, tc.storageFunc, resp.GetDigestFunction())

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
	for _, tc := range []struct {
		name                string
		checksumQualifier   string
		requestedDigestFunc repb.DigestFunction_Value
		expectedDigestFunc  repb.DigestFunction_Value
	}{
		{
			name:                "default_digest_func__sri_sha1",
			checksumQualifier:   sha1CRI,
			requestedDigestFunc: repb.DigestFunction_UNKNOWN,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:                "default_digest_func__sri_sha256",
			checksumQualifier:   sha256CRI,
			requestedDigestFunc: repb.DigestFunction_UNKNOWN,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:                "default_digest_func__sri_sha512",
			checksumQualifier:   sha512CRI,
			requestedDigestFunc: repb.DigestFunction_UNKNOWN,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:                "default_digest_func__sri_blake3",
			checksumQualifier:   blake3CRI,
			requestedDigestFunc: repb.DigestFunction_UNKNOWN,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:                "default_digest_func__no_sri",
			checksumQualifier:   "",
			requestedDigestFunc: repb.DigestFunction_UNKNOWN,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:                "sha256_digest_func__sri_blake3",
			checksumQualifier:   blake3CRI,
			requestedDigestFunc: repb.DigestFunction_SHA256,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:                "sha256_digest_func__no_sri",
			checksumQualifier:   "",
			requestedDigestFunc: repb.DigestFunction_SHA256,
			expectedDigestFunc:  repb.DigestFunction_SHA256,
		},
		{
			name:                "blake3_digest_func__sri_sha256",
			checksumQualifier:   sha256CRI,
			requestedDigestFunc: repb.DigestFunction_BLAKE3,
			expectedDigestFunc:  repb.DigestFunction_BLAKE3,
		},
		{
			name:                "blake3_digest_func__no_sri",
			checksumQualifier:   "",
			requestedDigestFunc: repb.DigestFunction_BLAKE3,
			expectedDigestFunc:  repb.DigestFunction_BLAKE3,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			require.NoError(t, scratchspace.Init())
			clientConn := runFetchServer(ctx, t, te)
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
			assert.Equal(t, tc.expectedDigestFunc, resp.GetDigestFunction())
			assert.Contains(t, resp.GetUri(), ts.URL)
			expectedDigest, err := digest.Compute(bytes.NewReader([]byte(content)), tc.expectedDigestFunc)
			require.NoError(t, err)
			assert.Equal(t, expectedDigest.GetHash(), resp.GetBlobDigest().GetHash())
		})
	}
}

func TestSubsequentRequestCacheHit(t *testing.T) {
	for _, tc := range []struct {
		name              string
		digestFunc        repb.DigestFunction_Value
		checksumQualifier string
	}{
		{
			name:              "sha256_digest_func__sri_sha256",
			digestFunc:        repb.DigestFunction_SHA256,
			checksumQualifier: sha256CRI,
		},
		{
			name:              "blake3_digest_func__sri_sha256",
			digestFunc:        repb.DigestFunction_BLAKE3,
			checksumQualifier: sha256CRI,
		},
		{
			name:              "sha256_digest_func__sri_sha512",
			digestFunc:        repb.DigestFunction_SHA256,
			checksumQualifier: sha512CRI,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			require.NoError(t, scratchspace.Init())
			clientConn := runFetchServer(ctx, t, te)
			fetchClient := rapb.NewFetchClient(clientConn)

			// a cache miss would translate to an incoming request handled by http test server
			cacheMissCount := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				cacheMissCount += 1
				fmt.Fprint(w, content)
			}))
			defer ts.Close()

			request := &rapb.FetchBlobRequest{
				Uris:           []string{ts.URL},
				DigestFunction: tc.digestFunc,
			}
			if tc.checksumQualifier != "" {
				request.Qualifiers = []*rapb.Qualifier{
					{
						Name:  fetch_server.ChecksumQualifier,
						Value: tc.checksumQualifier,
					},
				}
			}

			{
				// First fetch request, we expect cache to always miss here
				resp, err := fetchClient.FetchBlob(ctx, request)
				assert.NoError(t, err)
				require.NotNil(t, resp)
				assert.Equal(t, int32(0), resp.GetStatus().Code)
				assert.Equal(t, "", resp.GetStatus().Message)
				assert.Contains(t, resp.GetUri(), ts.URL)
				require.NoError(t, err)
				require.Equal(t, 1, cacheMissCount)
			}

			for range 2 {
				resp, err := fetchClient.FetchBlob(ctx, request)
				assert.NoError(t, err)
				require.NotNil(t, resp)
				require.Equal(t, 1, cacheMissCount, "subsequent fetch requests should get cached blob")
				assert.Equal(t, int32(0), resp.GetStatus().Code)
				assert.Equal(t, "", resp.GetStatus().Message)
				// we are not current storing which url we got the cached blob from
				assert.Equal(t, "", resp.Uri)
			}
		})
	}
}

func TestFetchBlobWithBazelQualifiers(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	require.NoError(t, scratchspace.Init())
	clientConn := runFetchServer(ctx, t, te)
	fetchClient := rapb.NewFetchClient(clientConn)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, r.Header.Get("hkey"), "hvalue")
		fmt.Fprint(w, "some blob")
	}))
	defer ts.Close()

	request := &rapb.FetchBlobRequest{
		Uris: []string{ts.URL},
		Qualifiers: []*rapb.Qualifier{
			{
				Name:  fetch_server.BazelCanonicalIDQualifier,
				Value: "some-bazel-id",
			},
			{
				Name:  fetch_server.BazelHttpHeaderPrefixQualifier + "hkey",
				Value: "hvalue",
			},
		},
	}
	resp, err := fetchClient.FetchBlob(ctx, request)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestFetchBlobWithHeaderUrl(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	require.NoError(t, scratchspace.Init())
	clientConn := runFetchServer(ctx, t, te)
	fetchClient := rapb.NewFetchClient(clientConn)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, []string{"hvalue"}, r.Header.Values("hkey"))
		fmt.Fprint(w, "some blob")
	}))
	defer ts.Close()
	invalidTs := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "no blob here", http.StatusForbidden)
	}))
	defer ts.Close()

	for _, tc := range []struct {
		name       string
		uris       []string
		qualifiers []*rapb.Qualifier
	}{
		{
			name: "single_url",
			uris: []string{
				ts.URL,
			},
			qualifiers: []*rapb.Qualifier{
				{
					Name:  fetch_server.BazelHttpHeaderUrlPrefixQualifier + "0:hkey",
					Value: "hvalue",
				},
			},
		},
		{
			name: "second_url",
			uris: []string{
				invalidTs.URL,
				ts.URL,
			},
			qualifiers: []*rapb.Qualifier{
				{
					Name:  fetch_server.BazelHttpHeaderUrlPrefixQualifier + "1:hkey",
					Value: "hvalue",
				},
			},
		},
		{
			name: "multiple_urls",
			uris: []string{
				invalidTs.URL,
				ts.URL,
			},
			qualifiers: []*rapb.Qualifier{
				{
					Name:  fetch_server.BazelHttpHeaderUrlPrefixQualifier + "0:hkey",
					Value: "hvalue0",
				},
				{
					Name:  fetch_server.BazelHttpHeaderUrlPrefixQualifier + "1:hkey",
					Value: "hvalue",
				},
			},
		},
		{
			name: "header_override",
			uris: []string{
				ts.URL,
			},
			qualifiers: []*rapb.Qualifier{
				{
					Name:  fetch_server.BazelHttpHeaderPrefixQualifier + "hkey",
					Value: "hvalue0",
				},
				{
					Name:  fetch_server.BazelHttpHeaderUrlPrefixQualifier + "0:hkey",
					Value: "hvalue",
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := &rapb.FetchBlobRequest{
				Uris:       tc.uris,
				Qualifiers: tc.qualifiers,
			}
			resp, err := fetchClient.FetchBlob(ctx, request)
			require.NoError(t, err)
			require.NotNil(t, resp)
		})
	}
}

func TestFetchBlobWithUnknownQualifiers(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	require.NoError(t, scratchspace.Init())
	clientConn := runFetchServer(ctx, t, te)
	fetchClient := rapb.NewFetchClient(clientConn)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "some blob")
	}))
	defer ts.Close()

	request := &rapb.FetchBlobRequest{
		Uris: []string{ts.URL},
		Qualifiers: []*rapb.Qualifier{
			{
				Name:  "unknown-qualifier",
				Value: "some-value",
			},
		},
	}
	resp, err := fetchClient.FetchBlob(ctx, request)
	// TODO: Return an error when an unknown qualifier is used
	assert.NoError(t, err)
	assert.NotNil(t, resp)
}

func TestFetchDirectory(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runFetchServer(ctx, t, te)
	fetchClient := rapb.NewFetchClient(clientConn)

	resp, err := fetchClient.FetchDirectory(ctx, &rapb.FetchDirectoryRequest{})
	assert.EqualError(t, err, "rpc error: code = Unimplemented desc = FetchDirectory is not yet implemented")
	assert.Nil(t, resp)
}
