package fetch_server

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"

	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	cspb "github.com/buildbuddy-io/buildbuddy/proto/cache_service"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gerrdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

func runFetchServer(ctx context.Context, t *testing.T, env *testenv.TestEnv) *grpc.ClientConn {
	byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	err = buildbuddy_server.Register(env)
	require.NoError(t, err)
	err = content_addressable_storage_server.Register(env)
	require.NoError(t, err)
	err = cache_server.Register(env)
	require.NoError(t, err)

	// Allow 127.0.0.1 so we can dial the server in the test.
	flags.Set(t, "remote_asset.allowed_private_ips", []string{"127.0.0.0/8"})

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	clientConn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)

	env.SetByteStreamClient(bspb.NewByteStreamClient(clientConn))
	env.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(clientConn))
	env.SetBuildBuddyServiceClient(bbspb.NewBuildBuddyServiceClient(clientConn))
	env.SetCacheClient(cspb.NewCacheClient(clientConn))

	fetchServer, err := NewFetchServer(env)
	require.NoError(t, err)

	rapb.RegisterFetchServer(grpcServer, fetchServer)
	bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)
	bbspb.RegisterBuildBuddyServiceServer(grpcServer, env.GetBuildBuddyServer())
	repb.RegisterContentAddressableStorageServer(grpcServer, env.GetCASServer())
	cspb.RegisterCacheServer(grpcServer, env.GetCacheServer())
	go runFunc()

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
						Name:  ChecksumQualifier,
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

			ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
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
						Name:  ChecksumQualifier,
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
						Name:  ChecksumQualifier,
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
						Name:  ChecksumQualifier,
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
				Name:  BazelCanonicalIDQualifier,
				Value: "some-bazel-id",
			},
			{
				Name:  BazelHttpHeaderPrefixQualifier + "hkey",
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
					Name:  BazelHttpHeaderUrlPrefixQualifier + "0:hkey",
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
					Name:  BazelHttpHeaderUrlPrefixQualifier + "1:hkey",
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
					Name:  BazelHttpHeaderUrlPrefixQualifier + "0:hkey",
					Value: "hvalue0",
				},
				{
					Name:  BazelHttpHeaderUrlPrefixQualifier + "1:hkey",
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
					Name:  BazelHttpHeaderPrefixQualifier + "hkey",
					Value: "hvalue0",
				},
				{
					Name:  BazelHttpHeaderUrlPrefixQualifier + "0:hkey",
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
				Name:  BazelCanonicalIDQualifier,
				Value: "known-qualifier",
			},
			{
				Name:  "unknown-qualifier",
				Value: "some-value",
			},
		},
	}
	resp, err := fetchClient.FetchBlob(ctx, request)
	require.Error(t, err)
	status, ok := gstatus.FromError(err)
	assert.Nil(t, resp)
	assert.True(t, ok)
	require.NotNil(t, status)
	assert.Equal(t, gcodes.InvalidArgument, status.Code())
	assert.Equal(t, "Unsupported qualifiers: unknown-qualifier", status.Message())
	require.Len(t, status.Details(), 1)
	expectedDetail := &gerrdetails.BadRequest{
		FieldViolations: []*gerrdetails.BadRequest_FieldViolation{
			{
				Field:       "qualifiers.name",
				Description: `"unknown-qualifier" not supported`,
			},
		},
	}
	require.IsType(t, expectedDetail, status.Details()[0])
	actualDetail := status.Details()[0].(*gerrdetails.BadRequest)
	assert.True(t, proto.Equal(expectedDetail, actualDetail))
}

func TestFetchBlob_CacheProxy(t *testing.T) {
	localEnv := testenv.GetTestEnv(t)
	require.NoError(t, scratchspace.Init())

	runRemoteCacheServers(t, t.Context(), localEnv)
	clientConn := runFetchServerWithCacheProxy(t.Context(), localEnv, t)
	fetchClient := rapb.NewFetchClient(clientConn)

	content := "hello world"
	contentDigest, err := digest.Compute(bytes.NewReader([]byte(content)), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	remoteFetches := 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteFetches++
		fmt.Fprint(w, content)
	}))
	defer ts.Close()

	request := &rapb.FetchBlobRequest{
		Uris: []string{ts.URL},
		Qualifiers: []*rapb.Qualifier{
			{
				Name:  ChecksumQualifier,
				Value: checksumQualifierFromContent(t, contentDigest.GetHash(), repb.DigestFunction_BLAKE3),
			},
		},
		DigestFunction: repb.DigestFunction_BLAKE3,
	}
	resp, err := fetchClient.FetchBlob(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, contentDigest.GetHash(), resp.GetBlobDigest().GetHash())
	require.Equal(t, 1, remoteFetches)

	// Verify on the second read, you can read the blob from local BSS server without another fetch from the HTTP server.
	resp, err = fetchClient.FetchBlob(t.Context(), request)
	require.NoError(t, err)
	require.Equal(t, contentDigest.GetHash(), resp.GetBlobDigest().GetHash())
	require.Equal(t, 1, remoteFetches)

	// Verify you can not read the blob from the remote cache.
	// To reduce egress across zones, we don't want artifacts propagated to the remote cache.
	// This is okay because remote assets can be refetched if necessary, so the build won't fail
	// if the proxy data is lost.
	rn := digest.NewCASResourceName(contentDigest, "", repb.DigestFunction_BLAKE3)
	buf := bytes.NewBuffer(make([]byte, 0, contentDigest.GetSizeBytes()))
	remoteBSS := localEnv.GetByteStreamClient()
	err = cachetools.GetBlob(t.Context(), remoteBSS, rn, buf)
	require.Error(t, err)
}

// Run remote cache servers with a separate backing cache.
func runRemoteCacheServers(t testing.TB, ctx context.Context, localEnv *testenv.TestEnv) {
	remoteEnv := testenv.GetTestEnv(t)

	if err := byte_stream_server.Register(remoteEnv); err != nil {
		t.Fatal(err)
	}
	if err := buildbuddy_server.Register(remoteEnv); err != nil {
		t.Fatal(err)
	}
	if err := content_addressable_storage_server.Register(remoteEnv); err != nil {
		t.Fatal(err)
	}

	remoteGRPCServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, remoteEnv)
	bspb.RegisterByteStreamServer(remoteGRPCServer, remoteEnv.GetByteStreamServer())
	repb.RegisterContentAddressableStorageServer(remoteGRPCServer, remoteEnv.GetCASServer())
	bbspb.RegisterBuildBuddyServiceServer(remoteGRPCServer, remoteEnv.GetBuildBuddyServer())
	go runFunc()

	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	// Point clients in the proxy to the remote cache.
	localEnv.SetByteStreamClient(bspb.NewByteStreamClient(conn))
	localEnv.SetContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	localEnv.SetBuildBuddyServiceClient(bbspb.NewBuildBuddyServiceClient(conn))
}

func runFetchServerWithCacheProxy(ctx context.Context, env *testenv.TestEnv, t testing.TB) *grpc.ClientConn {
	// Allow 127.0.0.1 so we can dial the server in the test.
	flags.Set(t, "remote_asset.allowed_private_ips", []string{"127.0.0.0/8"})

	// Run the local GRPC servers. They handle local-only reads and writes to the proxy and are not exposed
	// to external GRPC traffic.
	// The fetch server should use these local servers, which do NOT write through to the remote cache.
	localBSS, err := byte_stream_server.NewByteStreamServer(env)
	require.NoError(t, err)
	env.SetLocalByteStreamServer(localBSS)

	localCAS, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	require.NoError(t, err)
	env.SetLocalCASServer(localCAS)

	localCacheServer := cache_server.New(env)

	fetchServer, err := NewFetchServer(env)
	require.NoError(t, err)

	grpcServer, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	bspb.RegisterByteStreamServer(grpcServer, localBSS)
	repb.RegisterContentAddressableStorageServer(grpcServer, localCAS)
	rapb.RegisterFetchServer(grpcServer, fetchServer)
	cspb.RegisterCacheServer(grpcServer, localCacheServer)
	go runFunc()

	conn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	env.SetLocalByteStreamClient(bspb.NewByteStreamClient(conn))
	env.SetLocalContentAddressableStorageClient(repb.NewContentAddressableStorageClient(conn))
	env.SetLocalCacheClient(cspb.NewCacheClient(conn))
	return conn
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

func TestFetchBlobFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		name  string
		paths []string
		code  gcodes.Code
	}{
		{"missing", []string{"404", "404"}, gcodes.NotFound},
		{"forbidden", []string{"403"}, gcodes.NotFound},
		{"unauthorized", []string{"401"}, gcodes.NotFound},
		{"server_error", []string{"503"}, gcodes.Unavailable},
		{"timeout", []string{"408"}, gcodes.Unavailable},
		{"rate_limit", []string{"429"}, gcodes.Unavailable},
		{"reset_then_missing", []string{"reset", "404"}, gcodes.Unavailable},
		{"missing_then_server_error", []string{"404", "503"}, gcodes.Unavailable},
		{"server_error_then_missing", []string{"503", "404"}, gcodes.Unavailable},
		{"checksum_mismatch", []string{"bad_checksum"}, gcodes.InvalidArgument},
		{"checksum_then_missing", []string{"bad_checksum", "404"}, gcodes.InvalidArgument},
		{"checksum_then_server_error", []string{"bad_checksum", "503"}, gcodes.Unavailable},
		{"server_error_then_success", []string{"503", "ok"}, gcodes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			require.NoError(t, scratchspace.Init())
			client := rapb.NewFetchClient(runFetchServer(ctx, t, te))
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/reset":
					conn, _, err := w.(http.Hijacker).Hijack()
					if err != nil {
						t.Error(err)
						return
					}
					conn.Close()
				case "/ok":
					fmt.Fprint(w, content)
				case "/bad_checksum":
					fmt.Fprint(w, "wrong content")
				default:
					code, err := strconv.Atoi(strings.TrimPrefix(r.URL.Path, "/"))
					if err != nil {
						t.Error(err)
						return
					}
					w.WriteHeader(code)
				}
			}))
			defer ts.Close()
			var uris []string
			for _, path := range tc.paths {
				uris = append(uris, ts.URL+"/"+path)
			}
			resp, err := client.FetchBlob(ctx, &rapb.FetchBlobRequest{
				Uris:       uris,
				Qualifiers: []*rapb.Qualifier{{Name: ChecksumQualifier, Value: sha256CRI}},
			})
			if tc.code != gcodes.OK && tc.code != gcodes.NotFound {
				require.Equal(t, tc.code, gstatus.Code(err), "%v", err)
				require.Nil(t, resp)
				return
			}
			require.NoError(t, err)
			require.Equal(t, int32(tc.code), resp.GetStatus().GetCode())
			if tc.code == gcodes.NotFound {
				require.Equal(t, uris[len(uris)-1], resp.GetUri())
				require.Equal(t, strings.Repeat("1", 64), resp.GetBlobDigest().GetHash())
				require.Equal(t, int64(1), resp.GetBlobDigest().GetSizeBytes())
			}
		})
	}
}

func TestFetchBlobRecoversFromTransportFailure(t *testing.T) {
	ctx := t.Context()
	te := testenv.GetTestEnv(t)
	client := rapb.NewFetchClient(runFetchServer(ctx, t, te))
	var attempts atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err != nil {
				t.Error(err)
				return
			}
			conn.Close()
			return
		}
		fmt.Fprint(w, content)
	}))
	defer ts.Close()
	resp, err := client.FetchBlob(ctx, &rapb.FetchBlobRequest{
		Uris:       []string{ts.URL},
		Qualifiers: []*rapb.Qualifier{{Name: ChecksumQualifier, Value: sha256CRI}},
	})
	require.NoError(t, err)
	require.Equal(t, int32(gcodes.OK), resp.GetStatus().GetCode())
	require.Equal(t, int32(2), attempts.Load())
	expected, err := digest.Compute(strings.NewReader(content), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	require.Equal(t, expected.GetHash(), resp.GetBlobDigest().GetHash())
}

// Invoke the handler directly for parent context tests so a client-side gRPC
// cancellation cannot mask an incorrectly returned response status.
func TestFetchBlobTimeouts(t *testing.T) {
	for _, mode := range []string{"fetch_timeout", "rpc_deadline", "rpc_deadline_no_fetch_timeout", "rpc_canceled", "already_canceled"} {
		t.Run(mode, func(t *testing.T) {
			te := testenv.GetTestEnv(t)
			conn := runFetchServer(t.Context(), t, te)
			server, err := NewFetchServer(te)
			require.NoError(t, err)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			var attempts atomic.Int32
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				if mode == "rpc_canceled" {
					cancel()
				}
				<-r.Context().Done()
			}))
			defer ts.Close()
			req := &rapb.FetchBlobRequest{
				Uris:    []string{ts.URL, ts.URL + "/another-mirror"},
				Timeout: durationpb.New(time.Minute),
			}
			switch mode {
			case "fetch_timeout":
				req.Timeout = durationpb.New(100 * time.Millisecond)
			case "rpc_deadline", "rpc_deadline_no_fetch_timeout":
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithTimeout(ctx, 100*time.Millisecond)
				defer deadlineCancel()
				if mode == "rpc_deadline_no_fetch_timeout" {
					req.Timeout = nil
				}
			case "already_canceled":
				cancel()
			}
			var resp *rapb.FetchBlobResponse
			if mode == "fetch_timeout" {
				// Verify the inline status also survives an actual RPC.
				resp, err = rapb.NewFetchClient(conn).FetchBlob(ctx, req)
				require.NoError(t, err)
				require.Equal(t, int32(gcodes.DeadlineExceeded), resp.GetStatus().GetCode())
				require.Contains(t, resp.GetStatus().GetMessage(), "timed out")
				require.Empty(t, resp.GetUri())
				require.Contains(t, resp.GetStatus().GetMessage(), "attempting 1 of 2 URIs")
				require.Contains(t, resp.GetStatus().GetMessage(), ts.URL)
				// Keep the existing workaround for clients that ignore response status.
				require.Equal(t, strings.Repeat("1", 64), resp.GetBlobDigest().GetHash())
				require.Equal(t, int64(1), resp.GetBlobDigest().GetSizeBytes())
				require.NoError(t, ctx.Err())
			} else {
				resp, err = server.FetchBlob(ctx, req)
				require.Nil(t, resp)
				expected := gcodes.Canceled
				if mode == "rpc_deadline" || mode == "rpc_deadline_no_fetch_timeout" {
					expected = gcodes.DeadlineExceeded
				}
				require.Equal(t, expected, gstatus.Code(err))
			}
			if mode == "already_canceled" {
				require.Zero(t, attempts.Load())
			} else {
				// Do not try another mirror once the fetch budget or RPC has expired.
				require.Equal(t, int32(1), attempts.Load())
			}
		})
	}
}

func TestFetchBlobPermanentURIFailures(t *testing.T) {
	te := testenv.GetTestEnv(t)
	runFetchServer(t.Context(), t, te)
	flags.Set(t, "remote_asset.allowed_private_ips", []string{})
	flags.Set(t, "http.client.allow_localhost", false)
	server, err := NewFetchServer(te)
	require.NoError(t, err)
	for _, uri := range []string{"http://127.0.0.1/asset", "ftp://example.com/asset", "relative/path", "http:///path"} {
		t.Run(uri, func(t *testing.T) {
			rsp, err := server.FetchBlob(t.Context(), &rapb.FetchBlobRequest{Uris: []string{uri}})
			require.NoError(t, err)
			require.Equal(t, int32(gcodes.NotFound), rsp.GetStatus().GetCode())
			require.Equal(t, uri, rsp.GetUri())
		})
	}
}

func TestFetchBlobPermanentFailureTriesNextMirror(t *testing.T) {
	te := testenv.GetTestEnv(t)
	require.NoError(t, scratchspace.Init())
	client := rapb.NewFetchClient(runFetchServer(t.Context(), t, te))
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "ftp://example.com/asset", http.StatusFound)
			return
		}
		fmt.Fprint(w, content)
	}))
	defer ts.Close()
	for _, uri := range []string{"ftp://example.com/asset", "relative/path", ts.URL + "/redirect"} {
		t.Run(uri, func(t *testing.T) {
			rsp, err := client.FetchBlob(t.Context(), &rapb.FetchBlobRequest{Uris: []string{uri, ts.URL + "/ok"}})
			require.NoError(t, err)
			require.Equal(t, int32(gcodes.OK), rsp.GetStatus().GetCode())
			require.Equal(t, ts.URL+"/ok", rsp.GetUri())
		})
	}
}

func TestHTTPFetchError(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		code gcodes.Code
	}{
		{"blocked_ip", httpclient.ErrIPNotAllowed, gcodes.NotFound},
		{"nxdomain", &net.DNSError{Err: "no such host", IsNotFound: true}, gcodes.NotFound},
		{"dns_timeout", &net.DNSError{Err: "timeout", IsTimeout: true}, gcodes.Unavailable},
		{"dns_temporary", &net.DNSError{Err: "server failure", IsTemporary: true}, gcodes.Unavailable},
		{"TLS_timeout", errors.New("net/http: TLS handshake timeout"), gcodes.Unavailable},
		{"connection_reset", errors.New("connection reset by peer"), gcodes.Unavailable},
		{"redirect_rejected", status.NotFoundError("stopped after 10 redirects"), gcodes.NotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// net/http wraps dial failures through both net.OpError and url.Error.
			err := &url.Error{Op: "Get", URL: "https://example.com/asset", Err: &net.OpError{Op: "dial", Net: "tcp", Err: tc.err}}
			classified := httpFetchError(err.URL, err)
			require.Equal(t, tc.code, gstatus.Code(classified))
			require.Contains(t, status.Message(classified), err.URL)
			require.Contains(t, status.Message(classified), tc.err.Error())
		})
	}
}

func TestValidateHTTPURL(t *testing.T) {
	for _, uri := range []string{"ftp://example.com/a", "file:///tmp/a", "relative/path", "//example.com/a", "http:///path"} {
		t.Run(uri, func(t *testing.T) {
			u, err := url.Parse(uri)
			require.NoError(t, err)
			require.Equal(t, gcodes.NotFound, gstatus.Code(validateHTTPURL(u)))
		})
	}
	for _, uri := range []string{"https://example.com/a", "http://example.com/a"} {
		u, err := url.Parse(uri)
		require.NoError(t, err)
		require.NoError(t, validateHTTPURL(u))
	}
}

func TestFetchTimeoutErrorPreservesLastAttempt(t *testing.T) {
	// The budget may expire between a 404 response and the next mirror. Keep
	// the 404 diagnostic while making clear that not all mirrors were tried.
	err := fetchTimeoutError(1, 2, fmt.Errorf("https://example.com/first: %w", status.NotFoundError("HTTP 404 Not Found")))
	require.Equal(t, gcodes.DeadlineExceeded, gstatus.Code(err))
	require.Contains(t, status.Message(err), "attempting 1 of 2 URIs")
	require.Contains(t, status.Message(err), "https://example.com/first")
	require.Contains(t, status.Message(err), "404 Not Found")
	err = fetchTimeoutError(0, 2, nil)
	require.Contains(t, status.Message(err), "attempting 0 of 2 URIs")
	require.NotContains(t, status.Message(err), "last fetch error")
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type trackedBody struct {
	io.Reader
	closed bool
}

func (b *trackedBody) Close() error { b.closed = true; return nil }

func TestFetchHTTPRetries(t *testing.T) {
	for _, tc := range []struct {
		name     string
		outcomes []int // 0 represents a transport failure.
		code     gcodes.Code
	}{
		{"TLS_timeout_then_success", []int{0, 200}, gcodes.OK},
		{"exhaust_transport", []int{0, 0, 0}, gcodes.Unavailable},
		{"server_errors_then_success", []int{502, 503, 200}, gcodes.OK},
		{"exhaust_server_errors", []int{503, 503, 503}, gcodes.Unavailable},
		{"rate_limit_then_success", []int{429, 200}, gcodes.OK},
		{"request_timeout_then_success", []int{408, 200}, gcodes.OK},
		{"not_found", []int{404}, gcodes.NotFound},
		{"forbidden", []int{403}, gcodes.NotFound},
		{"bad_request", []int{400}, gcodes.NotFound},
		{"transient_then_terminal", []int{503, 404}, gcodes.NotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			var bodies []*trackedBody
			client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				for _, body := range bodies {
					require.True(t, body.closed, "failed response must be closed before retry")
				}
				require.Less(t, attempts, len(tc.outcomes))
				outcome := tc.outcomes[attempts]
				attempts++
				require.Equal(t, "secret", r.Header.Get("Authorization"))
				if outcome == 0 {
					return nil, errors.New("net/http: TLS handshake timeout")
				}
				body := &trackedBody{Reader: strings.NewReader("body")}
				bodies = append(bodies, body)
				return &http.Response{StatusCode: outcome, Status: fmt.Sprint(outcome), Body: body, Header: make(http.Header)}, nil
			})}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/asset", nil)
			require.NoError(t, err)
			req.Header.Set("Authorization", "secret")
			rsp, err := fetchHTTP(req, client)
			require.Equal(t, tc.code, gstatus.Code(err), "%v", err)
			require.Equal(t, len(tc.outcomes), attempts)
			if tc.code == gcodes.OK {
				require.NotNil(t, rsp)
				require.False(t, bodies[len(bodies)-1].closed)
				rsp.Body.Close()
			} else {
				require.Nil(t, rsp)
			}
			for _, body := range bodies {
				require.True(t, body.closed)
			}
		})
	}
}

func TestFetchHTTPContext(t *testing.T) {
	for _, mode := range []string{"already_canceled", "during_request", "during_backoff", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			wantCode := gcodes.Canceled
			if mode == "deadline" {
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer deadlineCancel()
				wantCode = gcodes.DeadlineExceeded
			}
			attempts := 0
			returned := make(chan struct{})
			client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				attempts++
				switch mode {
				case "during_request":
					cancel()
				case "during_backoff":
					close(returned)
				case "deadline":
					<-r.Context().Done()
				}
				return nil, errors.New("connection reset")
			})}
			if mode == "already_canceled" {
				cancel()
			}
			if mode == "during_backoff" {
				go func() { <-returned; time.Sleep(10 * time.Millisecond); cancel() }()
			}
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://example.com/asset", nil)
			require.NoError(t, err)
			rsp, err := fetchHTTP(req, client)
			require.Nil(t, rsp)
			require.Equal(t, wantCode, gstatus.Code(err))
			if mode == "already_canceled" {
				require.Zero(t, attempts)
			} else {
				require.Equal(t, 1, attempts)
			}
		})
	}
}

func TestFetchHTTPDoesNotRetryPermanentTransportErrors(t *testing.T) {
	for _, failure := range []error{httpclient.ErrIPNotAllowed, &net.DNSError{Err: "no such host", IsNotFound: true}} {
		t.Run(failure.Error(), func(t *testing.T) {
			attempts := 0
			client := &http.Client{Transport: roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				attempts++
				return nil, failure
			})}
			req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://example.com/asset", nil)
			require.NoError(t, err)
			rsp, err := fetchHTTP(req, client)
			require.Nil(t, rsp)
			require.Equal(t, gcodes.NotFound, gstatus.Code(err))
			require.Equal(t, 1, attempts)
		})
	}
}
