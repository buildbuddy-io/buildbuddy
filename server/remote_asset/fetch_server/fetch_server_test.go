package fetch_server_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"encoding/hex"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/remote_asset/fetch_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

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

	fetchServer, err := fetch_server.NewFetchServer(env)
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
				Name:  fetch_server.BazelCanonicalIDQualifier,
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
				Name:  fetch_server.ChecksumQualifier,
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

	fetchServer, err := fetch_server.NewFetchServer(env)
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

// fetchWaitContext signals when singleflight has registered the caller and
// started waiting. The shared work uses WithoutCancel, so it cannot signal this.
type fetchWaitContext struct {
	context.Context
	once    sync.Once
	waiting chan struct{}
}

func (c *fetchWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

type fetchResult struct {
	response *rapb.FetchBlobResponse
	err      error
}

func awaitFetch[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for fetch")
		var zero T
		return zero
	}
}

func startFetch(t *testing.T, server *fetch_server.FetchServer, ctx context.Context, req *rapb.FetchBlobRequest) <-chan fetchResult {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	t.Cleanup(cancel)
	waitCtx := &fetchWaitContext{Context: ctx, waiting: make(chan struct{})}
	result := make(chan fetchResult, 1)
	go func() {
		resp, err := server.FetchBlob(waitCtx, req)
		result <- fetchResult{response: resp, err: err}
	}()
	awaitFetch(t, waitCtx.waiting)
	return result
}

type fetchOrigin struct {
	url      string
	started  chan struct{}
	canceled chan struct{}
	release  chan struct{}
	once     sync.Once
	requests atomic.Int32
	status   atomic.Int32
}

func newFetchOrigin(t *testing.T) *fetchOrigin {
	t.Helper()
	o := &fetchOrigin{
		started:  make(chan struct{}, 16),
		canceled: make(chan struct{}, 16),
		release:  make(chan struct{}),
	}
	o.status.Store(http.StatusOK)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		o.requests.Add(1)
		o.started <- struct{}{}
		select {
		case <-o.release:
			w.WriteHeader(int(o.status.Load()))
			fmt.Fprint(w, content)
		case <-r.Context().Done():
			o.canceled <- struct{}{}
		}
	}))
	o.url = ts.URL
	t.Cleanup(ts.Close)
	t.Cleanup(o.unblock)
	return o
}

func (o *fetchOrigin) unblock() {
	o.once.Do(func() { close(o.release) })
}

func newDirectFetchServer(t *testing.T, env *testenv.TestEnv) *fetch_server.FetchServer {
	t.Helper()
	require.NoError(t, scratchspace.Init())
	// Dedupe tests count origin requests per flight; keep retries out of it.
	flags.Set(t, "remote_asset.max_fetch_retries", 0)
	conn := runFetchServer(context.Background(), t, env)
	t.Cleanup(func() { conn.Close() })
	server, err := fetch_server.NewFetchServer(env)
	require.NoError(t, err)
	return server
}

func fetchMissRequest(url string) *rapb.FetchBlobRequest {
	return &rapb.FetchBlobRequest{
		Uris:       []string{url},
		Qualifiers: []*rapb.Qualifier{{Name: fetch_server.ChecksumQualifier, Value: sha256CRI}},
	}
}

func TestFetchBlobDedupeConcurrentMisses(t *testing.T) {
	server := newDirectFetchServer(t, testenv.GetTestEnv(t))
	origin := newFetchOrigin(t)
	req := fetchMissRequest(origin.url)
	results := make([]<-chan fetchResult, 8)
	for i := range results {
		// Distinct but equal protos must share work, not just identical pointers.
		results[i] = startFetch(t, server, context.Background(), proto.Clone(req).(*rapb.FetchBlobRequest))
	}
	awaitFetch(t, origin.started)
	origin.unblock()
	var responses []*rapb.FetchBlobResponse
	for _, result := range results {
		r := awaitFetch(t, result)
		require.NoError(t, r.err)
		require.Equal(t, int32(gcodes.OK), r.response.GetStatus().GetCode())
		require.NotNil(t, r.response.GetBlobDigest())
		responses = append(responses, r.response)
	}
	require.Equal(t, int32(1), origin.requests.Load())
	for _, resp := range responses[1:] {
		require.True(t, proto.Equal(responses[0], resp))
		require.NotSame(t, responses[0], resp)
		require.NotSame(t, responses[0].Status, resp.Status)
		require.NotSame(t, responses[0].BlobDigest, resp.BlobDigest)
	}
	responses[0].BlobDigest.Hash = "changed by caller"
	assert.NotEqual(t, responses[0].BlobDigest.Hash, responses[1].BlobDigest.Hash)
}

func TestFetchBlobDedupeUsesFullRequest(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*rapb.FetchBlobRequest)
	}{
		{"instance", func(r *rapb.FetchBlobRequest) { r.InstanceName = "other" }},
		{"digest", func(r *rapb.FetchBlobRequest) { r.DigestFunction = repb.DigestFunction_SHA1 }},
		{"timeout", func(r *rapb.FetchBlobRequest) { r.Timeout = durationpb.New(time.Minute) }},
		{"oldest_content", func(r *rapb.FetchBlobRequest) { r.OldestContentAccepted = timestamppb.New(time.Unix(1, 0)) }},
		{"uri_order", func(r *rapb.FetchBlobRequest) { r.Uris[0], r.Uris[1] = r.Uris[1], r.Uris[0] }},
		{"header", func(r *rapb.FetchBlobRequest) {
			r.Qualifiers = append(r.Qualifiers, &rapb.Qualifier{Name: fetch_server.BazelHttpHeaderPrefixQualifier + "Authorization", Value: "Bearer other"})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newDirectFetchServer(t, testenv.GetTestEnv(t))
			origin := newFetchOrigin(t)
			req := fetchMissRequest(origin.url + "/first")
			req.Uris = append(req.Uris, origin.url+"/second")
			otherReq := proto.Clone(req).(*rapb.FetchBlobRequest)
			tc.change(otherReq)

			first := startFetch(t, server, context.Background(), req)
			awaitFetch(t, origin.started)
			second := startFetch(t, server, context.Background(), otherReq)
			// Both requests must reach the origin before either can complete.
			awaitFetch(t, origin.started)
			origin.unblock()
			for _, result := range []<-chan fetchResult{first, second} {
				r := awaitFetch(t, result)
				require.NoError(t, r.err)
				require.Equal(t, int32(gcodes.OK), r.response.GetStatus().GetCode())
			}
			require.Equal(t, int32(2), origin.requests.Load())
		})
	}
}

func TestFetchBlobDedupeSeparatesAuthScopes(t *testing.T) {
	for _, group := range []string{"GROUP1", "GROUP2"} {
		t.Run(group, func(t *testing.T) {
			env := testenv.GetTestEnv(t)
			auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("USER1", "GROUP1", "USER2", group))
			env.SetAuthenticator(auth)
			server := newDirectFetchServer(t, env)
			ctx1, err := auth.WithAuthenticatedUser(context.Background(), "USER1")
			require.NoError(t, err)
			ctx2, err := auth.WithAuthenticatedUser(context.Background(), "USER2")
			require.NoError(t, err)
			require.NotEqual(t, auth.TrustedJWTFromAuthContext(ctx1), auth.TrustedJWTFromAuthContext(ctx2))
			origin := newFetchOrigin(t)
			// No cache writes needed: this test only checks work isolation.
			origin.status.Store(http.StatusServiceUnavailable)
			req := &rapb.FetchBlobRequest{Uris: []string{origin.url}}
			first := startFetch(t, server, ctx1, req)
			awaitFetch(t, origin.started)
			second := startFetch(t, server, ctx2, req)
			awaitFetch(t, origin.started)
			origin.unblock()
			for _, result := range []<-chan fetchResult{first, second} {
				r := awaitFetch(t, result)
				require.NoError(t, r.err)
				require.Equal(t, int32(gcodes.NotFound), r.response.GetStatus().GetCode())
			}
			require.Equal(t, int32(2), origin.requests.Load())
		})
	}
}

func TestFetchBlobDedupeCancellation(t *testing.T) {
	for _, cancelAll := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancel_all=%t", cancelAll), func(t *testing.T) {
			server := newDirectFetchServer(t, testenv.GetTestEnv(t))
			origin := newFetchOrigin(t)
			req := fetchMissRequest(origin.url)
			ctx1, cancel1 := context.WithCancel(context.Background())
			defer cancel1()
			ctx2, cancel2 := context.WithCancel(context.Background())
			defer cancel2()
			first := startFetch(t, server, ctx1, req)
			awaitFetch(t, origin.started)
			second := startFetch(t, server, ctx2, req)
			cancel1()
			require.ErrorIs(t, awaitFetch(t, first).err, context.Canceled)
			if cancelAll {
				cancel2()
				require.ErrorIs(t, awaitFetch(t, second).err, context.Canceled)
				awaitFetch(t, origin.canceled)
			} else {
				origin.unblock()
				r := awaitFetch(t, second)
				require.NoError(t, r.err)
				require.Equal(t, int32(gcodes.OK), r.response.GetStatus().GetCode())
			}
			require.Equal(t, int32(1), origin.requests.Load())
		})
	}
}

func TestFetchBlobDedupeTimeouts(t *testing.T) {
	for _, tc := range []struct {
		name           string
		requestTimeout time.Duration
		rpcTimeout     time.Duration
	}{
		{"request_timeout", time.Second, 30 * time.Second},
		{"sole_waiter_rpc_deadline", 30 * time.Second, time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newDirectFetchServer(t, testenv.GetTestEnv(t))
			origin := newFetchOrigin(t)
			req := fetchMissRequest(origin.url)
			req.Timeout = durationpb.New(tc.requestTimeout)
			ctx, cancel := context.WithTimeout(context.Background(), tc.rpcTimeout)
			defer cancel()
			result := startFetch(t, server, ctx, req)
			awaitFetch(t, origin.started)

			r := awaitFetch(t, result)
			if tc.rpcTimeout < tc.requestTimeout {
				require.ErrorIs(t, r.err, context.DeadlineExceeded)
			} else {
				require.NoError(t, r.err)
				require.NoError(t, ctx.Err(), "request timeout must not wait for the RPC deadline")
				// Preserve the helper's existing HTTP failure response semantics.
				require.Equal(t, int32(gcodes.NotFound), r.response.GetStatus().GetCode())
				require.Contains(t, r.response.GetStatus().GetMessage(), "context deadline exceeded")
			}
			// The origin stays blocked unless the shared fetch is canceled.
			awaitFetch(t, origin.canceled)
			require.Equal(t, int32(1), origin.requests.Load())
		})
	}
}

func TestFetchBlobDedupeRetriesFailedFlight(t *testing.T) {
	server := newDirectFetchServer(t, testenv.GetTestEnv(t))
	origin := newFetchOrigin(t)
	origin.status.Store(http.StatusServiceUnavailable)
	req := fetchMissRequest(origin.url)
	first := startFetch(t, server, context.Background(), req)
	awaitFetch(t, origin.started)
	second := startFetch(t, server, context.Background(), req)
	origin.unblock()
	for _, result := range []<-chan fetchResult{first, second} {
		r := awaitFetch(t, result)
		require.NoError(t, r.err)
		require.Equal(t, int32(gcodes.NotFound), r.response.GetStatus().GetCode())
	}
	require.Equal(t, int32(1), origin.requests.Load())

	origin.status.Store(http.StatusOK)
	resp, err := server.FetchBlob(context.Background(), req)
	require.NoError(t, err)
	require.Equal(t, int32(gcodes.OK), resp.GetStatus().GetCode())
	require.Equal(t, int32(2), origin.requests.Load())
}

func TestFetchBlobRetries(t *testing.T) {
	// dropConnection sends a partial body and then closes the connection,
	// simulating a connection reset mid-download.
	dropConnection := func(w http.ResponseWriter) {
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, content[:3])
		w.(http.Flusher).Flush()
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}
	for _, tc := range []struct {
		name              string
		checksumQualifier string
		// handler serves the given (1-indexed) request to the server.
		handler          func(w http.ResponseWriter, attempt int64)
		expectedCode     gcodes.Code
		expectedRequests int64
	}{
		{
			name: "retries_server_errors",
			handler: func(w http.ResponseWriter, attempt int64) {
				if attempt < 3 {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				fmt.Fprint(w, content)
			},
			expectedCode:     gcodes.OK,
			expectedRequests: 3,
		},
		{
			name: "retries_too_many_requests",
			handler: func(w http.ResponseWriter, attempt int64) {
				if attempt < 2 {
					w.WriteHeader(http.StatusTooManyRequests)
					return
				}
				fmt.Fprint(w, content)
			},
			expectedCode:     gcodes.OK,
			expectedRequests: 2,
		},
		{
			name:              "retries_dropped_connection_streaming_upload",
			checksumQualifier: sha256CRI,
			handler: func(w http.ResponseWriter, attempt int64) {
				if attempt < 2 {
					dropConnection(w)
					return
				}
				fmt.Fprint(w, content)
			},
			expectedCode:     gcodes.OK,
			expectedRequests: 2,
		},
		{
			name: "retries_dropped_connection_temp_file",
			handler: func(w http.ResponseWriter, attempt int64) {
				if attempt < 2 {
					dropConnection(w)
					return
				}
				fmt.Fprint(w, content)
			},
			expectedCode:     gcodes.OK,
			expectedRequests: 2,
		},
		{
			name: "gives_up_after_max_retries",
			handler: func(w http.ResponseWriter, attempt int64) {
				w.WriteHeader(http.StatusBadGateway)
			},
			expectedCode:     gcodes.NotFound,
			expectedRequests: 3, // 1 attempt + 2 retries
		},
		{
			name: "does_not_retry_not_found",
			handler: func(w http.ResponseWriter, attempt int64) {
				w.WriteHeader(http.StatusNotFound)
			},
			expectedCode:     gcodes.NotFound,
			expectedRequests: 1,
		},
		{
			name:              "does_not_retry_checksum_mismatch",
			checksumQualifier: sha256CRI,
			handler: func(w http.ResponseWriter, attempt int64) {
				fmt.Fprint(w, "not the expected content")
			},
			expectedCode:     gcodes.NotFound,
			expectedRequests: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			require.NoError(t, scratchspace.Init())
			flags.Set(t, "remote_asset.max_fetch_retries", 2)
			flags.Set(t, "remote_asset.fetch_retry_initial_backoff", time.Millisecond)
			clientConn := runFetchServer(ctx, t, te)
			fetchClient := rapb.NewFetchClient(clientConn)

			var requests atomic.Int64
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				tc.handler(w, requests.Add(1))
			}))
			defer ts.Close()

			request := &rapb.FetchBlobRequest{Uris: []string{ts.URL}}
			if tc.checksumQualifier != "" {
				request.Qualifiers = []*rapb.Qualifier{
					{Name: fetch_server.ChecksumQualifier, Value: tc.checksumQualifier},
				}
			}
			resp, err := fetchClient.FetchBlob(ctx, request)
			require.NoError(t, err)
			assert.Equal(t, int32(tc.expectedCode), resp.GetStatus().GetCode(), "status: %s", resp.GetStatus().GetMessage())
			assert.Equal(t, tc.expectedRequests, requests.Load())
			if tc.expectedCode == gcodes.OK {
				expectedDigest, err := digest.Compute(strings.NewReader(content), repb.DigestFunction_SHA256)
				require.NoError(t, err)
				assert.Equal(t, expectedDigest.GetHash(), resp.GetBlobDigest().GetHash())
			}
		})
	}
}

func TestIsRetryableTransportError(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	httpsURL := &url.URL{Scheme: "https", Host: "example.com"}
	for _, tc := range []struct {
		name      string
		ctx       context.Context
		url       *url.URL
		err       error
		retryable bool
	}{
		{"connection_reset", context.Background(), httpsURL, &url.Error{Op: "Get", Err: syscall.ECONNRESET}, true},
		{"tls_handshake_timeout", context.Background(), httpsURL, &url.Error{Op: "Get", Err: errors.New("net/http: TLS handshake timeout")}, true},
		{"dns_temporary", context.Background(), httpsURL, &url.Error{Op: "Get", Err: &net.DNSError{Err: "server misbehaving", IsTemporary: true}}, true},
		{"dns_not_found", context.Background(), httpsURL, &url.Error{Op: "Get", Err: &net.DNSError{Err: "no such host", IsNotFound: true}}, false},
		{"certificate_verification", context.Background(), httpsURL, &url.Error{Op: "Get", Err: &tls.CertificateVerificationError{Err: errors.New("unknown authority")}}, false},
		{"ip_not_allowed", context.Background(), httpsURL, &url.Error{Op: "Get", Err: fmt.Errorf("dial: %w", httpclient.ErrIPNotAllowed)}, false},
		{"unsupported_scheme", context.Background(), &url.URL{Scheme: "ftp", Host: "example.com"}, &url.Error{Op: "Get", Err: errors.New("unsupported protocol scheme \"ftp\"")}, false},
		{"context_canceled", canceledCtx, httpsURL, &url.Error{Op: "Get", Err: context.Canceled}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.retryable, fetch_server.IsRetryableTransportError(tc.ctx, tc.url, tc.err))
		})
	}
}

func TestFetchBlobDoesNotRetryCertificateErrors(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	require.NoError(t, scratchspace.Init())
	flags.Set(t, "remote_asset.max_fetch_retries", 2)
	flags.Set(t, "remote_asset.fetch_retry_initial_backoff", time.Millisecond)
	clientConn := runFetchServer(ctx, t, te)
	fetchClient := rapb.NewFetchClient(clientConn)

	// The fetch server does not trust the httptest CA, so every attempt fails
	// during the TLS handshake and never reaches the handler. Count handshakes
	// instead of requests.
	var handshakes atomic.Int64
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, content)
	}))
	ts.TLS = &tls.Config{
		GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
			handshakes.Add(1)
			return nil, nil
		},
	}
	ts.StartTLS()
	defer ts.Close()

	resp, err := fetchClient.FetchBlob(ctx, &rapb.FetchBlobRequest{Uris: []string{ts.URL}})
	require.NoError(t, err)
	assert.Equal(t, int32(gcodes.NotFound), resp.GetStatus().GetCode())
	assert.Contains(t, resp.GetStatus().GetMessage(), "certificate")
	assert.Equal(t, int64(1), handshakes.Load())
}

func TestFetchBlobValidatesURIsLazily(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	require.NoError(t, scratchspace.Init())
	flags.Set(t, "remote_asset.max_fetch_retries", 2)
	flags.Set(t, "remote_asset.fetch_retry_initial_backoff", time.Millisecond)
	clientConn := runFetchServer(ctx, t, te)
	fetchClient := rapb.NewFetchClient(clientConn)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, content)
	}))
	defer ts.Close()
	const badURI = "::not a uri"

	// A malformed fallback URI must not fail a request satisfied by an
	// earlier URI, with or without retry rounds.
	resp, err := fetchClient.FetchBlob(ctx, &rapb.FetchBlobRequest{Uris: []string{ts.URL, badURI}})
	require.NoError(t, err)
	assert.Equal(t, int32(gcodes.OK), resp.GetStatus().GetCode(), "status: %s", resp.GetStatus().GetMessage())
	assert.Equal(t, ts.URL, resp.GetUri())

	// A malformed URI that is attempted is still rejected.
	_, err = fetchClient.FetchBlob(ctx, &rapb.FetchBlobRequest{Uris: []string{badURI, ts.URL}})
	require.Error(t, err)
	assert.Equal(t, gcodes.InvalidArgument, gstatus.Code(err))
}

func TestFetchBlobRetriesWithMirrors(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	require.NoError(t, scratchspace.Init())
	flags.Set(t, "remote_asset.max_fetch_retries", 2)
	flags.Set(t, "remote_asset.fetch_retry_initial_backoff", time.Millisecond)
	clientConn := runFetchServer(ctx, t, te)
	fetchClient := rapb.NewFetchClient(clientConn)

	// The first mirror permanently fails, so it should only be tried once.
	var notFoundRequests atomic.Int64
	notFound := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		notFoundRequests.Add(1)
		w.WriteHeader(http.StatusNotFound)
	}))
	defer notFound.Close()
	// The second mirror fails transiently once, then succeeds.
	var flakyRequests atomic.Int64
	flaky := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if flakyRequests.Add(1) < 2 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, content)
	}))
	defer flaky.Close()

	resp, err := fetchClient.FetchBlob(ctx, &rapb.FetchBlobRequest{
		Uris: []string{notFound.URL, flaky.URL},
	})
	require.NoError(t, err)
	assert.Equal(t, int32(gcodes.OK), resp.GetStatus().GetCode(), "status: %s", resp.GetStatus().GetMessage())
	assert.Equal(t, flaky.URL, resp.GetUri())
	assert.Equal(t, int64(1), notFoundRequests.Load())
	assert.Equal(t, int64(2), flakyRequests.Load())
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
