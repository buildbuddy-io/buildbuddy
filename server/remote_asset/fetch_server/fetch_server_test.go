package fetch_server_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/buildbuddy_server"
	"github.com/buildbuddy-io/buildbuddy/server/cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_asset/fetch_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/rs/zerolog"
	zlog "github.com/rs/zerolog/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

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
		name           string
		content        string
		digestFunc     repb.DigestFunction_Value
		trailingBadURI bool
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
		{
			name:           "successful_mirror_skips_invalid_trailing_uri",
			content:        content,
			digestFunc:     repb.DigestFunction_SHA256,
			trailingBadURI: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			clientConn := runFetchServer(ctx, t, te)
			fetchClient := rapb.NewFetchClient(clientConn)

			contentDigest, err := digest.Compute(bytes.NewReader([]byte(tc.content)), tc.digestFunc)
			require.NoError(t, err)

			var requests atomic.Int64
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				fmt.Fprint(w, tc.content)
			}))
			defer ts.Close()
			uris := []string{ts.URL}
			if tc.trailingBadURI {
				uris = append(uris, "http://%zz")
			}

			resp, err := fetchClient.FetchBlob(ctx, &rapb.FetchBlobRequest{
				Uris: uris,
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
			assert.Equal(t, ts.URL, resp.GetUri())
			assert.Equal(t, contentDigest.GetHash(), resp.GetBlobDigest().GetHash())
			assert.Equal(t, contentDigest.GetSizeBytes(), resp.GetBlobDigest().GetSizeBytes())
			assert.Equal(t, int64(1), requests.Load())
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
		invalidURI   bool
		checksumList bool
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
		{
			name:         "warm_hit_skips_invalid_uri",
			checksumFunc: repb.DigestFunction_SHA256,
			storageFunc:  repb.DigestFunction_SHA256,
			invalidURI:   true,
		},
		{
			name:         "checksum_list_hit_and_conversion",
			checksumFunc: repb.DigestFunction_SHA256,
			storageFunc:  repb.DigestFunction_BLAKE3,
			checksumList: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "storage.tempdir", t.TempDir())
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

			var requests atomic.Int64
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				http.Error(w, "should not request this", http.StatusForbidden)
			}))
			defer ts.Close()
			uris := []string{ts.URL}
			if tc.invalidURI {
				uris = []string{"http://%zz"}
			}
			checksum := checksumQualifierFromContent(t, checksumDigest.GetHash(), tc.checksumFunc)
			if tc.checksumList {
				wrongSHA256, err := digest.Compute(strings.NewReader("wrong"), repb.DigestFunction_SHA256)
				require.NoError(t, err)
				checksum = checksumQualifierFromContent(t, wrongSHA256.GetHash(), repb.DigestFunction_SHA256) + " " + checksum
			}

			resp, err := fetchClient.FetchBlob(ctx, &rapb.FetchBlobRequest{
				Uris: uris,
				Qualifiers: []*rapb.Qualifier{
					{
						Name:  fetch_server.ChecksumQualifier,
						Value: checksum,
					},
				},
				DigestFunction: tc.storageFunc,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, int32(0), resp.GetStatus().Code)
			assert.Equal(t, tc.storageFunc, resp.GetDigestFunction())
			assert.Empty(t, resp.GetUri())
			assert.Zero(t, requests.Load())
			storageDigest, err := digest.Compute(strings.NewReader(content), tc.storageFunc)
			require.NoError(t, err)
			assert.Equal(t, storageDigest, resp.GetBlobDigest())

			exist, err := te.GetCache().Contains(ctx, digest.NewResourceName(&repb.Digest{
				Hash:      resp.GetBlobDigest().GetHash(),
				SizeBytes: resp.GetBlobDigest().GetSizeBytes(),
			}, "", resource.CacheType_CAS, tc.storageFunc).ToProto())
			require.NoError(t, err)
			require.True(t, exist)
			var got bytes.Buffer
			rn := digest.NewCASResourceName(resp.GetBlobDigest(), "", resp.GetDigestFunction())
			require.NoError(t, cachetools.GetBlob(ctx, te.GetByteStreamClient(), rn, &got))
			require.Equal(t, content, got.String())
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
	for _, tc := range []struct {
		name              string
		firstURI          bool
		qualifiers        []*rapb.Qualifier
		wantFirstHeaders  map[string][]string
		wantSecondHeaders map[string][]string
	}{
		{
			name: "single_url",
			qualifiers: []*rapb.Qualifier{
				{Name: fetch_server.BazelHttpHeaderUrlPrefixQualifier + "0:hkey", Value: "hvalue"},
			},
			wantSecondHeaders: map[string][]string{"hkey": {"hvalue"}},
		},
		{
			name:     "second_url",
			firstURI: true,
			qualifiers: []*rapb.Qualifier{
				{Name: fetch_server.BazelHttpHeaderUrlPrefixQualifier + "1:hkey", Value: "hvalue"},
			},
			wantSecondHeaders: map[string][]string{"hkey": {"hvalue"}},
		},
		{
			name:     "multiple_urls",
			firstURI: true,
			qualifiers: []*rapb.Qualifier{
				{Name: fetch_server.BazelHttpHeaderUrlPrefixQualifier + "0:hkey", Value: "hvalue0"},
				{Name: fetch_server.BazelHttpHeaderUrlPrefixQualifier + "1:hkey", Value: "hvalue"},
			},
			wantFirstHeaders:  map[string][]string{"hkey": {"hvalue0"}},
			wantSecondHeaders: map[string][]string{"hkey": {"hvalue"}},
		},
		{
			name: "header_override",
			qualifiers: []*rapb.Qualifier{
				{Name: fetch_server.BazelHttpHeaderPrefixQualifier + "hkey", Value: "hvalue0"},
				{Name: fetch_server.BazelHttpHeaderUrlPrefixQualifier + "0:hkey", Value: "hvalue"},
			},
			wantSecondHeaders: map[string][]string{"hkey": {"hvalue"}},
		},
		{
			name:     "repeated_headers_and_uri_override",
			firstURI: true,
			qualifiers: []*rapb.Qualifier{
				{Name: fetch_server.BazelHttpHeaderPrefixQualifier + "shared", Value: "shared-1"},
				{Name: fetch_server.BazelHttpHeaderPrefixQualifier + "shared", Value: "shared-2"},
				{Name: fetch_server.BazelHttpHeaderPrefixQualifier + "override", Value: "shared-override-1"},
				{Name: fetch_server.BazelHttpHeaderPrefixQualifier + "override", Value: "shared-override-2"},
				{Name: fetch_server.BazelHttpHeaderUrlPrefixQualifier + "0:override", Value: "first-1"},
				{Name: fetch_server.BazelHttpHeaderUrlPrefixQualifier + "0:override", Value: "first-2"},
			},
			wantFirstHeaders: map[string][]string{
				"shared":   {"shared-1", "shared-2"},
				"override": {"first-2"},
			},
			wantSecondHeaders: map[string][]string{
				"shared":   {"shared-1", "shared-2"},
				"override": {"shared-override-1", "shared-override-2"},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			flags.Set(t, "storage.tempdir", t.TempDir())
			require.NoError(t, scratchspace.Init())
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			fetchClient := rapb.NewFetchClient(runFetchServer(ctx, t, te))
			var firstRequests, secondRequests atomic.Int64
			headerKeys := []string{"hkey", "shared", "override"}
			first := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				firstRequests.Add(1)
				for _, key := range headerKeys {
					assert.Equal(t, tc.wantFirstHeaders[key], r.Header.Values(key), key)
				}
				http.Error(w, "no blob here", http.StatusForbidden)
			}))
			defer first.Close()
			second := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				secondRequests.Add(1)
				for _, key := range headerKeys {
					assert.Equal(t, tc.wantSecondHeaders[key], r.Header.Values(key), key)
				}
				fmt.Fprint(w, "some blob")
			}))
			defer second.Close()
			uris := []string{second.URL}
			if tc.firstURI {
				uris = []string{first.URL, second.URL}
			}
			request := &rapb.FetchBlobRequest{
				Uris:       uris,
				Qualifiers: tc.qualifiers,
			}
			resp, err := fetchClient.FetchBlob(ctx, request)
			require.NoError(t, err)
			require.NotNil(t, resp)
			require.Equal(t, int32(gcodes.OK), resp.GetStatus().GetCode(), resp.GetStatus().GetMessage())
			require.Equal(t, second.URL, resp.GetUri())
			if tc.firstURI {
				require.Equal(t, int64(1), firstRequests.Load())
			} else {
				require.Zero(t, firstRequests.Load())
			}
			require.Equal(t, int64(1), secondRequests.Load())
		})
	}
}

func TestFetchBlob_CacheConversionFailureFallsThroughToLaterChecksum(t *testing.T) {
	scratchRoot := t.TempDir()
	flags.Set(t, "storage.tempdir", scratchRoot)
	require.NoError(t, scratchspace.Init())
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	client := rapb.NewFetchClient(runFetchServer(ctx, t, te))
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)
	sha256Digest, err := digest.Compute(strings.NewReader(content), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	blake3Digest, err := digest.Compute(strings.NewReader(content), repb.DigestFunction_BLAKE3)
	require.NoError(t, err)
	require.NoError(t, te.GetCache().Set(ctx, digest.NewCASResourceName(sha256Digest, "", repb.DigestFunction_SHA256).ToProto(), []byte(content)))
	require.NoError(t, te.GetCache().Set(ctx, digest.NewCASResourceName(blake3Digest, "", repb.DigestFunction_BLAKE3).ToProto(), []byte(content)))
	// Make the SHA-256 to BLAKE3 conversion fail. The next matching BLAKE3
	// checksum should still satisfy the request without contacting the origin.
	require.NoError(t, os.RemoveAll(filepath.Join(scratchRoot, "buildbuddy-scratch")))

	resp, err := client.FetchBlob(ctx, &rapb.FetchBlobRequest{
		Uris: []string{"http://%zz"},
		Qualifiers: []*rapb.Qualifier{{
			Name:  fetch_server.ChecksumQualifier,
			Value: sha256CRI + " " + blake3CRI,
		}},
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	require.NoError(t, err)
	require.Equal(t, int32(gcodes.OK), resp.GetStatus().GetCode(), resp.GetStatus().GetMessage())
	require.Equal(t, blake3Digest, resp.GetBlobDigest())
	require.Empty(t, resp.GetUri())
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

func TestFetchDirectory(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	clientConn := runFetchServer(ctx, t, te)
	fetchClient := rapb.NewFetchClient(clientConn)

	resp, err := fetchClient.FetchDirectory(ctx, &rapb.FetchDirectoryRequest{})
	assert.EqualError(t, err, "rpc error: code = Unimplemented desc = FetchDirectory is not yet implemented")
	assert.Nil(t, resp)
}

func TestFetchBlob_InvalidChecksum(t *testing.T) {
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	client := rapb.NewFetchClient(runFetchServer(ctx, t, te))
	var requests atomic.Int64
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		fmt.Fprint(w, content)
	}))
	defer origin.Close()
	for _, tc := range []struct {
		name, checksum, wantMessage string
	}{
		{"empty", "", ""},
		{"unknown_algorithm", "sha999-AAAA", "No supported checksum algorithm"},
		{"missing_separator", "sha256", ""},
		{"empty_hash", "sha256-", ""},
		{"invalid_base64", "sha256-!", ""},
		{"short_hash", "sha256-" + base64.StdEncoding.EncodeToString(make([]byte, 31)), ""},
		{"long_hash", "sha256-" + base64.StdEncoding.EncodeToString(make([]byte, 33)), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.FetchBlob(ctx, &rapb.FetchBlobRequest{
				Uris:       []string{origin.URL},
				Qualifiers: []*rapb.Qualifier{{Name: fetch_server.ChecksumQualifier, Value: tc.checksum}},
			})
			require.Equal(t, gcodes.InvalidArgument, gstatus.Code(err))
			if tc.wantMessage != "" {
				require.Contains(t, gstatus.Convert(err).Message(), tc.wantMessage)
			}
			require.Zero(t, requests.Load(), "invalid integrity constraints must not fetch the origin")
		})
	}
}

func TestFetchBlob_ChecksumSRIList(t *testing.T) {
	flags.Set(t, "storage.tempdir", t.TempDir())
	require.NoError(t, scratchspace.Init())
	wrongSHA256, err := digest.Compute(strings.NewReader("wrong"), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	wrongSHA512, err := digest.Compute(strings.NewReader("wrong"), repb.DigestFunction_SHA512)
	require.NoError(t, err)
	wrongSHA256CRI := checksumQualifierFromContent(t, wrongSHA256.GetHash(), repb.DigestFunction_SHA256)
	wrongSHA512CRI := checksumQualifierFromContent(t, wrongSHA512.GetHash(), repb.DigestFunction_SHA512)

	for _, tc := range []struct {
		name              string
		checksum          string
		chunked           bool
		wantRPCCode       gcodes.Code
		wantResponseCode  gcodes.Code
		wantOriginFetches int64
	}{
		{
			name:              "unsupported_then_matching_supported_known_length",
			checksum:          "sha999-AAAA " + sha256CRI,
			wantResponseCode:  gcodes.OK,
			wantOriginFetches: 1,
		},
		{
			name:              "matching_supported_then_unsupported_chunked",
			checksum:          sha256CRI + " sha999-AAAA",
			chunked:           true,
			wantResponseCode:  gcodes.OK,
			wantOriginFetches: 1,
		},
		{
			name:              "matching_first_then_wrong_same_algorithm",
			checksum:          sha256CRI + " " + wrongSHA256CRI,
			wantResponseCode:  gcodes.OK,
			wantOriginFetches: 1,
		},
		{
			name:              "whitespace_separated_alternatives",
			checksum:          "sha999-AAAA\t" + sha256CRI + "\nsha888-BBBB",
			chunked:           true,
			wantResponseCode:  gcodes.OK,
			wantOriginFetches: 1,
		},
		{
			name:              "same_algorithm_first_wrong_second_matches",
			checksum:          wrongSHA256CRI + " " + sha256CRI,
			wantResponseCode:  gcodes.OK,
			wantOriginFetches: 1,
		},
		{
			name:              "matching_checksum_different_from_storage",
			checksum:          wrongSHA256CRI + " " + sha512CRI,
			wantResponseCode:  gcodes.OK,
			wantOriginFetches: 1,
		},
		{
			name:              "cross_algorithm_first_wrong_second_matches",
			checksum:          wrongSHA512CRI + " " + sha256CRI,
			chunked:           true,
			wantResponseCode:  gcodes.OK,
			wantOriginFetches: 1,
		},
		{
			name:              "no_supported_checksum_algorithms",
			checksum:          "sha999-AAAA sha888-BBBB",
			wantRPCCode:       gcodes.InvalidArgument,
			wantOriginFetches: 0,
		},
		{
			name:              "malformed_supported_token_rejected_even_with_match",
			checksum:          "sha256-not-base64 " + sha256CRI,
			wantRPCCode:       gcodes.InvalidArgument,
			wantOriginFetches: 0,
		},
		{
			name:              "none_match",
			checksum:          wrongSHA256CRI + " " + wrongSHA512CRI,
			wantResponseCode:  gcodes.NotFound,
			wantOriginFetches: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			te := testenv.GetTestEnv(t)
			client := rapb.NewFetchClient(runFetchServer(ctx, t, te))
			var requests atomic.Int64
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				if tc.chunked {
					w.(http.Flusher).Flush()
				}
				fmt.Fprint(w, content)
			}))
			defer origin.Close()

			resp, err := client.FetchBlob(ctx, &rapb.FetchBlobRequest{
				Uris:           []string{origin.URL},
				Qualifiers:     []*rapb.Qualifier{{Name: fetch_server.ChecksumQualifier, Value: tc.checksum}},
				DigestFunction: repb.DigestFunction_SHA256,
			})
			if tc.wantRPCCode != gcodes.OK {
				require.Equal(t, tc.wantRPCCode, gstatus.Code(err))
				require.Nil(t, resp)
			} else {
				require.NoError(t, err)
				require.Equal(t, int32(tc.wantResponseCode), resp.GetStatus().GetCode(), resp.GetStatus().GetMessage())
				if tc.name == "none_match" {
					require.Contains(t, resp.GetStatus().GetMessage(), "did not match any supported checksum")
				}
				if tc.wantResponseCode == gcodes.OK {
					var got bytes.Buffer
					rn := digest.NewCASResourceName(resp.GetBlobDigest(), "", resp.GetDigestFunction())
					require.NoError(t, cachetools.GetBlob(ctx, te.GetByteStreamClient(), rn, &got))
					require.Equal(t, content, got.String())
				}
			}
			require.Equal(t, tc.wantOriginFetches, requests.Load())
		})
	}
}

func TestFetchBlob_CompressionCompatibility(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		for _, chunked := range []bool{false, true} {
			t.Run(fmt.Sprintf("compression=%t/chunked=%t", compressed, chunked), func(t *testing.T) {
				flags.Set(t, "cache.zstd_transcoding_enabled", compressed)
				flags.Set(t, "storage.tempdir", t.TempDir())
				require.NoError(t, scratchspace.Init())
				ctx := context.Background()
				te := testenv.GetTestEnv(t)
				conn := runFetchServer(ctx, t, te)
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if chunked {
						w.(http.Flusher).Flush()
					}
					fmt.Fprint(w, content)
				}))
				defer origin.Close()
				resp, err := rapb.NewFetchClient(conn).FetchBlob(ctx, &rapb.FetchBlobRequest{
					Uris:           []string{origin.URL},
					Qualifiers:     []*rapb.Qualifier{{Name: fetch_server.ChecksumQualifier, Value: sha256CRI}},
					DigestFunction: repb.DigestFunction_SHA256,
				})
				require.NoError(t, err)
				require.Equal(t, int32(gcodes.OK), resp.GetStatus().GetCode(), resp.GetStatus().GetMessage())
				var got bytes.Buffer
				rn := digest.NewCASResourceName(resp.GetBlobDigest(), "", resp.GetDigestFunction())
				require.NoError(t, cachetools.GetBlob(ctx, bspb.NewByteStreamClient(conn), rn, &got))
				require.Equal(t, content, got.String())
			})
		}
	}
}

func TestFetchBlob_FailedDownloadCleansScratchFiles(t *testing.T) {
	scratchRoot := t.TempDir()
	flags.Set(t, "storage.tempdir", scratchRoot)
	require.NoError(t, scratchspace.Init())
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	client := rapb.NewFetchClient(runFetchServer(ctx, t, te))
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No checksum is supplied, so this truncated response takes the scratch-file
		// path even though the origin advertises a content length.
		w.Header().Set("Content-Length", "100")
		fmt.Fprint(w, "partial")
	}))
	defer origin.Close()
	resp, err := client.FetchBlob(ctx, &rapb.FetchBlobRequest{Uris: []string{origin.URL}})
	require.NoError(t, err)
	require.Equal(t, int32(gcodes.NotFound), resp.GetStatus().GetCode())
	entries, err := os.ReadDir(filepath.Join(scratchRoot, "buildbuddy-scratch"))
	require.NoError(t, err)
	require.Empty(t, entries, "failed downloads must not leave partial scratch files")
}

func TestFetchBlob_RedactsURLSecrets(t *testing.T) {
	ctx := context.Background()
	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			fmt.Fprint(w, content)
		case "/redirect-loop":
			w.Header().Set("Location", r.URL.String())
			w.WriteHeader(http.StatusFound)
		case "/redirect":
			// net/http's error includes the invalid Location and its nested parse error.
			w.Header().Set("Location", "http://redirect-user:redirect-password@example.test/%zz?token=redirect-token#redirect-fragment")
			w.WriteHeader(http.StatusFound)
		default:
			http.Error(w, "missing", http.StatusNotFound)
		}
	}))
	defer origin.Close()
	deadOrigin := httptest.NewServer(http.NotFoundHandler())
	deadOrigin.Close()
	tlsOrigin := httptest.NewTLSServer(http.NotFoundHandler())
	defer tlsOrigin.Close()
	for _, tc := range []struct {
		name, uri   string
		code        gcodes.Code
		rpcError    bool
		wantMessage string
	}{
		{"success", origin.URL + "/ok", gcodes.OK, false, ""},
		{"http_error", origin.URL + "/missing", gcodes.NotFound, false, "HTTP 404"},
		{"redirect_parse_error", origin.URL + "/redirect", gcodes.NotFound, false, "request failed"},
		{"redirect_limit", origin.URL + "/redirect-loop", gcodes.NotFound, false, "stopped after 10 redirects"},
		{"tls_error", tlsOrigin.URL + "/asset", gcodes.NotFound, false, "TLS certificate verification failed"},
		{"private_ip", origin.URL + "/ok", gcodes.NotFound, false, "IP address not allowed"},
		{"network_error", deadOrigin.URL + "/asset", gcodes.NotFound, false, "connection refused"},
		{"parse_error", origin.URL + "/%zz", gcodes.InvalidArgument, true, ""},
		{"opaque_uri", "https:opaque-token", gcodes.NotFound, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logFile, err := os.CreateTemp(t.TempDir(), "fetch-logs")
			require.NoError(t, err)
			t.Cleanup(func() { logFile.Close() })
			originalLogger := zlog.Logger
			zlog.Logger = zerolog.New(logFile).Level(zerolog.DebugLevel)
			// Restore only after the environment has stopped its server goroutines.
			t.Cleanup(func() { zlog.Logger = originalLogger })
			te := testenv.GetTestEnv(t)
			runFetchServer(ctx, t, te)
			if tc.name == "private_ip" {
				flags.Set(t, "remote_asset.allowed_private_ips", []string{})
				flags.Set(t, "http.client.allow_localhost", false)
			}
			server, err := fetch_server.NewFetchServer(te)
			require.NoError(t, err)
			uri := strings.Replace(tc.uri, "://", "://origin-user:origin-password@", 1) + "?token=origin-token#origin-fragment"
			resp, err := server.FetchBlob(ctx, &rapb.FetchBlobRequest{
				Uris:         []string{uri},
				InstanceName: tc.name,
				Qualifiers:   []*rapb.Qualifier{{Name: fetch_server.ChecksumQualifier, Value: sha256CRI}},
			})
			var message string
			if tc.rpcError {
				require.Equal(t, tc.code, gstatus.Code(err))
				message = err.Error()
			} else {
				require.NoError(t, err)
				require.Equal(t, int32(tc.code), resp.GetStatus().GetCode())
				require.Equal(t, uri, resp.GetUri(), "protocol URI must remain unchanged")
				message = resp.GetStatus().GetMessage()
			}
			if tc.wantMessage != "" {
				assert.Contains(t, message, tc.wantMessage)
			}
			logs, err := os.ReadFile(logFile.Name())
			require.NoError(t, err)
			if !tc.rpcError {
				require.NotEmpty(t, logs)
			}
			for _, secret := range []string{"origin-user", "origin-password", "origin-token", "origin-fragment", "redirect-user", "redirect-password", "redirect-token", "redirect-fragment", "opaque-token"} {
				assert.NotContains(t, string(logs), secret)
				assert.NotContains(t, message, secret)
			}
			if !tc.rpcError && tc.name != "opaque_uri" {
				parsed, err := url.Parse(tc.uri)
				require.NoError(t, err)
				assert.Contains(t, string(logs), parsed.Host, "retain useful origin context")
			}
		})
	}
}

func TestFetchBlob_DigestConversionClosesScratchFile(t *testing.T) {
	scratchRoot := t.TempDir()
	flags.Set(t, "storage.tempdir", scratchRoot)
	require.NoError(t, scratchspace.Init())
	// Disable finalizers so they cannot hide a leaked descriptor before we check.
	previousGCPercent := debug.SetGCPercent(-1)
	defer debug.SetGCPercent(previousGCPercent)
	ctx := context.Background()
	te := testenv.GetTestEnv(t)
	client := rapb.NewFetchClient(runFetchServer(ctx, t, te))
	ctx, err := prefix.AttachUserPrefixToContext(ctx, te.GetAuthenticator())
	require.NoError(t, err)
	checksumDigest, err := digest.Compute(strings.NewReader(content), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	require.NoError(t, te.GetCache().Set(ctx, digest.NewResourceName(checksumDigest, "", resource.CacheType_CAS, repb.DigestFunction_SHA256).ToProto(), []byte(content)))
	// The origin cannot be fetched, so success requires converting the cached blob.
	resp, err := client.FetchBlob(ctx, &rapb.FetchBlobRequest{
		Uris:           []string{"urn:cached-asset"},
		Qualifiers:     []*rapb.Qualifier{{Name: fetch_server.ChecksumQualifier, Value: sha256CRI}},
		DigestFunction: repb.DigestFunction_BLAKE3,
	})
	require.NoError(t, err)
	require.Equal(t, int32(gcodes.OK), resp.GetStatus().GetCode())
	var got bytes.Buffer
	require.NoError(t, cachetools.GetBlob(ctx, te.GetByteStreamClient(), digest.NewCASResourceName(resp.GetBlobDigest(), "", resp.GetDigestFunction()), &got))
	require.Equal(t, content, got.String())
	entries, err := os.ReadDir(filepath.Join(scratchRoot, "buildbuddy-scratch"))
	require.NoError(t, err)
	require.Empty(t, entries)
	if runtime.GOOS != "linux" {
		return
	}
	descriptors, err := os.ReadDir("/proc/self/fd")
	require.NoError(t, err)
	for _, descriptor := range descriptors {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", descriptor.Name()))
		// Descriptors may close while enumerating.
		if os.IsNotExist(err) {
			continue
		}
		require.NoError(t, err)
		assert.NotContains(t, target, scratchRoot, "digest conversion must close its temporary file, not just unlink it")
	}
}
