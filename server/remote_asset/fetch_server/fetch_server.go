package fetch_server

import (
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gcodes "google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	checksumQualifier = "checksum.sri"
	sha256Prefix      = "sha256-"
	maxHTTPTimeout    = 60 * time.Minute
)

type FetchServer struct {
	env environment.Env
}

func Register(env environment.Env) error {
	// OPTIONAL CACHE API -- only enable if configured.
	if err := checkPreconditions(env); err != nil {
		return nil
	}
	fetchServer, err := NewFetchServer(env)
	if err != nil {
		return status.InternalErrorf("Error initializing FetchServer: %s", err)
	}
	env.SetFetchServer(fetchServer)
	return nil
}

func NewFetchServer(env environment.Env) (*FetchServer, error) {
	if err := checkPreconditions(env); err != nil {
		return nil, err
	}
	return &FetchServer{env: env}, nil
}

func checkPreconditions(env environment.Env) error {
	if env.GetCache() == nil {
		return status.FailedPreconditionError("missing Cache")
	}
	if env.GetByteStreamClient() == nil {
		return status.FailedPreconditionError("missing ByteStreamClient")
	}
	return nil
}

func timeoutFromContext(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return deadline.Sub(time.Now()), true
}

func timeoutHTTPClient(ctx context.Context, protoTimeout *durationpb.Duration) *http.Client {
	timeout := time.Duration(0)
	if ctxDuration, ok := timeoutFromContext(ctx); ok {
		timeout = ctxDuration
	}
	if protoTimeout != nil {
		timeout = protoTimeout.AsDuration()
	}
	if timeout == 0 || timeout > maxHTTPTimeout {
		timeout = maxHTTPTimeout
	}

	tp := &http.Transport{
		Dial: (&net.Dialer{
			Timeout: timeout,
		}).Dial,
		TLSHandshakeTimeout: timeout,
	}

	return &http.Client{
		Timeout:   timeout,
		Transport: tp,
	}
}

func (p *FetchServer) FetchBlob(ctx context.Context, req *rapb.FetchBlobRequest) (*rapb.FetchBlobResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, p.env)
	if err != nil {
		return nil, err
	}

	var expectedSHA256 string

	for _, qualifier := range req.GetQualifiers() {
		if qualifier.GetName() == checksumQualifier && strings.HasPrefix(qualifier.GetValue(), sha256Prefix) {
			b64sha256 := strings.TrimPrefix(qualifier.GetValue(), sha256Prefix)
			sha256, err := base64.StdEncoding.DecodeString(b64sha256)
			if err != nil {
				return nil, status.FailedPreconditionErrorf("Error decoding qualifier %q: %s", qualifier.GetName(), err.Error())
			}
			blobDigest := &repb.Digest{
				Hash: fmt.Sprintf("%x", sha256),
				// The digest size is unknown since the client only sends up
				// the hash. We can look up the size using the Metadata API,
				// which looks up only using the hash, so the size we pass here
				// doesn't matter.
				SizeBytes: 1,
			}
			expectedSHA256 = blobDigest.Hash
			cacheRN := digest.NewResourceName(blobDigest, req.GetInstanceName(), rspb.CacheType_CAS, repb.DigestFunction_SHA256)

			log.CtxInfof(ctx, "Looking up %s in cache", blobDigest.Hash)

			// Lookup metadata to get the correct digest size to be returned to
			// the client.
			cache := p.env.GetCache()
			md, err := cache.Metadata(ctx, cacheRN.ToProto())
			if err != nil {
				log.CtxInfof(ctx, "FetchServer failed to get metadata for %s: %s", expectedSHA256, err)
				continue
			}
			blobDigest.SizeBytes = md.DigestSizeBytes

			// Even though we successfully fetched metadata, we need to renew
			// the cache entry (using Contains()) to ensure that it doesn't
			// expire by the time the client requests it from cache.
			cacheRN = digest.NewResourceName(blobDigest, req.GetInstanceName(), rspb.CacheType_CAS, repb.DigestFunction_SHA256)
			exists, err := cache.Contains(ctx, cacheRN.ToProto())
			if err != nil {
				log.CtxErrorf(ctx, "Failed to renew %s: %s", digest.String(blobDigest), err)
				continue
			}
			if !exists {
				log.CtxInfof(ctx, "Blob %s expired before we could renew it", digest.String(blobDigest))
				continue
			}
			log.CtxInfof(ctx, "FetchServer found %s in cache", digest.String(blobDigest))
			return &rapb.FetchBlobResponse{
				Status:     &statuspb.Status{Code: int32(gcodes.OK)},
				BlobDigest: blobDigest,
			}, nil
		}
	}
	httpClient := timeoutHTTPClient(ctx, req.GetTimeout())

	// Keep track of the last fetch error so that if we fail to fetch, we at
	// least have something we can return to the client.
	var lastFetchErr error

	for _, uri := range req.GetUris() {
		_, err := url.Parse(uri)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("unparsable URI: %q", uri)
		}
		blobDigest, err := mirrorToCache(ctx, p.env.GetByteStreamClient(), req.GetInstanceName(), httpClient, uri, expectedSHA256)
		if err != nil {
			lastFetchErr = err
			log.CtxWarningf(ctx, "Failed to mirror %q to cache: %s", uri, err)
			continue
		}
		return &rapb.FetchBlobResponse{
			Uri:        uri,
			Status:     &statuspb.Status{Code: int32(gcodes.OK)},
			BlobDigest: blobDigest,
		}, nil
	}

	log.CtxInfof(ctx, "Fetch: returning NotFound for %s", req.GetUris())
	return &rapb.FetchBlobResponse{
		Status: &statuspb.Status{
			// Note: returning NotFound here because the other error codes in
			// the proto documentation for FetchBlobResponse.status don't really
			// apply when we fail to fetch. (PermissionDenied and Aborted might
			// make sense in some cases, but it's unclear at the moment whether
			// there is any benefit to using those.)
			Code:    int32(gcodes.NotFound),
			Message: status.Message(lastFetchErr),
		},
	}, nil
}

func (p *FetchServer) FetchDirectory(ctx context.Context, req *rapb.FetchDirectoryRequest) (*rapb.FetchDirectoryResponse, error) {
	return nil, status.UnimplementedError("FetchDirectory is not yet implemented")
}

// mirrorToCache uploads the contents at the given URI to the given cache,
// returning the digest. The fetched contents are checked against the given
// expectedSHA256 (if non-empty), and if there is a mismatch then an error is
// returned.
func mirrorToCache(ctx context.Context, bsClient bspb.ByteStreamClient, remoteInstanceName string, httpClient *http.Client, uri, expectedSHA256 string) (*repb.Digest, error) {
	log.CtxInfof(ctx, "Fetching %s", uri)
	rsp, err := httpClient.Get(uri)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to fetch %q: HTTP GET failed: %s", uri, err)
	}
	defer rsp.Body.Close()
	if rsp.StatusCode < 200 || rsp.StatusCode >= 400 {
		return nil, status.UnavailableErrorf("failed to fetch %q: HTTP %s", uri, err)
	}

	// If we know what the SHA256 should be and the content length is known,
	// then we know the full digest, and can pipe directly from the HTTP
	// response to cache.
	if expectedSHA256 != "" && rsp.ContentLength >= 0 {
		d := &repb.Digest{Hash: expectedSHA256, SizeBytes: rsp.ContentLength}
		rn := digest.NewResourceName(d, remoteInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256)
		if _, err := cachetools.UploadFromReader(ctx, bsClient, rn, rsp.Body); err != nil {
			return nil, status.UnavailableErrorf("failed to upload %s to cache: %s", digest.String(d), err)
		}
		log.CtxInfof(ctx, "Mirrored %s to cache (digest: %s)", uri, digest.String(d))
		return d, nil
	}

	// Otherwise we need to download the whole file before uploading to cache,
	// since we don't know the digest. Download to disk rather than memory,
	// since these downloads can be large.
	//
	// TODO: Support cache uploads with unknown digest length, so that we can
	// pipe directly from the HTTP response to the cache.
	tmpFilePath, err := tempCopy(rsp.Body)
	if err != nil {
		return nil, err
	}
	defer func() {
		if err := os.Remove(tmpFilePath); err != nil {
			log.Errorf("Failed to remove temp file: %s", err)
		}
	}()
	blobDigest, err := cachetools.UploadFile(ctx, bsClient, remoteInstanceName, repb.DigestFunction_SHA256, tmpFilePath)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to add object to cache: %s", err)
	}
	if expectedSHA256 != "" && blobDigest.Hash != expectedSHA256 {
		return nil, status.InvalidArgumentErrorf("response body checksum for %q was %q but wanted %q", uri, blobDigest.Hash, expectedSHA256)
	}
	log.CtxInfof(ctx, "Mirrored %s to cache (digest: %s)", uri, digest.String(blobDigest))
	return blobDigest, nil
}

func tempCopy(r io.Reader) (path string, err error) {
	f, err := scratchspace.CreateTemp("remote-asset-fetch-*")
	if err != nil {
		return "", status.UnavailableErrorf("failed to create temp file for download: %s", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, r); err != nil {
		return "", status.UnavailableErrorf("failed to copy HTTP response to temp file: %s", err)
	}
	return f.Name(), nil
}
