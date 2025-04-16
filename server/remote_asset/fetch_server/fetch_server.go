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
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/types/known/durationpb"

	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gerrdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gcodes "google.golang.org/grpc/codes"
	gstatus "google.golang.org/grpc/status"
)

var (
	allowedPrivateIPs = flag.Slice("remote_asset.allowed_private_ips", []string{}, "Allowed IP ranges for fetching remote assets. Private IPs are disallowed by default.")
)

const (
	ChecksumQualifier                 = "checksum.sri"
	BazelCanonicalIDQualifier         = "bazel.canonical_id"
	BazelHttpHeaderPrefixQualifier    = "http_header:"
	BazelHttpHeaderUrlPrefixQualifier = "http_header_url:"

	maxHTTPTimeout = 60 * time.Minute
)

// makeUnsupportedQualifiersErrStatus creates a gRPC status error that includes a list of unsupported qualifiers.
func makeUnsupportedQualifiersErrStatus(qualifierNames []string) error {
	fieldViolations := make([]*gerrdetails.BadRequest_FieldViolation, 0, len(qualifierNames))
	for _, name := range qualifierNames {
		fieldViolations = append(fieldViolations, &gerrdetails.BadRequest_FieldViolation{
			Field:       "qualifiers.name",
			Description: fmt.Sprintf("%q not supported", name),
		})
	}
	s := gstatus.New(gcodes.InvalidArgument, fmt.Sprintf("Unsupported qualifiers: %s", strings.Join(qualifierNames, ", ")))
	s, err := s.WithDetails(&gerrdetails.BadRequest{FieldViolations: fieldViolations})
	// should never happen
	if err != nil {
		log.Warningf("Failed to encode qualifier field violation: %v", err)
	}
	return s.Err()
}

type FetchServer struct {
	env                  environment.Env
	allowedPrivateIPNets []*net.IPNet
}

func Register(env *real_environment.RealEnv) error {
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
	allowedPrivateIPNets := make([]*net.IPNet, 0, len(*allowedPrivateIPs))
	for _, r := range *allowedPrivateIPs {
		_, ipNet, err := net.ParseCIDR(r)
		if err != nil {
			return nil, fmt.Errorf("parse 'remote_asset.allowed_private_ips': %w", err)
		}
		allowedPrivateIPNets = append(allowedPrivateIPNets, ipNet)
	}
	return &FetchServer{
		env:                  env,
		allowedPrivateIPNets: allowedPrivateIPNets,
	}, nil
}

func checkPreconditions(env environment.Env) error {
	if env.GetCache() == nil {
		return status.FailedPreconditionError("missing Cache")
	}
	return nil
}

func timeoutFromContext(ctx context.Context) (time.Duration, bool) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return 0, false
	}
	return time.Until(deadline), true
}

// computeRequestTimeout determines the overall timeout for the request.
func (s *FetchServer) computeRequestTimeout(ctx context.Context, protoTimeout *durationpb.Duration) time.Duration {
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
	return timeout
}

// parseChecksumQualifier returns a digest function and digest hash
// given a "checksum.sri" qualifier.
func parseChecksumQualifier(qualifier *rapb.Qualifier) (repb.DigestFunction_Value, string, error) {
	for _, digestFunc := range digest.SupportedDigestFunctions() {
		pr := fmt.Sprintf("%s-", strings.ToLower(repb.DigestFunction_Value_name[int32(digestFunc)]))
		if strings.HasPrefix(qualifier.GetValue(), pr) {
			b64hash := strings.TrimPrefix(qualifier.GetValue(), pr)
			decodedHash, err := base64.StdEncoding.DecodeString(b64hash)
			if err != nil {
				return repb.DigestFunction_UNKNOWN, "", status.FailedPreconditionErrorf("Error decoding qualifier %q: %s", qualifier.GetName(), err.Error())
			}
			expectedChecksum := fmt.Sprintf("%x", decodedHash)
			return digestFunc, expectedChecksum, nil
		}
	}
	return repb.DigestFunction_UNKNOWN, "", nil
}

func (p *FetchServer) FetchBlob(ctx context.Context, req *rapb.FetchBlobRequest) (*rapb.FetchBlobResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, p.env)
	if err != nil {
		return nil, err
	}

	storageFunc := req.GetDigestFunction()
	if storageFunc == repb.DigestFunction_UNKNOWN {
		storageFunc = repb.DigestFunction_SHA256
	}
	var unsupportedQualifierNames []string
	sharedHeader := make(http.Header)
	uriHeaders := make(map[int]http.Header)
	var checksumFunc repb.DigestFunction_Value
	var expectedChecksum string
	for _, qualifier := range req.GetQualifiers() {
		if qualifier.GetName() == ChecksumQualifier {
			checksumFunc, expectedChecksum, err = parseChecksumQualifier(qualifier)
			if err != nil {
				return nil, err
			}
			continue
		}
		if strings.HasPrefix(qualifier.GetName(), BazelHttpHeaderPrefixQualifier) {
			sharedHeader.Add(
				strings.TrimPrefix(qualifier.GetName(), BazelHttpHeaderPrefixQualifier),
				qualifier.GetValue(),
			)
			continue
		}
		if strings.HasPrefix(qualifier.GetName(), BazelHttpHeaderUrlPrefixQualifier) {
			idxAndKey := strings.TrimPrefix(qualifier.GetName(), BazelHttpHeaderUrlPrefixQualifier)
			halves := strings.Split(idxAndKey, ":")
			if len(halves) != 2 {
				// The http_header_url qualifier should be in the form
				//   http_header_url:<url_index>:<header_name>
				// Note: Avoid raising log level above DEBUG.
				// The header name + value may contains sensitive information.
				log.CtxDebugf(ctx, "Invalid http_header_url qualifier: %s", idxAndKey)
				continue
			}
			uriIndex, err := strconv.Atoi(halves[0])
			if err != nil {
				// The http_header_url qualifier should be in the form
				//   http_header_url:<url_index>:<header_name>
				log.CtxWarningf(ctx, "Failed to decode URI index: %s", err)
				continue
			}
			if _, found := uriHeaders[uriIndex]; !found {
				// If the URI index is not found, create a new header map.
				uriHeaders[uriIndex] = make(http.Header)
			}
			uriHeaders[uriIndex].Add(halves[1], qualifier.GetValue())
			continue
		}
		if qualifier.GetName() == BazelCanonicalIDQualifier {
			// TODO: Implement canonical ID handling.
			continue
		}
		unsupportedQualifierNames = append(unsupportedQualifierNames, qualifier.GetName())
	}
	if len(unsupportedQualifierNames) > 0 {
		return nil, makeUnsupportedQualifiersErrStatus(unsupportedQualifierNames)
	}
	if len(expectedChecksum) != 0 {
		blobDigest := p.findBlobInCache(ctx, req.GetInstanceName(), checksumFunc, expectedChecksum)
		// If the digestFunc is supplied and differ from the checksum sri,
		// after looking up the cached blob using checksum sri, re-upload
		// that blob using the requested digestFunc.
		if blobDigest != nil && checksumFunc != storageFunc {
			blobDigest = p.rewriteToCache(ctx, blobDigest, req.GetInstanceName(), checksumFunc, storageFunc)
		}

		if blobDigest != nil {
			return &rapb.FetchBlobResponse{
				Status:         &statuspb.Status{Code: int32(gcodes.OK)},
				BlobDigest:     blobDigest,
				DigestFunction: storageFunc,
			}, nil
		}
	}

	httpClient := httpclient.NewWithAllowedPrivateIPs(p.allowedPrivateIPNets)

	ctx, cancel := context.WithTimeout(ctx, p.computeRequestTimeout(ctx, req.GetTimeout()))
	defer cancel()

	// Keep track of the last fetch error so that if we fail to fetch, we at
	// least have something we can return to the client.
	var lastFetchErr error
	var lastFetchUri string

	for i, uri := range req.GetUris() {
		_, err := url.Parse(uri)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("unparsable URI: %q", uri)
		}
		header := sharedHeader.Clone()
		if uriHeader, found := uriHeaders[i]; found {
			for k, v := range uriHeader {
				for _, vv := range v {
					// URI-specific headers take precedence over shared headers.
					header.Set(k, vv)
				}
			}
		}
		blobDigest, err := mirrorToCache(
			ctx,
			p.env.GetByteStreamClient(),
			req.GetInstanceName(),
			httpClient,
			uri,
			header,
			storageFunc,
			checksumFunc,
			expectedChecksum,
		)
		if err != nil {
			lastFetchErr = fmt.Errorf("%s: %w", uri, err)
			lastFetchUri = uri
			log.CtxWarningf(ctx, "Failed to mirror %q to cache: %s", uri, err)
			continue
		}
		return &rapb.FetchBlobResponse{
			Uri:            uri,
			Status:         &statuspb.Status{Code: int32(gcodes.OK)},
			BlobDigest:     blobDigest,
			DigestFunction: storageFunc,
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
		Uri: lastFetchUri,
		// Workaround for a bug in Bazel 8 and earlier: Bazel doesn't check
		// the status code and continues to look up the digest in the cache
		// even in the case of an error. The lookup for the empty Digest
		// message may succeed and return a cache hit for the empty file,
		// which is incorrect. To prevent this while remaining
		// spec-compliant, we return a valid Digest message that will never
		// be a cache hit. Spec-compliant clients should ignore it entirely.
		// https://github.com/bazelbuild/bazel/pull/25244
		BlobDigest: &repb.Digest{
			Hash:      strings.Repeat("1", 64),
			SizeBytes: 1,
		},
	}, nil
}

func (p *FetchServer) FetchDirectory(ctx context.Context, req *rapb.FetchDirectoryRequest) (*rapb.FetchDirectoryResponse, error) {
	return nil, status.UnimplementedError("FetchDirectory is not yet implemented")
}

func (p *FetchServer) rewriteToCache(ctx context.Context, blobDigest *repb.Digest, instanceName string, fromFunc, toFunc repb.DigestFunction_Value) *repb.Digest {
	cacheRN := digest.NewCASResourceName(blobDigest, instanceName, fromFunc)
	cache := p.env.GetCache()
	reader, err := cache.Reader(ctx, cacheRN.ToProto(), 0, 0)
	if err != nil {
		log.CtxErrorf(ctx, "Failed to get cache reader for %s: %s", digest.String(blobDigest), err)
		return nil
	}
	defer reader.Close()

	tmpFilePath, err := tempCopy(reader)
	if err != nil {
		log.CtxErrorf(ctx, "Failed to copy from reader to temp for %s: %s", digest.String(blobDigest), err)
		return nil
	}
	defer func() {
		if err := os.Remove(tmpFilePath); err != nil {
			log.Errorf("Failed to remove temp file: %s", err)
		}
	}()

	bsClient := p.env.GetByteStreamClient()
	storageDigest, err := cachetools.UploadFile(ctx, bsClient, instanceName, toFunc, tmpFilePath)
	if err != nil {
		log.CtxErrorf(ctx, "Failed to re-upload blob with new digestFunc %s for %s: %s", toFunc, digest.String(blobDigest), err)
		return nil
	}
	return storageDigest
}

func (p *FetchServer) findBlobInCache(ctx context.Context, instanceName string, checksumFunc repb.DigestFunction_Value, expectedChecksum string) *repb.Digest {
	blobDigest := &repb.Digest{
		Hash: expectedChecksum,
		// The digest size is unknown since the client only sends up
		// the hash. We can look up the size using the Metadata API,
		// which looks up only using the hash, so the size we pass here
		// doesn't matter.
		SizeBytes: 1,
	}
	cacheRN := digest.NewCASResourceName(blobDigest, instanceName, checksumFunc)
	log.CtxDebugf(ctx, "Looking up %s in cache", blobDigest.Hash)

	// Lookup metadata to get the correct digest size to be returned to
	// the client.
	cache := p.env.GetCache()
	md, err := cache.Metadata(ctx, cacheRN.ToProto())
	if err != nil {
		log.CtxInfof(ctx, "FetchServer failed to get metadata for %s: %s", expectedChecksum, err)
		return nil
	}
	blobDigest.SizeBytes = md.DigestSizeBytes

	// Even though we successfully fetched metadata, we need to renew
	// the cache entry (using Contains()) to ensure that it doesn't
	// expire by the time the client requests it from cache.
	cacheRN = digest.NewCASResourceName(blobDigest, instanceName, checksumFunc)
	exists, err := cache.Contains(ctx, cacheRN.ToProto())
	if err != nil {
		log.CtxErrorf(ctx, "Failed to renew %s: %s", digest.String(blobDigest), err)
		return nil
	}
	if !exists {
		log.CtxInfof(ctx, "Blob %s expired before we could renew it", digest.String(blobDigest))
		return nil
	}

	log.CtxDebugf(ctx, "FetchServer found %s in cache", digest.String(blobDigest))
	return blobDigest
}

// mirrorToCache uploads the contents at the given URI to the given cache,
// returning the digest. The fetched contents are checked against the given
// expectedChecksum (if non-empty), and if there is a mismatch then an error is
// returned.
func mirrorToCache(
	ctx context.Context,
	bsClient bspb.ByteStreamClient,
	remoteInstanceName string,
	httpClient *http.Client,
	uri string,
	header http.Header,
	storageFunc repb.DigestFunction_Value,
	checksumFunc repb.DigestFunction_Value,
	expectedChecksum string,
) (*repb.Digest, error) {
	log.CtxDebugf(ctx, "Fetching %s", uri)
	req, err := http.NewRequestWithContext(ctx, "GET", uri, nil)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to fetch %q: create request failed: %s", uri, err)
	}
	req.Header = header
	rsp, err := httpClient.Do(req)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to fetch %q: HTTP GET failed: %s", uri, err)
	}
	defer rsp.Body.Close()
	if rsp.StatusCode < 200 || rsp.StatusCode >= 400 {
		return nil, status.UnavailableErrorf("failed to fetch %q: HTTP %s", uri, err)
	}

	// If we know what the hash should be and the content length is known,
	// then we know the full digest, and can pipe directly from the HTTP
	// response to cache.
	if checksumFunc == storageFunc && expectedChecksum != "" && rsp.ContentLength >= 0 {
		d := &repb.Digest{Hash: expectedChecksum, SizeBytes: rsp.ContentLength}
		rn := digest.NewCASResourceName(d, remoteInstanceName, storageFunc)
		if _, _, err := cachetools.UploadFromReader(ctx, bsClient, rn, rsp.Body); err != nil {
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

	// If the requested digestFunc is supplied and differ from the checksum sri,
	// verify the downloaded file with the checksum sri before storing it to our cache.
	//
	// This will store the downloaded blob in our cache twice:
	//  - One entry using the checksum digest function for future cache hits.
	//  - One entry using the storage digest function for client to download.
	//
	// TODO(sluongng): We can track download information in a KV store with value
	// pointing to the CAS entry. That way, we would only need to store the download
	// blob once.
	if checksumFunc != storageFunc {
		checksumDigestRN, err := cachetools.ComputeFileDigest(tmpFilePath, remoteInstanceName, checksumFunc)
		if err != nil {
			return nil, status.UnavailableErrorf("failed to compute checksum digest: %s", err)
		}
		if expectedChecksum != "" && checksumDigestRN.GetDigest().GetHash() != expectedChecksum {
			return nil, status.InvalidArgumentErrorf("response body checksum for %q was %q but wanted %q", uri, checksumDigestRN.GetDigest().Hash, expectedChecksum)
		}
		if _, err := cachetools.UploadFile(ctx, bsClient, remoteInstanceName, checksumFunc, tmpFilePath); err != nil {
			// Best effort storing downloaded blob to our cache.
			// This is Ok to fail because subsequent requests will simply get no cache hits
			// and download blob again from upstream URL.
			log.CtxWarningf(ctx, "failed to cache object with checksumFunc: %s", err)
		}
	}
	blobDigest, err := cachetools.UploadFile(ctx, bsClient, remoteInstanceName, storageFunc, tmpFilePath)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to add object to cache: %s", err)
	}
	// If the requested digestFunc is supplied is the same with the checksum sri,
	// verify the expected checksum of the downloaded file after storing it in our cache.
	if checksumFunc == storageFunc && expectedChecksum != "" && blobDigest.Hash != expectedChecksum {
		return nil, status.InvalidArgumentErrorf("response body checksum for %q was %q but wanted %q", uri, blobDigest.Hash, expectedChecksum)
	}
	log.CtxDebugf(ctx, "Mirrored %s to cache (digest: %s)", uri, digest.String(blobDigest))
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
