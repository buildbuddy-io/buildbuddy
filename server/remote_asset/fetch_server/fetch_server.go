package fetch_server

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
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

	cachepb "github.com/buildbuddy-io/buildbuddy/proto/cache"
	cspb "github.com/buildbuddy-io/buildbuddy/proto/cache_service"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	remote_cache_config "github.com/buildbuddy-io/buildbuddy/server/remote_cache/config"
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
	if env.GetByteStreamClient() == nil {
		return status.FailedPreconditionError("missing ByteStreamClient")
	}
	if env.GetContentAddressableStorageClient() == nil {
		return status.FailedPreconditionError("missing ContentAddressableStorageClient")
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

type checksum struct {
	digestFunction repb.DigestFunction_Value
	hash           string
}

// parseChecksumQualifier returns the supported checksums in a checksum.sri
// qualifier. The Remote Asset qualifier lexicon allows whitespace-separated
// alternatives; validating any one of them satisfies the qualifier.
// https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/asset/v1/qualifiers.md
func parseChecksumQualifier(qualifier *rapb.Qualifier) ([]checksum, error) {
	entries := strings.Fields(qualifier.GetValue())
	if len(entries) == 0 {
		return nil, status.InvalidArgumentErrorf("Empty %q qualifier", qualifier.GetName())
	}
	var checksums []checksum
	for _, entry := range entries {
		algorithm, b64hash, ok := strings.Cut(entry, "-")
		if !ok || algorithm == "" || b64hash == "" {
			return nil, status.InvalidArgumentErrorf("Malformed checksum in %q qualifier", qualifier.GetName())
		}
		for _, digestFunc := range digest.SupportedDigestFunctions() {
			if algorithm != strings.ToLower(repb.DigestFunction_Value_name[int32(digestFunc)]) {
				continue
			}
			decodedHash, err := base64.StdEncoding.DecodeString(b64hash)
			if err != nil {
				return nil, status.InvalidArgumentErrorf("Error decoding qualifier %q: %s", qualifier.GetName(), err)
			}
			expectedChecksum := hex.EncodeToString(decodedHash)
			if err := digest.Validate(&repb.Digest{Hash: expectedChecksum, SizeBytes: 1}, digestFunc); err != nil {
				return nil, status.InvalidArgumentErrorf("Invalid %q qualifier: %s", qualifier.GetName(), err)
			}
			checksums = append(checksums, checksum{digestFunction: digestFunc, hash: expectedChecksum})
			break
		}
	}
	if len(checksums) == 0 {
		return nil, status.InvalidArgumentErrorf("No supported checksum algorithm in %q qualifier", qualifier.GetName())
	}
	return checksums, nil
}

type fetchOptions struct {
	storageDigestFunction repb.DigestFunction_Value
	checksums             []checksum
	sharedHeaders         http.Header
	uriHeaders            map[int]http.Header
}

// parseFetchOptions parses qualifiers only. URIs are checked when attempted, so
// a cache hit or successful mirror does not require validating unused URIs.
func parseFetchOptions(ctx context.Context, req *rapb.FetchBlobRequest) (*fetchOptions, error) {
	storageFunc := req.GetDigestFunction()
	if storageFunc == repb.DigestFunction_UNKNOWN {
		storageFunc = repb.DigestFunction_SHA256
	}
	opts := &fetchOptions{
		storageDigestFunction: storageFunc,
		sharedHeaders:         make(http.Header),
		uriHeaders:            make(map[int]http.Header),
	}
	var unsupportedQualifierNames []string
	var err error
	for _, qualifier := range req.GetQualifiers() {
		if qualifier.GetName() == ChecksumQualifier {
			opts.checksums, err = parseChecksumQualifier(qualifier)
			if err != nil {
				return nil, err
			}
			continue
		}
		if after, ok := strings.CutPrefix(qualifier.GetName(), BazelHttpHeaderPrefixQualifier); ok {
			opts.sharedHeaders.Add(
				after,
				qualifier.GetValue(),
			)
			continue
		}
		if after, ok := strings.CutPrefix(qualifier.GetName(), BazelHttpHeaderUrlPrefixQualifier); ok {
			idxAndKey := after
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
			if _, found := opts.uriHeaders[uriIndex]; !found {
				// If the URI index is not found, create a new header map.
				opts.uriHeaders[uriIndex] = make(http.Header)
			}
			opts.uriHeaders[uriIndex].Add(halves[1], qualifier.GetValue())
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
	return opts, nil
}

func (o *fetchOptions) headersForURI(index int) http.Header {
	header := o.sharedHeaders.Clone()
	for key, values := range o.uriHeaders[index] {
		for _, value := range values {
			// URI-specific headers override shared headers. Preserve the
			// existing last-value-wins behavior for repeated overrides.
			header.Set(key, value)
		}
	}
	return header
}

func (p *FetchServer) FetchBlob(ctx context.Context, req *rapb.FetchBlobRequest) (*rapb.FetchBlobResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, p.env.GetAuthenticator())
	if err != nil {
		return nil, err
	}

	opts, err := parseFetchOptions(ctx, req)
	if err != nil {
		return nil, err
	}
	for _, checksum := range opts.checksums {
		checksumFunc := checksum.digestFunction
		blobDigest := p.findBlobInCache(ctx, req.GetInstanceName(), checksumFunc, checksum.hash)
		// If the digestFunc is supplied and differ from the checksum sri,
		// after looking up the cached blob using checksum sri, re-upload
		// that blob using the requested digestFunc.
		if blobDigest != nil && checksumFunc != opts.storageDigestFunction {
			blobDigest = p.rewriteToCache(ctx, blobDigest, req.GetInstanceName(), checksumFunc, opts.storageDigestFunction)
		}

		if blobDigest != nil {
			return &rapb.FetchBlobResponse{
				Status:         &statuspb.Status{Code: int32(gcodes.OK)},
				BlobDigest:     blobDigest,
				DigestFunction: opts.storageDigestFunction,
			}, nil
		}
	}

	httpClient := httpclient.New(p.allowedPrivateIPNets, "fetch_server")
	// Don't send Referer headers on redirects. Go's http.Client adds these
	// automatically, but some sites (e.g. SourceForge) use the Referer to
	// detect non-browser clients and serve HTML instead of the file download.
	// Curl doesn't automatically set this after redirects either.
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return fmt.Errorf("stopped after 10 redirects")
		}
		req.Header.Del("Referer")
		return nil
	}
	bsClient := getByteStreamClient(p.env)

	ctx, cancel := context.WithTimeout(ctx, p.computeRequestTimeout(ctx, req.GetTimeout()))
	defer cancel()

	// Keep track of the last fetch error so that if we fail to fetch, we at
	// least have something we can return to the client.
	var lastFetchErr error
	var lastFetchUri string

	for i, uri := range req.GetUris() {
		_, err := url.Parse(uri)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("unparsable URI at index %d", i)
		}
		blobDigest, err := mirrorToCache(
			ctx,
			bsClient,
			req.GetInstanceName(),
			httpClient,
			uri,
			opts.headersForURI(i),
			opts.storageDigestFunction,
			opts.checksums,
		)
		if err != nil {
			lastFetchErr = fmt.Errorf("%s: %w", redactedURI(uri), err)
			lastFetchUri = uri
			log.CtxWarningf(ctx, "Failed to mirror %q to cache: %s", redactedURI(uri), err)
			continue
		}
		return &rapb.FetchBlobResponse{
			Uri:            uri,
			Status:         &statuspb.Status{Code: int32(gcodes.OK)},
			BlobDigest:     blobDigest,
			DigestFunction: opts.storageDigestFunction,
		}, nil
	}

	log.CtxInfof(ctx, "Fetch: returning NotFound after trying %d URIs", len(req.GetUris()))
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
	tmpFile, err := scratchspace.CreateTemp("remote-asset-fetch-*")
	if err != nil {
		log.CtxErrorf(ctx, "failed to create temp file: %s", err)
		return nil
	}
	defer func() {
		if err := tmpFile.Close(); err != nil {
			log.CtxErrorf(ctx, "Failed to close temp file: %s", err)
		}
		if err := os.Remove(tmpFile.Name()); err != nil {
			log.CtxErrorf(ctx, "Failed to remove temp file: %s", err)
		}
	}()

	cacheRN := digest.NewCASResourceName(blobDigest, instanceName, fromFunc)
	if remote_cache_config.ZstdTranscodingEnabled() {
		cacheRN.SetCompressor(repb.Compressor_ZSTD)
	}
	if err := cachetools.GetBlob(ctx, getByteStreamClient(p.env), cacheRN, tmpFile); err != nil {
		log.CtxErrorf(ctx, "Failed to read blob from cache for %s: %s", digest.String(blobDigest), err)
		return nil
	}

	storageDigest, err := cachetools.UploadFile(ctx, getByteStreamClient(p.env), instanceName, toFunc, tmpFile.Name())
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
	md, err := getCacheClient(p.env).GetMetadata(ctx, &cachepb.GetCacheMetadataRequest{
		ResourceName: cacheRN.ToProto(),
	})
	if err != nil {
		log.CtxInfof(ctx, "FetchServer failed to get metadata for %s: %s", expectedChecksum, err)
		return nil
	}

	blobDigest.SizeBytes = md.DigestSizeBytes

	// The metadata API doesn't update the last access time, so we need to use the FindMissing API to renew the entry
	// to ensure it doesn't expire by the time the client requests it from cache.
	rsp, err := getCASClient(p.env).FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		InstanceName:   instanceName,
		BlobDigests:    []*repb.Digest{blobDigest},
		DigestFunction: checksumFunc,
		Purpose:        repb.FindMissingBlobsRequest_REMOTE_ASSET_FETCH,
	})
	if err != nil {
		log.CtxErrorf(ctx, "Failed to renew %s: %s", digest.String(blobDigest), err)
		return nil
	}
	if len(rsp.MissingBlobDigests) > 0 {
		log.CtxInfof(ctx, "Blob %s expired before we could renew it", digest.String(blobDigest))
		return nil
	}

	log.CtxDebugf(ctx, "FetchServer found %s in cache", digest.String(blobDigest))
	return blobDigest
}

// redactedURI omits credentials and signed query parameters from diagnostics.
// Malformed URLs and opaque URLs must not be included verbatim either.
func redactedURI(uri string) string {
	u, err := url.Parse(uri)
	if err != nil {
		return "[invalid URI]"
	}
	return (&url.URL{Scheme: u.Scheme, Host: u.Host, Path: u.Path}).String()
}

// httpErrorReason keeps useful failure categories without printing raw HTTP
// errors, which may include credentials from request or redirect URLs.
func httpErrorReason(err error) string {
	if errors.Is(err, context.Canceled) {
		return "request canceled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline exceeded"
	}
	if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
		return "request timed out"
	}
	if _, ok := errors.AsType[*net.DNSError](err); ok {
		return "DNS lookup failed"
	}
	if _, ok := errors.AsType[*tls.CertificateVerificationError](err); ok {
		return "TLS certificate verification failed"
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "connection refused"
	}
	// These errors have no exported type or sentinel. Match only their fixed
	// messages; arbitrary transport errors can contain URL secrets.
	for cause := err; cause != nil; cause = errors.Unwrap(cause) {
		switch cause.Error() {
		case "IP address not allowed":
			return "IP address not allowed"
		case "stopped after 10 redirects":
			return "stopped after 10 redirects"
		}
	}
	return "request failed"
}

// mirrorToCache uploads the contents at the given URI to the given cache,
// returning the digest. The fetched contents are checked against the given
// checksums (if any), and an error is returned if none match.
func mirrorToCache(
	ctx context.Context,
	bsClient bspb.ByteStreamClient,
	remoteInstanceName string,
	httpClient *http.Client,
	uri string,
	header http.Header,
	storageFunc repb.DigestFunction_Value,
	checksums []checksum,
) (*repb.Digest, error) {
	safeURI := redactedURI(uri)
	log.CtxDebugf(ctx, "Fetching %s", safeURI)
	req, err := http.NewRequestWithContext(ctx, "GET", uri, nil)
	if err != nil {
		return nil, status.UnavailableErrorf("failed to fetch %q: create request failed", safeURI)
	}
	req.Header = header
	rsp, err := httpClient.Do(req)
	if err != nil {
		// HTTP errors can contain credentials in the request URL or in a
		// malformed redirect target, including inside nested url.Errors.
		return nil, status.UnavailableErrorf("failed to fetch %q: HTTP GET failed: %s", safeURI, httpErrorReason(err))
	}
	defer rsp.Body.Close()
	if rsp.StatusCode < 200 || rsp.StatusCode >= 400 {
		return nil, status.UnavailableErrorf("failed to fetch %q: HTTP %d %s", safeURI, rsp.StatusCode, http.StatusText(rsp.StatusCode))
	}

	var checksumFunc repb.DigestFunction_Value
	var expectedChecksum string
	if len(checksums) == 1 {
		checksumFunc = checksums[0].digestFunction
		expectedChecksum = checksums[0].hash
	}

	// If we know what the hash should be and the content length is known,
	// then we know the full digest, and can pipe directly from the HTTP
	// response to cache.
	if checksumFunc == storageFunc && expectedChecksum != "" && rsp.ContentLength >= 0 {
		d := &repb.Digest{Hash: expectedChecksum, SizeBytes: rsp.ContentLength}
		rn := digest.NewCASResourceName(d, remoteInstanceName, storageFunc)
		if remote_cache_config.ZstdTranscodingEnabled() {
			rn.SetCompressor(repb.Compressor_ZSTD)
		}
		if _, _, err := cachetools.UploadFromReader(ctx, bsClient, rn, rsp.Body); err != nil {
			return nil, status.UnavailableErrorf("failed to upload %s to cache: %s", digest.String(d), err)
		}
		log.CtxInfof(ctx, "Mirrored %s to cache (digest: %s)", safeURI, digest.String(d))
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

	// Multiple alternatives cannot use the direct-upload path: a mismatch
	// against one checksum must not prevent another checksum from matching.
	// Fetch once, and compute each digest function at most once while checking
	// the alternatives against the downloaded file.
	if len(checksums) > 1 {
		hashes := make(map[repb.DigestFunction_Value]string)
		for _, checksum := range checksums {
			hash, ok := hashes[checksum.digestFunction]
			if !ok {
				rn, err := cachetools.ComputeFileDigest(tmpFilePath, remoteInstanceName, checksum.digestFunction)
				if err != nil {
					return nil, status.UnavailableErrorf("failed to compute checksum digest: %s", err)
				}
				hash = rn.GetDigest().GetHash()
				hashes[checksum.digestFunction] = hash
			}
			if hash == checksum.hash {
				checksumFunc = checksum.digestFunction
				expectedChecksum = checksum.hash
				break
			}
		}
		if expectedChecksum == "" {
			return nil, status.InvalidArgumentErrorf("response body checksum for %q did not match any supported checksum", safeURI)
		}
	}

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
			return nil, status.InvalidArgumentErrorf("response body checksum for %q was %q but wanted %q", safeURI, checksumDigestRN.GetDigest().Hash, expectedChecksum)
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
		return nil, status.InvalidArgumentErrorf("response body checksum for %q was %q but wanted %q", safeURI, blobDigest.Hash, expectedChecksum)
	}
	log.CtxDebugf(ctx, "Mirrored %s to cache (digest: %s)", safeURI, digest.String(blobDigest))
	return blobDigest, nil
}

func tempCopy(r io.Reader) (path string, err error) {
	f, err := scratchspace.CreateTemp("remote-asset-fetch-*")
	if err != nil {
		return "", status.UnavailableErrorf("failed to create temp file for download: %s", err)
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil && err == nil {
			path = ""
			err = status.UnavailableErrorf("failed to close temp file for download: %s", closeErr)
		}
		if err != nil {
			if removeErr := os.Remove(f.Name()); removeErr != nil {
				log.Errorf("Failed to remove temp file: %s", removeErr)
			}
		}
	}()
	if _, err := io.Copy(f, r); err != nil {
		return "", status.UnavailableErrorf("failed to copy HTTP response to temp file: %s", err)
	}
	return f.Name(), nil
}

// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/6187): Reduce gRPC overhead from self-RPCs.
func getByteStreamClient(env environment.Env) bspb.ByteStreamClient {
	bsClient := env.GetByteStreamClient()
	// If there is a local bytestream server, use it instead of the remote one.
	if env.GetLocalByteStreamClient() != nil {
		bsClient = env.GetLocalByteStreamClient()
	}
	return bsClient
}
func getCASClient(env environment.Env) repb.ContentAddressableStorageClient {
	casClient := env.GetContentAddressableStorageClient()
	// If there is a local content addressable storage server, use it instead of the remote one.
	if env.GetLocalContentAddressableStorageClient() != nil {
		casClient = env.GetLocalContentAddressableStorageClient()
	}
	return casClient
}
func getCacheClient(env environment.Env) cspb.CacheClient {
	cacheClient := env.GetCacheClient()
	// If there is a local cache server, use it instead of the remote one.
	if env.GetLocalCacheClient() != nil {
		cacheClient = env.GetLocalCacheClient()
	}
	return cacheClient
}
