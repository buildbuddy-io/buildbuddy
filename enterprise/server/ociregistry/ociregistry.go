package ociregistry

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/ioutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/protobuf/types/known/anypb"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	headerAccept              = "Accept"
	headerContentType         = "Content-Type"
	headerDockerContentDigest = "Docker-Content-Digest"
	headerContentLength       = "Content-Length"
	headerAuthorization       = "Authorization"
	headerWWWAuthenticate     = "WWW-Authenticate"
	headerRange               = "Range"

	blobOutputFilePath          = "_bb_ociregistry_blob_"
	blobMetadataOutputFilePath  = "_bb_ociregistry_blob_metadata_"
	actionResultInstanceName    = interfaces.OCIImageInstanceNamePrefix
	manifestContentInstanceName = interfaces.OCIImageInstanceNamePrefix + "_manifest_content_"

	maxManifestSize = 10000000

	hitLabel    = "hit"
	missLabel   = "miss"
	uploadLabel = "upload"

	actionCacheLabel = "action_cache"
	casLabel         = "cas"

	cacheDigestFunction = repb.DigestFunction_SHA256
)

var (
	blobsOrManifestsReqRegexp = regexp.MustCompile("/v2/(.+?)/(blobs|manifests)/(.+)")
	enableRegistry            = flag.Bool("ociregistry.enabled", false, "Whether to enable registry services")
)

type registry struct {
	env    environment.Env
	client *http.Client
}

func Register(env *real_environment.RealEnv) error {
	if !*enableRegistry {
		return nil
	}

	r, err := New(env)
	if err != nil {
		return err
	}

	env.SetOCIRegistry(r)
	return nil
}

func New(env environment.Env) (*registry, error) {
	client := httpclient.New()
	r := &registry{
		env:    env,
		client: client,
	}
	return r, nil
}

func (r *registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.handleRegistryRequest(w, req)
}

// The OCI registry is intended to be a read-through cache for public OCI images
// (to cut down on the number of API calls to Docker Hub and on bandwidth).
// handleRegistryRequest implements just enough of the [OCI Distribution Spec](https://github.com/opencontainers/distribution-spec/blob/main/spec.md)
// to allow clients to pull OCI images from remote registries that do not require authentication.
// This registry does not support resumable pulls via the Range header.
func (r *registry) handleRegistryRequest(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.env.GetAuthenticator())
	if err != nil {
		http.Error(w, fmt.Sprintf("could not attach user prefix: %s", err), http.StatusInternalServerError)
		return
	}

	// Only GET and HEAD requests for blobs and manifests are supported.
	if req.Method != http.MethodGet && req.Method != http.MethodHead {
		http.Error(w, fmt.Sprintf("unsupported HTTP method %s", req.Method), http.StatusNotFound)
		return
	}

	// Clients issue a GET or HEAD /v2/ request to verify that this  is a registry endpoint.
	//
	// When pulling an image from either a public or private repository, the `docker` client
	// will first issue a GET request to /v2/.
	// If authentication to Docker Hub (index.docker.io) is necessary, the client expects to
	// receive HTTP 401 and a `WWW-Authenticate` header. The client will then authenticate
	// and pass Authorization headers with subsequent requests.
	if req.RequestURI == "/v2/" {
		r.handleV2Request(ctx, w, req)
		return
	}

	if m := blobsOrManifestsReqRegexp.FindStringSubmatch(req.RequestURI); len(m) == 4 {
		// The image repository name, which can include a registry host and optional port.
		// For example, "alpine" is a repository name. By default, the registry portion is index.docker.io.
		// "mycustomregistry.com:8080/alpine" is also a repository name. The registry portion is mycustomregistry.com:8080.
		repository := m[1]

		blobsOrManifests := m[2]
		var ociResourceType ocipb.OCIResourceType
		switch blobsOrManifests {
		case "blobs":
			ociResourceType = ocipb.OCIResourceType_BLOB
		case "manifests":
			ociResourceType = ocipb.OCIResourceType_MANIFEST
		default:
			http.Error(w, fmt.Sprintf("Expected request for /v2/<repository>/blobs/... or /v2/<repository>/manifests/..., instead received %q", req.URL.Path), http.StatusBadRequest)
			return
		}

		// For manifests, the identifier can be a tag (such as "latest") or a digest
		// (such as "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").
		// The OCI image distribution spec refers to this string as <identifier>.
		// However, go-containerregistry has a separate Reference type and refers to this string as `identifier`.
		identifier := m[3]

		r.handleBlobsOrManifestsRequest(ctx, w, req, ociResourceType, repository, identifier)
		return
	}

	http.NotFound(w, req)
}

func (r *registry) handleV2Request(ctx context.Context, w http.ResponseWriter, inreq *http.Request) {
	scheme := "https"
	if inreq.URL.Scheme != "" {
		scheme = inreq.URL.Scheme
	}
	u := &url.URL{
		Scheme: scheme,
		Host:   gcrname.DefaultRegistry,
		Path:   "/v2/",
	}
	upreq, err := http.NewRequest(inreq.Method, u.String(), nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Could not make %s request to upstream registry %q: %s", inreq.Method, u.Hostname(), err), http.StatusServiceUnavailable)
		return
	}
	upresp, err := r.client.Do(upreq.WithContext(ctx))
	if err != nil {
		http.Error(w, fmt.Sprintf("Transport error making %s request to upstream registry %q: %s", inreq.Method, u.Hostname(), err), http.StatusServiceUnavailable)
		return
	}
	defer upresp.Body.Close()
	if upresp.Header.Get(headerWWWAuthenticate) != "" {
		w.Header().Add(headerWWWAuthenticate, upresp.Header.Get(headerWWWAuthenticate))
	}
	w.WriteHeader(upresp.StatusCode)
	_, err = io.Copy(w, upresp.Body)
	if err != nil && err != context.Canceled {
		log.CtxWarningf(ctx, "Error reading response from %s: %s", u.Hostname(), err)
	}
}

func (r *registry) makeUpstreamRequest(ctx context.Context, method, acceptHeader, authorizationHeader string, ociResourceType ocipb.OCIResourceType, ref gcrname.Reference) (*http.Response, error) {
	var path string
	switch ociResourceType {
	case ocipb.OCIResourceType_BLOB:
		path = "/v2/" + ref.Context().RepositoryStr() + "/blobs/" + ref.Identifier()
	case ocipb.OCIResourceType_MANIFEST:
		path = "/v2/" + ref.Context().RepositoryStr() + "/manifests/" + ref.Identifier()
	case ocipb.OCIResourceType_UNKNOWN:
		return nil, status.FailedPreconditionError("unknown OCI resource type, expected blobs or manifests")
	}
	u := &url.URL{
		Scheme: ref.Context().Scheme(),
		Host:   ref.Context().RegistryStr(),
		Path:   path,
	}

	upreq, err := http.NewRequest(method, u.String(), nil)
	if err != nil {
		return nil, status.UnavailableErrorf("could not make %s request to upstream registry %q: %s", method, upreq.URL.Hostname(), err)
	}
	if acceptHeader != "" {
		upreq.Header.Set(headerAccept, acceptHeader)
	}
	if authorizationHeader != "" {
		upreq.Header.Set(headerAuthorization, authorizationHeader)
	}
	return r.client.Do(upreq.WithContext(ctx))
}

func parseDockerContentDigestHeader(value string) (*gcr.Hash, error) {
	if value == "" {
		return nil, nil
	}
	hash, err := gcr.NewHash(value)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("could not parse %s header: %s", headerDockerContentDigest, err)
	}
	return &hash, nil
}

func (r *registry) resolveManifestDigest(ctx context.Context, acceptHeader, authorizationHeader string, ociResourceType ocipb.OCIResourceType, ref gcrname.Reference) (*gcr.Hash, bool, error) {
	headresp, err := r.makeUpstreamRequest(ctx, http.MethodHead, acceptHeader, authorizationHeader, ociResourceType, ref)
	if err != nil {
		return nil, false, status.UnavailableErrorf("error making %s request to upstream registry: %s", http.MethodHead, err)
	}
	defer headresp.Body.Close()

	if headresp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if headresp.StatusCode != http.StatusOK {
		return nil, false, status.UnavailableErrorf("could not fetch manifest for %s, upstream HTTP status %d", ref.Context(), headresp.StatusCode)
	}
	value := headresp.Header.Get(headerDockerContentDigest)
	hash, err := parseDockerContentDigestHeader(value)
	if err != nil {
		return nil, true, status.InvalidArgumentErrorf("error parsing %s header: %s", headerDockerContentDigest, err)
	}
	return hash, true, nil
}

func (r *registry) handleBlobsOrManifestsRequest(ctx context.Context, w http.ResponseWriter, inreq *http.Request, ociResourceType ocipb.OCIResourceType, repository, identifier string) {
	if inreq.Header.Get(headerRange) != "" {
		http.Error(w, "Range headers not supported", http.StatusNotImplemented)
		return
	}

	identifierIsDigest := isDigest(identifier)
	if ociResourceType == ocipb.OCIResourceType_BLOB && !identifierIsDigest {
		http.Error(w, fmt.Sprintf("Can only retrieve blobs by digest, received '%q'", identifier), http.StatusNotFound)
		return
	}

	ref, err := parseReference(repository, identifier)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error parsing image repository %q and identifier %q: %s", repository, identifier, err), http.StatusBadRequest)
		return
	}

	resolvedRef := ref
	resolvedRefIsDigest := identifierIsDigest
	if ociResourceType == ocipb.OCIResourceType_MANIFEST && !identifierIsDigest && inreq.Method == http.MethodGet {
		// Fetching a manifest by tag.
		// Since the mapping from tag to digest can change, we only look up manifests and blobs in the CAS by digest.
		// Docker Hub does not count "version checks" (HEAD requests) against the rate limit.
		// So make a HEAD request for the manifest, find its digest, and see if we can fetch from CAS.
		// TODO(dan): Docker Hub responds with Docker-Content-Digest headers. Other registries may not. Make code resilient to header absence.
		hash, exists, err := r.resolveManifestDigest(ctx, inreq.Header.Get(headerAccept), inreq.Header.Get(headerAuthorization), ociResourceType, ref)
		if err != nil {
			http.Error(w, fmt.Sprintf("Could not resolve digest for manifest %q: %s", ref.Context(), err), http.StatusServiceUnavailable)
			return
		}
		if !exists {
			http.Error(w, fmt.Sprintf("Could not find manifest %q", ref.Context()), http.StatusNotFound)
			return
		}
		if hash == nil {
			http.Error(w, fmt.Sprintf("Could not parse digest from manifest for %q", ref.Context()), http.StatusNotFound)
			return
		}
		manifestRef, err := parseReference(repository, hash.String())
		if err != nil {
			log.CtxErrorf(ctx, "Could not parse manifest reference for %q: %s", repository, err)
		} else {
			resolvedRef = manifestRef
			resolvedRefIsDigest = true
		}
	}

	bsClient := r.env.GetByteStreamClient()
	acClient := r.env.GetActionCacheClient()

	// To fetch a blob or manifest from the AC and CAS, you need a digest.
	// Blob requests MUST be by digest.
	// Manifest requests can be by digest or by tag.
	// If we successfully resolved a manifest tag to a digest above, it's safe
	// to look up the manifest in the cache.
	if resolvedRefIsDigest {
		writeBody := inreq.Method == http.MethodGet
		err := fetchBlobOrManifestFromCache(ctx, w, bsClient, acClient, resolvedRef, ociResourceType, writeBody)
		if err == nil {
			return // Successfully served request from cache.
		}
		if !status.IsNotFoundError(err) {
			log.CtxErrorf(ctx, "error fetching image %q from the cache: %s", resolvedRef.Context(), err)
			http.Error(w, fmt.Sprintf("Error fetching image %q from the cache: %s", resolvedRef.Context(), err), http.StatusServiceUnavailable)
			return
		}
	}

	upresp, err := r.makeUpstreamRequest(ctx, inreq.Method, inreq.Header.Get(headerAccept), inreq.Header.Get(headerAuthorization), ociResourceType, resolvedRef)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error making %s request to upstream registry: %s", inreq.Method, err), http.StatusServiceUnavailable)
		return
	}
	defer upresp.Body.Close()

	for _, header := range []string{headerContentLength, headerContentType, headerDockerContentDigest, headerWWWAuthenticate} {
		if upresp.Header.Get(header) != "" {
			w.Header().Add(header, upresp.Header.Get(header))
		}
	}
	w.WriteHeader(upresp.StatusCode)
	if inreq.Method == http.MethodHead {
		return
	}

	hasLength := upresp.Header.Get(headerContentLength) != ""
	contentLength, err := strconv.ParseInt(upresp.Header.Get(headerContentLength), 10, 64)
	if err != nil {
		hasLength = false
	}

	hasContentType := upresp.Header.Get(headerContentType) != ""
	contentType := upresp.Header.Get(headerContentType)

	// In order to cache a blob or manifest, you need a digest,
	// metadata (content length and content type), and the contents of the blob or manifest
	// (which will be in the body of an upstream HTTP GET).
	cacheable := upresp.StatusCode == http.StatusOK && hasLength && resolvedRefIsDigest && hasContentType
	if !cacheable {
		_, err = io.Copy(w, upresp.Body)
		if err != nil && err != context.Canceled {
			log.CtxWarningf(ctx, "Error writing response body for %q: %s", resolvedRef.Context(), err)
		}
		return
	}

	hash, err := gcr.NewHash(resolvedRef.Identifier())
	if err != nil {
		_, err = io.Copy(w, upresp.Body)
		if err != nil && err != context.Canceled {
			log.CtxWarningf(ctx, "Error writing response body for %q: %s", resolvedRef.Context(), err)
		}
		return
	}

	err = writeBlobOrManifestToCacheAndResponse(ctx, upresp.Body, w, bsClient, acClient, resolvedRef, ociResourceType, hash, contentType, contentLength)
	if err != nil && err != context.Canceled {
		log.CtxWarningf(ctx, "Error writing response body to cache for %q: %s", resolvedRef.Context(), err)
	}
}

func updateCacheEventMetric(cacheType, eventType string) {
	metrics.OCIRegistryCacheEvents.With(prometheus.Labels{
		metrics.CacheTypeLabel:      cacheType,
		metrics.CacheEventTypeLabel: eventType,
	}).Inc()
}

func fetchBlobMetadataFromCache(ctx context.Context, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference) (*ocipb.OCIBlobMetadata, error) {
	hash, err := gcr.NewHash(ref.Identifier())
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_BLOB,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		cacheDigestFunction,
	)
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	updateCacheEventMetric(actionCacheLabel, hitLabel)

	var blobMetadataCASDigest *repb.Digest
	var blobCASDigest *repb.Digest
	for _, outputFile := range ar.GetOutputFiles() {
		switch outputFile.GetPath() {
		case blobMetadataOutputFilePath:
			blobMetadataCASDigest = outputFile.GetDigest()
		case blobOutputFilePath:
			blobCASDigest = outputFile.GetDigest()
		default:
			log.CtxErrorf(ctx, "Unknown output file path %q in ActionResult for %q", outputFile.GetPath(), ref.Context())
		}
	}
	if blobMetadataCASDigest == nil || blobCASDigest == nil {
		updateCacheEventMetric(casLabel, missLabel)
		return nil, status.NotFoundErrorf("missing blob metadata digest or blob digest for %s", ref.Context())
	}
	blobMetadataRN := digest.NewCASResourceName(
		blobMetadataCASDigest,
		"",
		cacheDigestFunction,
	)
	blobMetadata := &ocipb.OCIBlobMetadata{}
	err = cachetools.GetBlobAsProto(ctx, bsClient, blobMetadataRN, blobMetadata)
	if err != nil {
		updateCacheEventMetric(casLabel, missLabel)
		return nil, err
	}
	updateCacheEventMetric(casLabel, hitLabel)
	return blobMetadata, nil
}

func fetchBlobFromCache(ctx context.Context, w io.Writer, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, hash gcr.Hash, contentLength int64) error {
	blobCASDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		cacheDigestFunction,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	counter := &ioutil.Counter{}
	mw := io.MultiWriter(w, counter)
	defer func() {
		metrics.OCIRegistryCacheDownloadSizeBytes.With(prometheus.Labels{
			metrics.CacheTypeLabel: casLabel,
		}).Observe(float64(counter.Count()))
	}()
	if err := cachetools.GetBlob(ctx, bsClient, blobRN, mw); err != nil {
		updateCacheEventMetric(casLabel, missLabel)
		return err
	}
	updateCacheEventMetric(casLabel, hitLabel)
	return nil
}

func fetchBlobOrManifestFromCache(ctx context.Context, w http.ResponseWriter, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, ociResourceType ocipb.OCIResourceType, writeBody bool) error {
	hash, err := gcr.NewHash(ref.Identifier())
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return err
	}
	if ociResourceType == ocipb.OCIResourceType_MANIFEST {
		mc, err := fetchManifestFromAC(ctx, acClient, ref, hash)
		if err != nil {
			return err
		}
		w.Header().Add(headerDockerContentDigest, hash.String())
		w.Header().Add(headerContentLength, strconv.Itoa(len(mc.GetRaw())))
		w.Header().Add(headerContentType, mc.GetContentType())
		w.WriteHeader(http.StatusOK)
		if !writeBody {
			return nil
		}
		counter := &ioutil.Counter{}
		mw := io.MultiWriter(w, counter)
		defer func() {
			metrics.OCIRegistryCacheDownloadSizeBytes.With(prometheus.Labels{
				metrics.CacheTypeLabel: actionCacheLabel,
			}).Observe(float64(counter.Count()))
		}()
		if _, err := io.Copy(mw, bytes.NewReader(mc.GetRaw())); err != nil {
			return err
		}
		return nil
	}

	blobMetadata, err := fetchBlobMetadataFromCache(ctx, bsClient, acClient, ref)
	if err != nil {
		return err
	}
	w.Header().Add(headerDockerContentDigest, hash.String())
	w.Header().Add(headerContentLength, strconv.FormatInt(blobMetadata.GetContentLength(), 10))
	w.Header().Add(headerContentType, blobMetadata.GetContentType())
	w.WriteHeader(http.StatusOK)

	if !writeBody {
		return nil
	}
	return fetchBlobFromCache(ctx, w, bsClient, acClient, hash, blobMetadata.GetContentLength())
}

func writeBlobOrManifestToCacheAndResponse(ctx context.Context, upstream io.Reader, w io.Writer, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, ociResourceType ocipb.OCIResourceType, hash gcr.Hash, contentType string, contentLength int64) error {
	r := upstream
	if ociResourceType == ocipb.OCIResourceType_MANIFEST {
		if contentLength > maxManifestSize {
			return status.FailedPreconditionErrorf("manifest too large (%d bytes) to write to cache (limit %d bytes)", contentLength, maxManifestSize)
		}
		raw, err := io.ReadAll(io.LimitReader(upstream, contentLength))
		if err != nil {
			return err
		}
		if err := writeManifestToAC(ctx, raw, acClient, ref, hash, contentType); err != nil {
			log.CtxWarningf(ctx, "Error writing manifest to AC for %q: %s", ref.Context(), err)
		}
		r = bytes.NewReader(raw)
	}

	blobCASDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		cacheDigestFunction,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	tr := io.TeeReader(r, w)
	updateCacheEventMetric(casLabel, uploadLabel)
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, blobRN, tr)
	if err != nil {
		return err
	}

	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	updateCacheEventMetric(casLabel, uploadLabel)
	blobMetadataCASDigest, err := cachetools.UploadProto(ctx, bsClient, "", cacheDigestFunction, blobMetadata)
	if err != nil {
		return err
	}

	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ociResourceType,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	ar := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{
				Path:   blobOutputFilePath,
				Digest: blobCASDigest,
			},
			{
				Path:   blobMetadataOutputFilePath,
				Digest: blobMetadataCASDigest,
			},
		},
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		return err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		cacheDigestFunction,
	)
	updateCacheEventMetric(actionCacheLabel, uploadLabel)
	err = cachetools.UploadActionResult(ctx, acClient, arRN, ar)
	if err != nil {
		return err
	}
	return nil
}

func writeManifestToAC(ctx context.Context, raw []byte, acClient repb.ActionCacheClient, ref gcrname.Reference, hash gcr.Hash, contentType string) error {
	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_MANIFEST,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		return err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		manifestContentInstanceName,
		cacheDigestFunction,
	)

	m := &ocipb.OCIManifestContent{
		Raw:         raw,
		ContentType: contentType,
	}
	any, err := anypb.New(m)
	if err != nil {
		return err
	}
	ar := &repb.ActionResult{
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{
				any,
			},
		},
	}
	updateCacheEventMetric(actionCacheLabel, uploadLabel)
	return cachetools.UploadActionResult(ctx, acClient, arRN, ar)
}

func fetchManifestFromAC(ctx context.Context, acClient repb.ActionCacheClient, ref gcrname.Reference, hash gcr.Hash) (*ocipb.OCIManifestContent, error) {
	arKey := &ocipb.OCIActionResultKey{
		Registry:      ref.Context().RegistryStr(),
		Repository:    ref.Context().RepositoryStr(),
		ResourceType:  ocipb.OCIResourceType_MANIFEST,
		HashAlgorithm: hash.Algorithm,
		HashHex:       hash.Hex,
	}
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), cacheDigestFunction)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		manifestContentInstanceName,
		cacheDigestFunction,
	)

	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, err
	}
	meta := ar.GetExecutionMetadata()
	if meta == nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		log.CtxWarningf(ctx, "Missing execution metadata for manifest in %q", ref.Context())
		return nil, status.InternalErrorf("missing execution metadata for manifest in %q", ref.Context())
	}
	aux := meta.GetAuxiliaryMetadata()
	if aux == nil || len(aux) != 1 {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		log.CtxWarningf(ctx, "Missing auxiliary metadata for manifest in %q", ref.Context())
		return nil, status.InternalErrorf("missing auxiliary metadata for manifest in %q", ref.Context())
	}
	any := aux[0]
	var mc ocipb.OCIManifestContent
	err = any.UnmarshalTo(&mc)
	if err != nil {
		updateCacheEventMetric(actionCacheLabel, missLabel)
		return nil, status.InternalErrorf("could not unmarshal metadata for manifest in %q: %s", ref.Context(), err)
	}
	updateCacheEventMetric(actionCacheLabel, hitLabel)
	return &mc, nil
}

func isDigest(identifier string) bool {
	_, err := gcr.NewHash(identifier)
	return err == nil
}

func parseReference(repository, identifier string) (gcrname.Reference, error) {
	joiner := ":"
	if isDigest(identifier) {
		joiner = "@"
	}
	ref, err := gcrname.ParseReference(repository + joiner + identifier)
	if err != nil {
		return nil, err
	}
	return ref, nil
}
