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
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

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

	blobOutputFilePath         = "_bb_ociregistry_blob_"
	blobMetadataOutputFilePath = "_bb_ociregistry_blob_metadata_"
	actionResultInstanceName   = interfaces.OCIImageInstanceNamePrefix
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
	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.env)
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
			http.Error(w, fmt.Sprintf("expect requests for /v2/<repository>/blobs/... or /v2/<repository>/manifests/..., instead received %s", req.URL.Path), http.StatusNotFound)
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
		http.Error(w, fmt.Sprintf("could not make %s request to upstream registry '%s': %s", inreq.Method, u, err), http.StatusNotFound)
		return
	}
	upresp, err := r.client.Do(upreq.WithContext(ctx))
	if err != nil {
		http.Error(w, fmt.Sprintf("transport error making %s request to upstream registry '%s': %s", inreq.Method, u, err), http.StatusNotFound)
		return
	}
	defer upresp.Body.Close()
	if upresp.Header.Get(headerWWWAuthenticate) != "" {
		w.Header().Add(headerWWWAuthenticate, upresp.Header.Get(headerWWWAuthenticate))
	}
	w.WriteHeader(upresp.StatusCode)
	_, err = io.Copy(w, upresp.Body)
	if err != nil {
		if err != context.Canceled {
			log.CtxWarningf(ctx, "error writing response body for '%s', upstream '%s': %s", inreq.URL, u, err)
		}
		return
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
		return nil, fmt.Errorf("unknown OCI resource type, expected blobs or manifests")
	}
	u := &url.URL{
		Scheme: ref.Context().Scheme(),
		Host:   ref.Context().RegistryStr(),
		Path:   path,
	}

	upreq, err := http.NewRequest(method, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("could not make %s request to upstream registry '%s': %s", method, u, err)
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
		return nil, fmt.Errorf("could not parse %s header (value '%s'): %s", headerDockerContentDigest, value, err)
	}
	return &hash, nil
}

func (r *registry) resolveManifestDigest(ctx context.Context, acceptHeader, authorizationHeader string, ociResourceType ocipb.OCIResourceType, ref gcrname.Reference) (*gcr.Hash, bool, error) {
	headresp, err := r.makeUpstreamRequest(ctx, http.MethodHead, acceptHeader, authorizationHeader, ociResourceType, ref)
	if err != nil {
		return nil, false, fmt.Errorf("error making %s request to upstream registry: %s", http.MethodHead, err)
	}
	defer headresp.Body.Close()

	if headresp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if headresp.StatusCode != http.StatusOK {
		return nil, false, fmt.Errorf("could not fetch manifest for %s", ref)
	}
	value := headresp.Header.Get(headerDockerContentDigest)
	hash, err := parseDockerContentDigestHeader(value)
	if err != nil {
		return nil, true, fmt.Errorf("error parsing %s header (value %s): %s", headerDockerContentDigest, value, err)
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
		http.Error(w, fmt.Sprintf("can only retrieve blobs by digest, received '%s'", identifier), http.StatusNotFound)
		return
	}

	ref, err := parseReference(repository, identifier)
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing image repository '%s' and identifier '%s': %s", repository, identifier, err), http.StatusNotFound)
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
			http.Error(w, fmt.Sprintf("could not resolve digest for manifest %s: %s", ref, err), http.StatusServiceUnavailable)
			return
		}
		if !exists {
			http.Error(w, fmt.Sprintf("could not find manifest %s", ref), http.StatusNotFound)
			return
		}
		if hash == nil {
			http.Error(w, fmt.Sprintf("could not parse digest from manifest for %s", ref), http.StatusNotFound)
			return
		}
		manifestRef, err := parseReference(repository, hash.String())
		if err != nil {
			log.CtxErrorf(ctx, "could not parse reference for manifest (repository %s, resolved hash %s): %s", repository, hash, err)
		} else {
			resolvedRef = manifestRef
			resolvedRefIsDigest = true
		}
	}

	bsClient := r.env.GetByteStreamClient()
	acClient := r.env.GetActionCacheClient()
	if resolvedRefIsDigest {
		writeBody := inreq.Method == http.MethodGet
		err := fetchBlobOrManifestFromCache(ctx, w, bsClient, acClient, resolvedRef, ociResourceType, writeBody)
		if err == nil {
			return
		} else if !status.IsNotFoundError(err) {
			message := fmt.Sprintf("error fetching image %s from the CAS: %s", ref, err)
			log.CtxError(ctx, message)
			http.Error(w, message, http.StatusNotFound)
			return
		}
	}

	upresp, err := r.makeUpstreamRequest(ctx, inreq.Method, inreq.Header.Get(headerAccept), inreq.Header.Get(headerAuthorization), ociResourceType, resolvedRef)
	if err != nil {
		http.Error(w, fmt.Sprintf("error making %s request to upstream registry: %s", inreq.Method, err), http.StatusServiceUnavailable)
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

	cacheable := upresp.StatusCode == http.StatusOK && hasLength && resolvedRefIsDigest && hasContentType
	if !cacheable {
		_, err = io.Copy(w, upresp.Body)
		if err != nil && err != context.Canceled {
			log.CtxWarningf(ctx, "error writing response body for '%s': %s", inreq.URL, err)
		}
		return
	}

	hash, err := gcr.NewHash(resolvedRef.Identifier())
	if err != nil {
		_, err = io.Copy(w, upresp.Body)
		if err != nil && err != context.Canceled {
			log.CtxWarningf(ctx, "error writing response body for '%s': %s", inreq.URL, err)
		}
		return
	}

	err = writeBlobOrManifestToCacheAndResponse(ctx, upresp.Body, w, bsClient, acClient, resolvedRef, ociResourceType, hash, contentType, contentLength)
	if err != nil && err != context.Canceled {
		log.CtxWarningf(ctx, "error writing response body to cache for '%s': %s", inreq.URL, err)
	}
}

func fetchBlobOrManifestFromCache(ctx context.Context, w http.ResponseWriter, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, ociResourceType ocipb.OCIResourceType, writeBody bool) error {
	hash, err := gcr.NewHash(ref.Identifier())
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
	arKeyBytes, err := proto.Marshal(arKey)
	if err != nil {
		return err
	}
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), repb.DigestFunction_SHA256)
	if err != nil {
		return err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		repb.DigestFunction_SHA256,
	)
	ar, err := cachetools.GetActionResult(ctx, acClient, arRN)
	if err != nil {
		return err
	}

	var blobMetadataCASDigest *repb.Digest
	var blobCASDigest *repb.Digest
	for _, outputFile := range ar.GetOutputFiles() {
		switch outputFile.GetPath() {
		case blobMetadataOutputFilePath:
			blobMetadataCASDigest = outputFile.GetDigest()
		case blobOutputFilePath:
			blobCASDigest = outputFile.GetDigest()
		default:
			log.CtxErrorf(ctx, "unknown output file path '%s' in ActionResult for %s", outputFile.GetPath(), ref)
		}
	}
	if blobMetadataCASDigest == nil || blobCASDigest == nil {
		return fmt.Errorf("missing blob metadata digest or blob digest for %s", ref)
	}
	blobMetadataRN := digest.NewCASResourceName(
		blobMetadataCASDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobMetadata := &ocipb.OCIBlobMetadata{}
	err = cachetools.GetBlobAsProto(ctx, bsClient, blobMetadataRN, blobMetadata)
	if err != nil {
		return err
	}
	w.Header().Add(headerDockerContentDigest, hash.String())
	w.Header().Add(headerContentLength, strconv.FormatInt(blobMetadata.GetContentLength(), 10))
	w.Header().Add(headerContentType, blobMetadata.GetContentType())
	w.WriteHeader(http.StatusOK)

	if writeBody {
		blobRN := digest.NewCASResourceName(
			blobCASDigest,
			"",
			repb.DigestFunction_SHA256,
		)
		blobRN.SetCompressor(repb.Compressor_ZSTD)
		return cachetools.GetBlob(ctx, bsClient, blobRN, w)
	} else {
		return nil
	}
}

func writeBlobOrManifestToCacheAndResponse(ctx context.Context, upstream io.Reader, w io.Writer, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, ref gcrname.Reference, ociResourceType ocipb.OCIResourceType, hash gcr.Hash, contentType string, contentLength int64) error {
	blobCASDigest := &repb.Digest{
		Hash:      hash.Hex,
		SizeBytes: contentLength,
	}
	blobRN := digest.NewCASResourceName(
		blobCASDigest,
		"",
		repb.DigestFunction_SHA256,
	)
	blobRN.SetCompressor(repb.Compressor_ZSTD)
	tr := io.TeeReader(upstream, w)
	_, _, err := cachetools.UploadFromReader(ctx, bsClient, blobRN, tr)
	if err != nil {
		return err
	}

	blobMetadata := &ocipb.OCIBlobMetadata{
		ContentLength: contentLength,
		ContentType:   contentType,
	}
	blobMetadataCASDigest, err := cachetools.UploadProto(ctx, bsClient, "", repb.DigestFunction_SHA256, blobMetadata)
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
	arDigest, err := digest.Compute(bytes.NewReader(arKeyBytes), repb.DigestFunction_SHA256)
	if err != nil {
		return err
	}
	arRN := digest.NewACResourceName(
		arDigest,
		actionResultInstanceName,
		repb.DigestFunction_SHA256,
	)
	err = cachetools.UploadActionResult(ctx, acClient, arRN, ar)
	if err != nil {
		return err
	}
	return nil
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
