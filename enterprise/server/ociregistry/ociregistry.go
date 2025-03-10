package ociregistry

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"regexp"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
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
	actionResultInstanceName   = "_bb_ociregistry_"
)

var (
	blobsOrManifestsReqRegexp = regexp.MustCompile("/v2/(.+?)/(blobs|manifests)/(.+)")
	enableRegistry            = flag.Bool("ociregistry.enabled", false, "Whether to enable registry services")
)

type registry struct {
	env environment.Env
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
	r := &registry{
		env: env,
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
	u := url.URL{
		Scheme: scheme,
		Host:   gcrname.DefaultRegistry,
		Path:   "/v2/",
	}
	upreq, err := http.NewRequest(inreq.Method, u.String(), nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not make %s request to upstream registry '%s': %s", inreq.Method, u.String(), err), http.StatusNotFound)
		return
	}
	upresp, err := http.DefaultClient.Do(upreq.WithContext(ctx))
	if err != nil {
		http.Error(w, fmt.Sprintf("transport error making %s request to upstream registry '%s': %s", inreq.Method, u.String(), err), http.StatusNotFound)
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
			log.CtxWarningf(ctx, "error writing response body for '%s', upstream '%s': %s", inreq.URL.String(), u.String(), err)
		}
		return
	}
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

	bsClient := r.env.GetByteStreamClient()
	acClient := r.env.GetActionCacheClient()
	if identifierIsDigest {
		writeBody := inreq.Method == http.MethodGet
		err := fetchBlobOrManifestFromCache(ctx, w, bsClient, acClient, ref, ociResourceType, writeBody)
		if err == nil {
			return
		} else if !status.IsNotFoundError(err) {
			message := fmt.Sprintf("error fetching image repository '%s' and identifier '%s' from the CAS: %s", repository, identifier, err)
			log.CtxError(ctx, message)
			http.Error(w, message, http.StatusNotFound)
			return
		}
	}

	var path string
	switch ociResourceType {
	case ocipb.OCIResourceType_BLOB:
		path = "/v2/" + ref.Context().RepositoryStr() + "/blobs/" + ref.Identifier()
	case ocipb.OCIResourceType_MANIFEST:
		path = "/v2/" + ref.Context().RepositoryStr() + "/manifests/" + ref.Identifier()
	case ocipb.OCIResourceType_UNKNOWN:
		message := fmt.Sprintf("unknown OCI resource type, expected blobs or manifests: %s", inreq.URL.Path)
		log.CtxError(ctx, message)
		http.Error(w, message, http.StatusNotFound)
		return
	}
	u := url.URL{
		Scheme: ref.Context().Scheme(),
		Host:   ref.Context().RegistryStr(),
		Path:   path,
	}
	upreq, err := http.NewRequest(inreq.Method, u.String(), nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not make %s request to upstream registry '%s': %s", inreq.Method, u.String(), err), http.StatusNotFound)
		return
	}
	if inreq.Header.Get(headerAccept) != "" {
		upreq.Header.Set(headerAccept, inreq.Header.Get(headerAccept))
	}
	if inreq.Header.Get(headerAuthorization) != "" {
		upreq.Header.Set(headerAuthorization, inreq.Header.Get(headerAuthorization))
	}
	upresp, err := http.DefaultClient.Do(upreq.WithContext(ctx))
	if err != nil {
		http.Error(w, fmt.Sprintf("transport error making %s request to upstream registry '%s': %s", inreq.Method, u.String(), err), http.StatusNotFound)
		return
	}
	defer upresp.Body.Close()

	for _, header := range []string{headerContentLength, headerContentType, headerDockerContentDigest, headerWWWAuthenticate} {
		if upresp.Header.Get(header) != "" {
			w.Header().Add(header, upresp.Header.Get(header))
		}
	}
	w.WriteHeader(upresp.StatusCode)

	hasContentLength := upresp.Header.Get(headerContentLength) != ""
	var contentLength int64
	if hasContentLength {
		contentLength, err = strconv.ParseInt(upresp.Header.Get(headerContentLength), 10, 64)
		if err != nil {
			http.Error(w, fmt.Sprintf("could not parse %s header (value '%s') from upstream: %s", headerContentLength, upresp.Header.Get(headerContentLength), err), http.StatusNotFound)
			return
		}
		if contentLength < 0 {
			http.Error(w, fmt.Sprintf("%s header must be 0 or greater, received value '%s' from upstream: %s", headerContentLength, upresp.Header.Get(headerContentLength), err), http.StatusNotFound)
			return
		}
	}

	hasContentType := upresp.Header.Get(headerContentType) != ""
	contentType := upresp.Header.Get(headerContentType)

	hasDockerContentDigest := upresp.Header.Get(headerDockerContentDigest) != ""
	var hash gcr.Hash
	if hasDockerContentDigest {
		hash, err = gcr.NewHash(upresp.Header.Get(headerDockerContentDigest))
		if err != nil {
			http.Error(w, fmt.Sprintf("could not parse %s header (value '%s') from upstream: %s", headerDockerContentDigest, upresp.Header.Get(headerDockerContentDigest), err), http.StatusNotFound)
			return
		}
	}

	if upresp.StatusCode == http.StatusOK && inreq.Method == http.MethodGet && hasContentLength && hasDockerContentDigest && hasContentType {
		err := writeBlobOrManifestToCacheAndResponse(ctx, upresp.Body, w, bsClient, acClient, ref, ociResourceType, hash, contentType, contentLength)
		if err != nil && err != context.Canceled {
			log.CtxWarningf(ctx, "error writing response body to cache for '%s', upstream '%s': %s", inreq.URL.String(), u.String(), err)
		}
	} else {
		_, err = io.Copy(w, upresp.Body)
		if err != nil && err != context.Canceled {
			log.CtxWarningf(ctx, "error writing response body for '%s', upstream '%s': %s", inreq.URL.String(), u.String(), err)
		}
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
	arRN := digest.NewResourceName(
		arDigest,
		actionResultInstanceName,
		rspb.CacheType_AC,
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
	blobMetadataRN := digest.NewResourceName(
		blobMetadataCASDigest,
		"",
		rspb.CacheType_CAS,
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
		blobRN := digest.NewResourceName(
			blobCASDigest,
			"",
			rspb.CacheType_CAS,
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
	blobRN := digest.NewResourceName(
		blobCASDigest,
		"",
		rspb.CacheType_CAS,
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
	arRN := digest.NewResourceName(
		arDigest,
		actionResultInstanceName,
		rspb.CacheType_AC,
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
