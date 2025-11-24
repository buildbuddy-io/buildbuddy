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

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
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

	actionCacheLabel = "action_cache"
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
	client := httpclient.New(nil, "ociregistry")
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

// resolveManifestDigest makes a HEAD request to the upstream registry and returns
// the digest from the Docker-Content-Digest header. Also validates that the client
// has access to the resource - returns appropriate error types for auth failures.
func (r *registry) resolveManifestDigest(ctx context.Context, acceptHeader, authorizationHeader string, ref gcrname.Reference) (*gcr.Hash, error) {
	headresp, err := r.makeUpstreamRequest(ctx, http.MethodHead, acceptHeader, authorizationHeader, ocipb.OCIResourceType_MANIFEST, ref)
	if err != nil {
		return nil, status.UnavailableErrorf("error making %s request to upstream registry: %s", http.MethodHead, err)
	}
	defer headresp.Body.Close()

	switch headresp.StatusCode {
	case http.StatusOK:
		value := headresp.Header.Get(headerDockerContentDigest)
		hash, err := parseDockerContentDigestHeader(value)
		if err != nil {
			return nil, status.InvalidArgumentErrorf("error parsing %s header: %s", headerDockerContentDigest, err)
		}
		return hash, nil
	case http.StatusNotFound:
		return nil, status.NotFoundErrorf("resource not found: %s", ref.Name())
	case http.StatusUnauthorized:
		return nil, status.PermissionDeniedErrorf("unauthorized access to %s", ref.Name())
	case http.StatusForbidden:
		return nil, status.PermissionDeniedErrorf("forbidden access to %s", ref.Name())
	default:
		return nil, status.UnavailableErrorf("upstream registry returned status %d for %s", headresp.StatusCode, ref.Name())
	}
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
		hash, err := r.resolveManifestDigest(ctx, inreq.Header.Get(headerAccept), inreq.Header.Get(headerAuthorization), ref)
		if err != nil {
			if status.IsNotFoundError(err) {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			if status.IsPermissionDeniedError(err) {
				http.Error(w, err.Error(), http.StatusForbidden)
				return
			}
			http.Error(w, fmt.Sprintf("Could not resolve digest for manifest %q: %s", ref.Context(), err), http.StatusServiceUnavailable)
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
		// For manifest requests by digest, validate access to upstream before serving from cache.
		// This ensures clients cannot access cached private images without authorization.
		// Skip validation for:
		// - Blob requests (blobs don't require validation per requirements)
		// - Tag requests (already validated by resolveManifestDigest above)
		if ociResourceType == ocipb.OCIResourceType_MANIFEST && identifierIsDigest {
			if _, err := r.resolveManifestDigest(ctx, inreq.Header.Get(headerAccept), inreq.Header.Get(headerAuthorization), resolvedRef); err != nil {
				if status.IsNotFoundError(err) {
					http.Error(w, err.Error(), http.StatusNotFound)
					return
				}
				if status.IsPermissionDeniedError(err) {
					http.Error(w, err.Error(), http.StatusForbidden)
					return
				}
				// Network errors, timeouts, unexpected status codes -> fail closed
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
		}

		writeBody := inreq.Method == http.MethodGet
		hash, err := gcr.NewHash(resolvedRef.Identifier())
		if err != nil {
			http.Error(w, fmt.Sprintf("Error parsing resolved digest in %q: %s", resolvedRef.Context(), err), http.StatusInternalServerError)
			return
		}
		err = fetchFromCacheWriteToResponse(ctx, w, bsClient, acClient, resolvedRef.Context(), hash, ociResourceType, writeBody, ref)
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

	err = ocicache.WriteBlobOrManifestToCacheAndWriter(ctx, upresp.Body, w, bsClient, acClient, resolvedRef.Context(), ociResourceType, hash, contentType, contentLength, ref)
	if err != nil && err != context.Canceled {
		log.CtxWarningf(ctx, "Error writing response body to cache for %q: %s", resolvedRef.Context(), err)
	}
}

func writeManifestMetadataToResponse(ctx context.Context, w http.ResponseWriter, hash gcr.Hash, mc *ocipb.OCIManifestContent) {
	w.Header().Add(headerDockerContentDigest, hash.String())
	w.Header().Add(headerContentLength, strconv.Itoa(len(mc.GetRaw())))
	w.Header().Add(headerContentType, mc.GetContentType())
}

func writeBlobMetadataToResponse(ctx context.Context, w http.ResponseWriter, hash gcr.Hash, blobMetadata *ocipb.OCIBlobMetadata) {
	w.Header().Add(headerDockerContentDigest, hash.String())
	w.Header().Add(headerContentLength, strconv.FormatInt(blobMetadata.GetContentLength(), 10))
	w.Header().Add(headerContentType, blobMetadata.GetContentType())
}

func fetchFromCacheWriteToResponse(ctx context.Context, w http.ResponseWriter, bsClient bspb.ByteStreamClient, acClient repb.ActionCacheClient, repo gcrname.Repository, hash gcr.Hash, ociResourceType ocipb.OCIResourceType, writeBody bool, originalRef gcrname.Reference) error {
	if ociResourceType == ocipb.OCIResourceType_MANIFEST {
		mc, err := ocicache.FetchManifestFromAC(ctx, acClient, repo, hash, originalRef)
		if err != nil {
			return err
		}
		writeManifestMetadataToResponse(ctx, w, hash, mc)
		w.WriteHeader(http.StatusOK)
		if !writeBody {
			return nil
		}
		if _, err := io.Copy(w, bytes.NewReader(mc.GetRaw())); err != nil {
			return err
		}
		return nil
	}

	blobMetadata, err := ocicache.FetchBlobMetadataFromCache(ctx, bsClient, acClient, repo, hash)
	if err != nil {
		return err
	}
	writeBlobMetadataToResponse(ctx, w, hash, blobMetadata)
	w.WriteHeader(http.StatusOK)

	if !writeBody {
		return nil
	}
	return ocicache.FetchBlobFromCache(ctx, w, bsClient, hash, blobMetadata.GetContentLength())
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
