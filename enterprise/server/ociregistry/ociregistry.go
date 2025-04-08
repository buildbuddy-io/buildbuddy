package ociregistry

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	ocipb "github.com/buildbuddy-io/buildbuddy/proto/ociregistry"
	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
)

const (
	headerAccept              = "Accept"
	headerContentType         = "Content-Type"
	headerDockerContentDigest = "Docker-Content-Digest"
	headerContentLength       = "Content-Length"
	headerAuthorization       = "Authorization"
	headerWWWAuthenticate     = "WWW-Authenticate"
	headerRange               = "Range"
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
	upresp, err := http.DefaultClient.Do(upreq.WithContext(ctx))
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

func parseContentLengthHeader(value string) (int64, error) {
	contentLength, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("could not parse %s header (value '%s'): %s", headerContentLength, value, err)
	}
	if contentLength < 0 {
		return 0, fmt.Errorf("%s header must be 0 or greater, received value '%s'", headerContentLength, value)
	}
	return contentLength, nil
}

func parseDockerContentDigestHeader(value string) (*gcr.Hash, error) {
	hash, err := gcr.NewHash(value)
	if err != nil {
		return nil, fmt.Errorf("could not parse hash from value '%s': %s", value, err)
	}
	return &hash, nil
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

	var digest gcrname.Digest
	if identifierIsDigest {
		digest = ref.Context().Digest(identifier)
	}
	if ociResourceType == ocipb.OCIResourceType_MANIFEST && !identifierIsDigest {
		// Fetching a manifest by tag.
		// Since the mapping from tag to digest can change, we only look up manifests and blobs in the CAS by digest.
		// Docker Hub does not count "version checks" (HEAD requests) against the rate limit.
		// So make a HEAD request for the manifest, find its digest, and see if we can fetch from CAS.
		// TODO(dan): Docker Hub responds with Docker-Content-Digest headers. Other registries may not. Make code resilient to header absence.
		headresp, err := r.makeUpstreamRequest(
			ctx,
			http.MethodHead,
			inreq.Header.Get(headerAccept),
			inreq.Header.Get(headerAuthorization),
			ociResourceType,
			ref,
		)
		if err != nil {
			http.Error(w, fmt.Sprintf("error fetching manifest for %s from upstream registry: %s", ref, err), http.StatusServiceUnavailable)
			return
		}
		defer headresp.Body.Close()

		if inreq.Method == http.MethodHead {
			writeUpstreamHeaders(headresp, w)
			w.WriteHeader(headresp.StatusCode)
			return
		}
		if headresp.StatusCode == http.StatusNotFound {
			writeUpstreamHeaders(headresp, w)
			http.Error(w, fmt.Sprintf("manifest for %s not found in upstream registry", ref), http.StatusNotFound)
			return
		}
		if headresp.StatusCode != http.StatusOK {
			writeUpstreamHeaders(headresp, w)
			http.Error(w, fmt.Sprintf("could not fetch manifest for %s in upstream registry", ref), headresp.StatusCode)
			return
		}

		hash, _, _, err := metadataFromResponse(headresp)
		if err != nil {
			writeUpstreamHeaders(headresp, w)
			http.Error(w, fmt.Sprintf("could not parse metadata for %s from upstream registry: %s", ref, err), http.StatusNotFound)
			return
		}
		digest = ref.Context().Digest(hash.String())
	}

	bsc := r.env.GetByteStreamClient()
	acc := r.env.GetActionCacheClient()
	casDigest, contentType, contentLength, err := oci.FetchBlobOrManifestMetadataFromCache(ctx, acc, bsc, digest, ociResourceType)
	if err == nil {
		w.Header().Add(headerDockerContentDigest, digest.DigestStr())
		w.Header().Add(headerContentLength, strconv.FormatInt(contentLength, 10))
		w.Header().Add(headerContentType, contentType)
		w.WriteHeader(http.StatusOK)

		if inreq.Method == http.MethodHead {
			return
		}

		err := oci.FetchBlobOrManifestFromCache(ctx, bsc, casDigest, w)
		if err == nil {
			return
		}
		log.CtxError(ctx, fmt.Sprintf("error fetching image %s from the CAS: %s", digest, err))
	}
	if !status.IsNotFoundError(err) {
		log.CtxError(ctx, fmt.Sprintf("error fetching image %s from the CAS: %s", digest, err))
	}

	upresp, err := r.makeUpstreamRequest(
		ctx,
		inreq.Method,
		inreq.Header.Get(headerAccept),
		inreq.Header.Get(headerAuthorization),
		ociResourceType,
		digest,
	)
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
	if upresp.StatusCode != http.StatusOK {
		_, err = io.Copy(w, upresp.Body)
		if err != nil && err != context.Canceled {
			log.CtxWarningf(ctx, "error writing response body for '%s': %s", inreq.URL, err)
		}
		return
	}

	_, uptype, uplength, err := metadataFromResponse(upresp)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not parse metadata from response: %s", err), http.StatusNotFound)
		return
	}

	tr := io.TeeReader(upresp.Body, w)
	err = oci.UploadBlobOrManifestToCache(
		ctx,
		acc,
		bsc,
		digest,
		ociResourceType,
		uptype,
		uplength,
		tr,
	)
	if err != nil && err != context.Canceled {
		log.CtxWarningf(ctx, "error writing response body to cache for '%s': %s", inreq.URL, err)
	}
}

func writeUpstreamHeaders(resp *http.Response, w http.ResponseWriter) {
	for _, header := range []string{headerContentLength, headerContentType, headerDockerContentDigest, headerWWWAuthenticate} {
		if resp.Header.Get(header) != "" {
			w.Header().Add(header, resp.Header.Get(header))
		}
	}
}

func metadataFromResponse(resp *http.Response) (*gcr.Hash, string, int64, error) {
	for _, header := range []string{headerDockerContentDigest, headerContentType, headerContentLength} {
		if resp.Header.Get(header) == "" {
			return nil, "", 0, fmt.Errorf("no %s header in response from upstream", header)
		}
	}
	hash, err := parseDockerContentDigestHeader(resp.Header.Get(headerDockerContentDigest))
	if err != nil {
		return nil, "", 0, err
	}
	contentLength, err := parseContentLengthHeader(resp.Header.Get(headerContentLength))
	if err != nil {
		return nil, "", 0, err
	}
	contentType := resp.Header.Get(headerContentType)
	return hash, contentType, contentLength, nil
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
