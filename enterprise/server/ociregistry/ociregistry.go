package ociregistry

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ocicache"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/trafficstats"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/third_party/singleflight"
	"github.com/prometheus/client_golang/prometheus"

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

	// How long to cache the mapping from a manifest tag to its digest.
	tagDigestCacheTTL = 30 * time.Second
	// Maximum number of tag->digest mappings to cache.
	tagDigestCacheSize = 10000
)

// Mirror configures OCI registry mirrors on the registry domain.
// Currently only registries that do not require auth can be mirrored.
type Mirror struct {
	// The registry subdomain that must match for this mirror config to be in effect.
	SubDomain string `yaml:"subdomain" json:"subdomain"`
	// The namespace (request path prefix) that must match for this config to be in effect.
	Namespace string `yaml:"namespace" json:"namespace"`

	// The remote registry to mirror.
	RemoteRegistry string `yaml:"remote_registry" json:"remote_registry"`
	// The repository (path prefix) on the remote repository.
	RemoteRepository string `yaml:"remote_repository" json:"remote_repository"`
}

var (
	blobsOrManifestsReqRegexp = regexp.MustCompile("/v2/(.+?)/(blobs|manifests)/(.+)")
	enableRegistry            = flag.Bool("ociregistry.enabled", false, "Whether to enable registry services")
	registryDomain            = flag.String("ociregistry.domain", "", "The domain on which the registry is hosted.")
	mirrorConfigs             = flag.Slice("ociregistry.mirrors", []Mirror{}, "List of repositories to mirror.")
)

var (
	errNotFound = status.NotFoundError("not found")
)

// manifestResolveResult holds the outcome of a tag->digest resolution so it can be
// shared across concurrent requests via singleflight.
type manifestResolveResult struct {
	hash   *gcr.Hash
	exists bool
}

type registry struct {
	env     environment.Env
	client  *http.Client
	mirrors []Mirror
	// tagDigestCache caches the mapping from manifest tag to digest to avoid
	// hitting the upstream registry on every request for the same tag.
	tagDigestCache lru.LRU[*gcr.Hash]
	// tagDigestGroup deduplicates concurrent upstream HEAD requests for the
	// same tag so only one request is made.
	tagDigestGroup singleflight.Group[string, manifestResolveResult]
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
	tagDigestCache, err := lru.New[*gcr.Hash](&lru.Config[*gcr.Hash]{
		MaxSize:    int64(tagDigestCacheSize),
		SizeFn:     func(v *gcr.Hash) int64 { return 1 },
		TTL:        tagDigestCacheTTL,
		ThreadSafe: true,
	})
	if err != nil {
		return nil, status.InternalErrorf("could not create tag digest cache: %s", err)
	}
	r := &registry{
		env:            env,
		client:         client,
		tagDigestCache: tagDigestCache,
	}
	if *registryDomain != "" {
		r.mirrors = *mirrorConfigs
	}
	return r, nil
}

type instrumentedWriter struct {
	http.ResponseWriter
	bytesWritten int64
}

func (rw *instrumentedWriter) Write(p []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(p)
	rw.bytesWritten += int64(n)
	return n, err
}

func (r *registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	rw := &instrumentedWriter{ResponseWriter: w}
	r.handleRegistryRequest(rw, req)

	provider := "unknown"
	region := "unknown"
	ip := clientip.Get(req.Context())
	dest, err := trafficstats.Classify(ip)
	if err == nil {
		provider, region = dest.Provider, dest.Region
	}
	metrics.OCIRegistryEgressBytes.With(prometheus.Labels{
		metrics.DestinationProviderLabel: provider,
		metrics.DestinationRegionLabel:   region,
	}).Add(float64(rw.bytesWritten))
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
		repository, err := r.determineUpstreamRepository(ctx, req, m[1])
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}

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

// isRegistryDomainRequest checks if req matches the configured registry domain.
// If the request host matches, the subdomain portion of the host is returned.
// e.g. if the domain is foo.com the function will return as follows
//
//	bar.foo.com -> ("bar", true)
//	foo.com -> ("", true)
//	baz.baz.com -> ("", false)
//	baz.com -> ("", false)
func isRegistryDomainRequest(req *http.Request) (string, bool) {
	if *registryDomain == "" {
		return "", false
	}

	// Strip the port if present. This makes it easier to run the registry locally.
	host := req.Host
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	if host == *registryDomain {
		return "", true
	}

	return strings.CutSuffix(host, "."+*registryDomain)
}

func (r *registry) determineUpstreamRepository(ctx context.Context, req *http.Request, repository string) (string, error) {
	// If the request is not for the dedicated registry domain, fallback to
	// the previous DockerHub mirroring behavior.
	registrySubdomain, ok := isRegistryDomainRequest(req)
	if !ok {
		if repository == "" {
			return gcrname.DefaultRegistry, nil
		}
		return repository, nil
	}

	for _, mirror := range r.mirrors {
		if mirror.SubDomain != registrySubdomain {
			continue
		}

		// If we're not looking up a specific repository then turn the bare registry.
		if repository == "" {
			return mirror.RemoteRegistry, nil
		}

		after, found := strings.CutPrefix(repository, mirror.Namespace+"/")
		if !found {
			continue
		}

		// Rewrite the target according to the mirroring config.
		return mirror.RemoteRegistry + "/" + mirror.RemoteRepository + "/" + after, nil
	}
	return "", errNotFound
}

func (r *registry) handleV2Request(ctx context.Context, w http.ResponseWriter, inreq *http.Request) {
	remoteRegistry, err := r.determineUpstreamRepository(ctx, inreq, "")
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	// If the request is not for the DockerHub mirror, for now just return
	// an OK response which tells the client that no auth is required. For now,
	// we are only mirroring repositories that don't require auth.
	if remoteRegistry != gcrname.DefaultRegistry {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
		return
	}

	scheme := "https"
	if inreq.URL.Scheme != "" {
		scheme = inreq.URL.Scheme
	}
	u := &url.URL{
		Scheme: scheme,
		Host:   remoteRegistry,
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

func (r *registry) makeUpstreamRequest(ctx context.Context, method string, acceptHeaders []string, authorizationHeader string, ociResourceType ocipb.OCIResourceType, ref gcrname.Reference) (*http.Response, error) {
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
		return nil, status.UnavailableErrorf("could not construct %s request for upstream registry %q: %s", method, u, err)
	}
	for _, acceptHeader := range acceptHeaders {
		upreq.Header.Add(headerAccept, acceptHeader)
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

func (r *registry) resolveManifestDigest(ctx context.Context, acceptHeaders []string, authorizationHeader string, ociResourceType ocipb.OCIResourceType, ref gcrname.Reference) (*gcr.Hash, bool, error) {
	headresp, err := r.makeUpstreamRequest(ctx, http.MethodHead, acceptHeaders, authorizationHeader, ociResourceType, ref)
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
		hash, err := r.resolveTagToDigest(ctx, inreq, ociResourceType, ref)
		if err != nil {
			if status.IsNotFoundError(err) {
				http.Error(w, err.Error(), http.StatusNotFound)
			} else {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
			}
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

	upresp, err := r.makeUpstreamRequest(ctx, inreq.Method, inreq.Header.Values(headerAccept), inreq.Header.Get(headerAuthorization), ociResourceType, resolvedRef)
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
			log.CtxWarningf(ctx, "Error writing non-cacheable response body for %q (digest parse failed): %s", resolvedRef.Context(), err)
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

// resolveTagToDigest resolves a manifest tag to its digest, using an
// in-memory cache and singleflight deduplication for unauthenticated requests.
func (r *registry) resolveTagToDigest(ctx context.Context, inreq *http.Request, ociResourceType ocipb.OCIResourceType, ref gcrname.Reference) (*gcr.Hash, error) {
	// Only use the tag->digest cache for unauthenticated requests, since
	// different auth tokens may have different access to manifests.
	useTagDigestCache := inreq.Header.Get(headerAuthorization) == ""
	cacheKey := tagDigestCacheKey(ref, inreq.Header.Values(headerAccept))

	if useTagDigestCache {
		if cached, ok := r.tagDigestCache.Get(cacheKey); ok {
			return cached, nil
		}
	}

	resolve := func(ctx context.Context) (manifestResolveResult, error) {
		hash, exists, err := r.resolveManifestDigest(ctx, inreq.Header.Values(headerAccept), inreq.Header.Get(headerAuthorization), ociResourceType, ref)
		if err != nil {
			return manifestResolveResult{}, err
		}
		return manifestResolveResult{hash: hash, exists: exists}, nil
	}

	var result manifestResolveResult
	var err error
	if useTagDigestCache {
		// Deduplicate concurrent upstream requests for the same tag.
		result, _, err = r.tagDigestGroup.Do(ctx, cacheKey, resolve)
	} else {
		result, err = resolve(ctx)
	}
	if err != nil {
		return nil, status.UnavailableErrorf("could not resolve digest for manifest %q: %s", ref.Context(), err)
	}
	if !result.exists {
		return nil, status.NotFoundErrorf("could not find manifest %q", ref.Context())
	}
	if result.hash == nil {
		return nil, status.NotFoundErrorf("could not parse digest from manifest for %q", ref.Context())
	}
	if useTagDigestCache {
		r.tagDigestCache.Add(cacheKey, result.hash)
	}
	return result.hash, nil
}

// tagDigestCacheKey builds a cache key for the tag->digest cache.
// The key includes the full reference (repo + tag) and sorted accept headers,
// since content negotiation can affect which manifest digest is returned.
func tagDigestCacheKey(ref gcrname.Reference, acceptHeaders []string) string {
	sorted := make([]string, len(acceptHeaders))
	copy(sorted, acceptHeaders)
	sort.Strings(sorted)
	h := sha256.New()
	for _, s := range sorted {
		h.Write([]byte(s))
		h.Write([]byte{0})
	}
	return fmt.Sprintf("%s:%x", ref.String(), h.Sum(nil))
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
