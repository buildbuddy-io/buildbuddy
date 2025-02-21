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
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/v1/remote"

	gcrname "github.com/google/go-containerregistry/pkg/name"
	gcr "github.com/google/go-containerregistry/pkg/v1"
)

const (
	rangeHeaderBytesPrefix = "bytes="

	dockerContentDigestHeader = "Docker-Content-Digest"
)

var (
	manifestReqRE = regexp.MustCompile("/v2/(.+?)/manifests/(.+)")
	blobReqRE     = regexp.MustCompile("/v2/(.+?)/blobs/(.+)")

	enableRegistry = flag.Bool("ociregistry.enabled", false, "Whether to enable registry services")
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
// Pushing images is not supported.
func (r *registry) handleRegistryRequest(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	log.CtxDebugf(ctx, "%s %s", req.Method, req.URL)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.env)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not attach user prefix: %s", err), http.StatusInternalServerError)
		return
	}
	// Clients issue a GET or HEAD /v2/ request to verify that this  is a registry
	// endpoint.
	if req.RequestURI == "/v2/" {
		w.WriteHeader(http.StatusOK)
		return
	}

	if m := manifestReqRE.FindStringSubmatch(req.RequestURI); len(m) == 3 {
		// The image repository name, which can include a registry host and optional port.
		// For example, "alpine" is a repository name. By default, the registry portion is index.docker.io.
		// "mycustomregistry.com:8080/alpine" is also a repository name. The registry portion is mycustomregistry.com:8080.
		repository := m[1]

		// For manifests, the identifier can be a tag (such as "latest") or a digest
		// (such as "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef").
		// The OCI image distribution spec refers to this string as <identifier>.
		// However, go-containerregistry has a separate Reference type and refers to this string as `identifier`.
		identifier := m[2] // referrred to as <reference> in the OCI distribution spec, can be a tag or digest

		r.handleManifestRequest(ctx, w, req, repository, identifier)
		return
	}

	// Request for a blob (full layer or layer chunk).
	if m := blobReqRE.FindStringSubmatch(req.RequestURI); len(m) == 3 {
		imageName := m[1]
		refName := m[2]
		r.handleBlobRequest(ctx, w, req, imageName, refName)
		return
	}
	http.NotFound(w, req)
}

func (r *registry) handleManifestRequest(ctx context.Context, w http.ResponseWriter, inreq *http.Request, repository, identifier string) {
	ref, err := parseReference(repository, identifier)
	if err != nil {
		http.Error(w, fmt.Sprintf("error parsing image repository '%s' and identifier '%s': %s", repository, identifier, err), http.StatusNotFound)
		return
	}
	u := url.URL{
		Scheme: ref.Context().Scheme(),
		Host:   ref.Context().RegistryStr(),
		Path:   fmt.Sprintf("/v2/%s/manifests/%s", ref.Context().RepositoryStr(), ref.Identifier()),
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
	w.Header().Add("Content-Type", upresp.Header.Get("Content-Type"))
	w.Header().Add("Docker-Content-Digest", upresp.Header.Get("Docker-Content-Digest"))
	w.WriteHeader(upresp.StatusCode)
	_, err = io.Copy(w, upresp.Body)
	if err != nil {
		if err != context.Canceled {
			log.CtxWarningf(ctx, "error writing response body for '%s', upstream '%s': %s", inreq.URL.String(), u.String(), err)
		}
		return
	}
}

func parseReference(repository, identifier string) (gcrname.Reference, error) {
	joiner := ":"
	if _, err := gcr.NewHash(identifier); err == nil {
		joiner = "@"
	}
	ref, err := gcrname.ParseReference(repository + joiner + identifier)
	if err != nil {
		return nil, err
	}
	return ref, nil
}

// handleBlobRequest fetches all or part of a blob from a remote registry.
// Used to pull OCI image layers.
func (r *registry) handleBlobRequest(ctx context.Context, w http.ResponseWriter, req *http.Request, imageName, refName string) {
	reqStartTime := time.Now()

	d, err := gcrname.NewDigest(imageName + "@" + refName)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid hash %q: %s", refName, err), http.StatusNotFound)
		return
	}

	layer, err := r.getBlob(ctx, d)
	if err != nil {
		http.Error(w, fmt.Sprintf("error fetching layer %q: %s", d, err), http.StatusNotFound)
		return
	}
	blobSize, err := layer.Size()
	if err != nil {
		http.Error(w, fmt.Sprintf("error getting layer size %q: %s", d, err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(blobSize, 10))

	// If this is a HEAD request, and we have already figured out the blob
	// length then we are done.
	if req.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	offset := int64(0)
	limit := int64(0)

	if r := req.Header.Get("Range"); r != "" {
		parsedRanges, err := parseRangeHeader(r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if len(parsedRanges) != 1 {
			http.Error(w, "multipart range requests not supported", http.StatusBadRequest)
			return
		}
		parsedRange := parsedRanges[0]
		start := parsedRange.start
		end := parsedRange.end
		size := parsedRange.end - parsedRange.start + 1

		offset = start
		limit = size
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, blobSize))
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusPartialContent)
		defer func() {
			metrics.RegistryBlobRangeLatencyUsec.Observe(float64(time.Since(reqStartTime).Microseconds()))
		}()
	}

	rc, err := layer.Compressed()
	if err != nil {
		http.Error(w, fmt.Sprintf("could not create blob reader: %s", err), http.StatusInternalServerError)
		return
	}
	defer rc.Close()

	if offset > 0 {
		io.CopyN(io.Discard, rc, offset)
	}
	length := blobSize
	if limit != 0 && limit < length {
		length = limit
	}
	limitedReader := io.LimitReader(rc, length)
	_, err = io.Copy(w, limitedReader)
	if err != nil {
		if err != context.Canceled {
			log.CtxWarningf(ctx, "error serving %q: %s", req.RequestURI, err)
		}
		return
	}
}

func (r *registry) getBlob(ctx context.Context, d gcrname.Digest) (gcr.Layer, error) {
	return remote.Layer(d)
}

type byteRange struct {
	start, end int64
}

func parseRangeHeader(val string) ([]byteRange, error) {
	// Format of the header value is bytes=1-10[, 10-20]
	if !strings.HasPrefix(val, rangeHeaderBytesPrefix) {
		return nil, status.FailedPreconditionErrorf("range header %q does not have valid prefix", val)
	}
	val = strings.TrimPrefix(val, rangeHeaderBytesPrefix)
	ranges := strings.Split(val, ",")
	var parsedRanges []byteRange
	for _, r := range ranges {
		rParts := strings.Split(strings.TrimSpace(r), "-")
		if len(rParts) != 2 {
			return nil, status.FailedPreconditionErrorf("range header %q not valid, invalid range %q", val, r)
		}
		start, err := strconv.ParseInt(rParts[0], 10, 64)
		if err != nil {
			return nil, status.FailedPreconditionErrorf("range header %q not valid, range %q has invalid start: %s", val, r, err)
		}
		end, err := strconv.ParseInt(rParts[1], 10, 64)
		if err != nil {
			return nil, status.FailedPreconditionErrorf("range header %q not valid, range %q has invalid end: %s", val, r, err)
		}
		if end < start {
			return nil, status.FailedPreconditionErrorf("range header %q not valid, range %q has invalid bounds", val, r)
		}
		parsedRanges = append(parsedRanges, byteRange{start: start, end: end})
	}
	return parsedRanges, nil
}
