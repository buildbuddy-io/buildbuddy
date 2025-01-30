package ociregistry

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"

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
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
)

const rangeHeaderBytesPrefix = "bytes="

var (
	manifestReqRE = regexp.MustCompile("/v2/(.+?)/manifests/(.+)")
	blobReqRE     = regexp.MustCompile("/v2/(.+?)/blobs/(.+)")
	blobUploadRE  = regexp.MustCompile("/v2/(.+?)/blobs/uploads/")

	enableRegistry = flag.Bool("ociregistry.enabled", false, "Whether to enable registry services")
)

type registry struct {
	env environment.Env
}

func Register(env *real_environment.RealEnv) error {
	if !*enableRegistry {
		return nil
	}

	mux := env.GetInternalHTTPMux()
	if mux == nil {
		return status.FailedPreconditionErrorf("OCI registry requires internal HTTP mux")
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
	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.env)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not attach user prefix: %s", err), http.StatusInternalServerError)
		return
	}
	// Clients issue a GET /v2/ request to verify that this  is a registry
	// endpoint.
	if req.RequestURI == "/v2/" {
		w.WriteHeader(http.StatusOK)
		return
	}
	// X-Forwarded-Host is set to the host name requested by the client.
	// oci.Resolve() sets this header so we know which remote registry to pull from.
	forwardedRegistry := req.Header.Get("X-Forwarded-Host")
	// Request for a manifest or image index.
	if m := manifestReqRE.FindStringSubmatch(req.RequestURI); len(m) == 3 {
		imageName := m[1]
		refName := m[2]
		if forwardedRegistry != "" {
			imageName = forwardedRegistry + "/" + imageName
		}
		r.handleManifestRequest(ctx, w, req, imageName, refName)
		return
	}
	// Uploading a blob is not supported.
	if m := blobUploadRE.FindStringSubmatch(req.RequestURI); len(m) == 2 {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
	// Request for a blob (full layer or layer chunk).
	if m := blobReqRE.FindStringSubmatch(req.RequestURI); len(m) == 3 {
		imageName := m[1]
		refName := m[2]
		if forwardedRegistry != "" {
			imageName = forwardedRegistry + "/" + imageName
		}
		r.handleBlobRequest(ctx, w, req, imageName, refName)
		return
	}
	http.NotFound(w, req)
}

func (r *registry) handleManifestRequest(ctx context.Context, w http.ResponseWriter, req *http.Request, imageName, refName string) {
	manifestBytes, mediaType, err := r.getManifest(ctx, imageName, refName)
	if err != nil {
		log.CtxWarningf(ctx, "could not get manifest: %s", err)
		http.Error(w, fmt.Sprintf("could not get manifest: %s", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-type", string(mediaType))
	if _, err := w.Write(manifestBytes); err != nil {
		log.CtxWarningf(ctx, "error serving cached manifest: %s", err)
	}
}

// Fetch the manifest for an OCI image from a remote registry.
func (r *registry) getManifest(ctx context.Context, imageName, refName string) ([]byte, types.MediaType, error) {
	joiner := ":"
	if _, err := v1.NewHash(refName); err == nil {
		joiner = "@"
	}
	ref, err := name.ParseReference(imageName + joiner + refName)
	if err != nil {
		return nil, "", err
	}

	img, err := getImage(ctx, ref)
	if err != nil {
		return nil, "", err
	}

	manifest, err := img.Manifest()
	if err != nil {
		return nil, "", status.NotFoundErrorf("error fetching manifest for %s: %s", ref, err)
	}

	manifestBytes, err := img.RawManifest()
	if err != nil {
		return nil, "", status.NotFoundErrorf("error fetching manifest bytes for %s: %s", ref, err)
	}

	return manifestBytes, manifest.MediaType, nil
}

// Fetch a reference to an OCI image in a remote repository.
// Used to pull manifest information for the image.
func getImage(ctx context.Context, ref name.Reference) (v1.Image, error) {
	remoteOpts := []remote.Option{remote.WithContext(ctx)}
	remoteDesc, err := remote.Get(ref, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	// Image() should resolve both images and image indices to an appropriate image
	img, err := remoteDesc.Image()
	if err != nil {
		switch remoteDesc.MediaType {
		// This is an "image index", a meta-manifest that contains a list of
		// {platform props, manifest hash} properties to allow client to decide
		// which manifest they want to use based on platform.
		case types.OCIImageIndex, types.DockerManifestList:
			return nil, status.UnknownErrorf("could not get image in image index from descriptor: %s", err)
		case types.OCIManifestSchema1, types.DockerManifestSchema2:
			return nil, status.UnknownErrorf("could not get image from descriptor: %s", err)
		default:
			return nil, status.UnknownErrorf("descriptor has unknown media type %q, oci error: %s", remoteDesc.MediaType, err)
		}
	}
	return img, nil
}

// handleBlobRequest fetches all or part of a blob from a remote registry.
// Used to pull OCI image layers.
func (r *registry) handleBlobRequest(ctx context.Context, w http.ResponseWriter, req *http.Request, imageName, refName string) {
	reqStartTime := time.Now()

	d, err := name.NewDigest(imageName + "@" + refName)
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

func (r *registry) getBlob(ctx context.Context, d name.Digest) (v1.Layer, error) {
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
