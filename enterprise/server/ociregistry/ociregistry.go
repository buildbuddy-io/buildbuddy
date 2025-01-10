package ociregistry

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"

	// "net/http/httputil"
	// "net/url"

	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"

	//	"google.golang.org/protobuf/proto"
	//      "golang.org/x/sync/singleflight"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
)

const (
	rangeHeaderBytesPrefix = "bytes="

	registryInstanceName = "registry_artifacts"
)

var (
	manifestReqRE = regexp.MustCompile("/v2/(.+?)/manifests/(.+)")
	blobReqRE     = regexp.MustCompile("/v2/(.+?)/blobs/(.+)")

	enableRegistry = flag.Bool("ociregistry.enabled", false, "Whether to enable registry services")
)

type registry struct {
	env environment.Env
}

func Register(env environment.Env) error {
	if !*enableRegistry {
		return nil
	}

	mux := env.GetInternalHTTPMux()
	if mux == nil {
		return status.FailedPreconditionErrorf("Registry requires internal HTTP mux")
	}

	r, err := New(env)
	if err != nil {
		return err
	}

	// remote, err := url.Parse("http://localhost:9999")
	// if err != nil {
	// 	return err
	// }

	// handler := func(p *httputil.ReverseProxy) func(http.ResponseWriter, *http.Request) {
	//         return func(w http.ResponseWriter, r *http.Request) {
	//                 log.Printf("PROXYING %q", r.URL)
	//                 r.Host = remote.Host
	//                 p.ServeHTTP(w, r)
	//         }
	// }

	// proxy := httputil.NewSingleHostReverseProxy(remote)
	// mux.HandleFunc("/", handler(proxy))
	mux.Handle("/v2/", r)
	log.Debugf("TYLER: oci registry is enabled")
	return nil
}

func New(env environment.Env) (*registry, error) {
	r := &registry{
		env: env,
	}
	return r, nil
}

func getImage(ctx context.Context, ref name.Reference, credentials *rgpb.Credentials) (v1.Image, error) {
	remoteOpts := []remote.Option{remote.WithContext(ctx)}
	if credentials.GetUsername() != "" || credentials.GetPassword() != "" {
		authenticator := &authn.Basic{
			Username: credentials.GetUsername(),
			Password: credentials.GetPassword(),
		}
		remoteOpts = append(remoteOpts, remote.WithAuth(authenticator))
	}
	log.CtxDebugf(ctx, "getImage name %s registry %s", ref.Name(), ref.Context().RegistryStr())

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

/*
func (r *registry) getCachedManifest(ctx context.Context, d string) (*rgpb.Manifest, error) {
	mfBytes, err := r.manifestStore.ReadBlob(ctx, blobKey(d))
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	mfProto := &rgpb.Manifest{}
	if err := proto.Unmarshal(mfBytes, mfProto); err != nil {
		return nil, status.UnknownErrorf("could not unmarshal manifest proto %s: %s", d, err)
	}

	cacheResources := digest.ResourceNames(rspb.CacheType_CAS, registryInstanceName, mfProto.CasDependencies)
	missing, err := r.cache.FindMissing(ctx, cacheResources)
	if err != nil {
		return nil, status.UnavailableErrorf("could not check blob existence in CAS: %s", err)
	}
	// If any of the CAS dependencies are missing then pretend we don't have a
	// manifest so that we trigger a new conversion and repopulate the data in
	// the CAS.
	if len(missing) > 0 {
		log.CtxInfof(ctx, "Some blobs are missing from CAS for manifest %q, ignoring cached manifest", d)
		return nil, nil
	}
	return mfProto, nil
}
*/

func (r *registry) getManifest(ctx context.Context, imageName, refName string) ([]byte, types.MediaType, error) {
	joiner := ":"
	if _, err := v1.NewHash(refName); err == nil {
		joiner = "@"
	}
	ref, err := name.ParseReference(imageName + joiner + refName)
	if err != nil {
		return nil, "", err
	}

	// STOPSHIP(tylerw): populate rgpb.Credentials from context / headers.
	img, err := getImage(ctx, ref, &rgpb.Credentials{})
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

func (r *registry) handleManifestRequest(ctx context.Context, w http.ResponseWriter, req *http.Request, imageName, refName string) {
	log.Debugf("req: %+v", req)
	log.Debugf("handleManifestRequest: image name: %q ref: %q", imageName, refName)

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

func blobResourceName(h v1.Hash) *rspb.ResourceName {
	d := &repb.Digest{
		Hash: h.Hex,
		// We don't actually know the blob size.
		// Set this to a large size as a hint to the BS server that we expect it
		// to be large. The server uses this to determine the buffer size.
		SizeBytes: 1024 * 1024 * 4,
	}
	rn := digest.NewResourceName(d, registryInstanceName, rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto()
	return rn
}

func (r *registry) getBlob(ctx context.Context, d name.Digest) (v1.Layer, error) {
	return remote.Layer(d)
}

/*
	if env.GetByteStreamClient() == nil {
		return nil, status.FailedPreconditionError("OCI Registry requires a bytestream client")
	}
	if env.GetActionCacheClient() == nil {
		return nil, status.FailedPreconditionError("OCI Registry requires an action cache client")
	}
*/

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
		log.CtxInfof(ctx, "%s %q %s", req.Method, req.RequestURI, r)
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
	//	rn := blobResourceName(h)
	//	rc, err := r.cache.Reader(ctx, rn, offset, limit)
	rc, err := layer.Uncompressed()
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

func (r *registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	r.handleRegistryRequest(w, req)
}

/*
	func getManifest() {
	  if isCached(manifest) {
	    return cached(manifest)
	 }
	  data, blah := singleflight.Do(func() {
	    cacheManifest()
	  })
	  return data
	}

same for blobs
*/
func (r *registry) handleRegistryRequest(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.env)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not attach user prefix: %s", err), http.StatusInternalServerError)
		return
	}
	log.CtxInfof(ctx, "%s %q", req.Method, req.RequestURI)
	// Clients issue a GET /v2/ request to verify that this is a registry
	// endpoint.
	if req.RequestURI == "/v2/" {
		w.WriteHeader(http.StatusOK)
		return
	}
	// Request for a manifest or image index.
	if m := manifestReqRE.FindStringSubmatch(req.RequestURI); len(m) == 3 {
		r.handleManifestRequest(ctx, w, req, m[1], m[2])
		return
	}
	// Request for a blob (full layer or layer chunk).
	if m := blobReqRE.FindStringSubmatch(req.RequestURI); len(m) == 3 {
		r.handleBlobRequest(ctx, w, req, m[1], m[2])
		return
	}
	http.NotFound(w, req)
}
