package registry

import (
	"context"
	"encoding/base32"
	"flag"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/v1"
	"google.golang.org/protobuf/proto"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	ctrname "github.com/google/go-containerregistry/pkg/name"
)

const (
	rangeHeaderBytesPrefix = "bytes="

	registryInstanceName = "registry_artifacts"
)

var (
	// Encoding used to pass the repo domain + image name between podman and
	// the stargz store. base32 to make it a valid image name.
	imageNameEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

	manifestReqRE = regexp.MustCompile("/v2/(.+?)/manifests/(.+)")
	blobReqRE     = regexp.MustCompile("/v2/(.+?)/blobs/(.+)")

	enableRegistry        = flag.Bool("registry.enabled", false, "Whether to enable registry services")
	imageConverterBackend = flag.String("registry.image_converter_backend", "", "gRPC endpoint of the image converter service")
)

func (r *registry) convertImage(ctx context.Context, targetImageRef ctrname.Digest, credentials *rgpb.Credentials) (*rgpb.Manifest, error) {
	rsp, err := r.imageConverterClient.ConvertImage(ctx, &rgpb.ConvertImageRequest{
		Image:       targetImageRef.String(),
		Credentials: credentials,
	})
	if err != nil {
		return nil, err
	}

	if err := r.writeManifest(ctx, targetImageRef.DigestStr(), rsp.GetManifest()); err != nil {
		return nil, status.UnknownErrorf("could not write converted manifest: %s", err)
	}

	return rsp.GetManifest(), nil
}

func blobKey(digest string) string {
	return "estargz-manifest-" + digest
}

func (r *registry) writeManifest(ctx context.Context, oldDigest string, newManifest *rgpb.Manifest) error {
	newDigest := newManifest.GetDigest()

	mfProtoBytes, err := proto.Marshal(newManifest)
	if err != nil {
		return status.UnknownErrorf("could not marshal proto for manifest: %s", err)
	}
	if _, err := r.manifestStore.WriteBlob(ctx, blobKey(oldDigest), mfProtoBytes); err != nil {
		return status.UnknownErrorf("could not write manifest: %s", err)
	}
	if _, err := r.manifestStore.WriteBlob(ctx, blobKey(newDigest), mfProtoBytes); err != nil {
		return status.UnknownErrorf("could not write manifest: %s", err)
	}
	return nil
}

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

func (r *registry) getOptimizedManifest(ctx context.Context, imageName, refName string) (*rgpb.Manifest, error) {
	if !strings.Contains(refName, ":") {
		return nil, status.InvalidArgumentErrorf("optimized manifest request should not use a tag")
	}

	nameRef := fmt.Sprintf("%s@%s", imageName, refName)
	// If the manifest/image index exists in the store, it means we have
	// already converted it to estargz, and we can serve the converted
	// manifest.
	manifest, err := r.getCachedManifest(ctx, refName)
	if err != nil {
		return nil, status.UnavailableErrorf("could not check for cached manifest %s: %s", nameRef, err)
	}
	if manifest == nil {
		return nil, status.NotFoundErrorf("optimized manifest %s not found", nameRef)
	}
	return manifest, nil
}

func (r *registry) handleManifestRequest(ctx context.Context, w http.ResponseWriter, req *http.Request, imageName, refName string) {
	realName, err := imageNameEncoding.DecodeString(strings.ToUpper(imageName))
	if err != nil {
		http.Error(w, fmt.Sprintf("could not decode image image name %q: %s", imageName, err), http.StatusBadRequest)
		return
	}
	imageName = string(realName)

	manifest, err := r.getOptimizedManifest(ctx, imageName, refName)
	if err != nil {
		log.CtxWarningf(ctx, "could not get optimized manifest: %s", err)
		http.Error(w, fmt.Sprintf("could not get optimized manifest: %s", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-type", manifest.ContentType)
	if _, err := w.Write(manifest.Data); err != nil {
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

func (r *registry) getBlobSize(ctx context.Context, h v1.Hash) (int64, error) {
	rn := blobResourceName(h)
	md, err := r.cache.Metadata(ctx, rn)
	if err != nil {
		return 0, err
	}
	return md.StoredSizeBytes, nil
}

func (r *registry) handleBlobRequest(ctx context.Context, w http.ResponseWriter, req *http.Request, name, refName string) {
	reqStartTime := time.Now()

	h, err := v1.NewHash(refName)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid hash %q: %s", refName, err), http.StatusNotFound)
		return
	}

	blobSize, err := r.getBlobSize(ctx, h)
	if err != nil {
		if err != context.Canceled {
			log.CtxWarningf(ctx, "could not determine blob size: %s", err)
		}
		http.Error(w, fmt.Sprintf("could not determine blob size: %s", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(blobSize, 10))

	// If this is a HEAD request, and we have already figured out the blob
	// length then we are done.
	if req.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}

	opts := &cachetools.StreamBlobOpts{
		Offset: 0,
		Limit:  0,
	}
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

		opts.Offset = start
		opts.Limit = size
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, blobSize))
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusPartialContent)
		defer func() {
			metrics.RegistryBlobRangeLatencyUsec.Observe(float64(time.Since(reqStartTime).Microseconds()))
		}()
	}
	rn := blobResourceName(h)
	rc, err := r.cache.Reader(ctx, rn, opts.Offset, opts.Limit)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not create blob reader: %s", err), http.StatusInternalServerError)
		return
	}
	defer rc.Close()

	_, err = io.Copy(w, rc)
	if err != nil {
		if err != context.Canceled {
			log.CtxWarningf(ctx, "error serving %q: %s", req.RequestURI, err)
		}
		return
	}
}

// TODO(vadim): investigate high memory usage during conversion
type registry struct {
	env                  environment.Env
	cache                interfaces.Cache
	imageConverterClient rgpb.ImageConverterClient
	manifestStore        interfaces.Blobstore
}

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

func Register(env environment.Env) error {
	if !*enableRegistry {
		return nil
	}

	if env.GetCache() == nil {
		return status.FailedPreconditionError("Registry requires a configured cache")
	}

	convConn, err := grpc_client.DialTarget(*imageConverterBackend)
	if err != nil {
		return status.UnavailableErrorf("could not connect to image converter: %s", err)
	}
	imageConverterClient := rgpb.NewImageConverterClient(convConn)

	bs := env.GetBlobstore()
	if bs == nil {
		return status.FailedPreconditionError("Registry requires Blobstore")
	}

	mux := env.GetInternalHTTPMux()
	if mux == nil {
		return status.FailedPreconditionErrorf("Registry requires internal HTTP mux")
	}

	r := &registry{
		env:                  env,
		cache:                env.GetCache(),
		imageConverterClient: imageConverterClient,
		manifestStore:        bs,
	}
	handler := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.handleRegistryRequest(w, req)
	})
	mux.Handle("/v2/", handler)
	return nil
}
