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

	"github.com/buildbuddy-io/buildbuddy/proto/resource"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/match"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
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

func (r *registry) getCachedManifest(ctx context.Context, digest string) (*rgpb.Manifest, error) {
	mfBytes, err := r.manifestStore.ReadBlob(ctx, blobKey(digest))
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	mfProto := &rgpb.Manifest{}
	if err := proto.Unmarshal(mfBytes, mfProto); err != nil {
		return nil, status.UnknownErrorf("could not unmarshal manifest proto %s: %s", digest, err)
	}

	c, err := r.cache.WithIsolation(ctx, resource.CacheType_CAS, registryInstanceName)
	if err != nil {
		return nil, err
	}
	missing, err := c.FindMissing(ctx, mfProto.CasDependencies)
	if err != nil {
		return nil, status.UnavailableErrorf("could not check blob existence in CAS: %s", err)
	}
	// If any of the CAS dependencies are missing then pretend we don't have a
	// manifest so that we trigger a new conversion and repopulate the data in
	// the CAS.
	if len(missing) > 0 {
		log.CtxInfof(ctx, "Some blobs are missing from CAS for manifest %q, ignoring cached manifest", digest)
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

func blobResourceName(h v1.Hash) *digest.ResourceName {
	d := &repb.Digest{
		Hash: h.Hex,
		// We don't actually know the blob size.
		// Set this to a large size as a hint to the BS server that we expect it
		// to be large. The server uses this to determine the buffer size.
		SizeBytes: 1024 * 1024 * 4,
	}
	return digest.NewResourceName(d, registryInstanceName)
}

func (r *registry) getBlobSize(ctx context.Context, h v1.Hash) (int64, error) {
	rn := blobResourceName(h)

	c, err := r.cache.WithIsolation(ctx, resource.CacheType_CAS, registryInstanceName)
	if err != nil {
		return 0, err
	}
	md, err := c.MetadataDeprecated(ctx, rn.GetDigest())
	if err != nil {
		return 0, err
	}
	return md.SizeBytes, nil
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
	c, err := r.cache.WithIsolation(ctx, resource.CacheType_CAS, registryInstanceName)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not get cache: %s", err), http.StatusInternalServerError)
		return
	}
	rc, err := c.Reader(ctx, rn.GetDigest(), opts.Offset, opts.Limit)
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

// checkAccess whether the supplied credentials are sufficient to retrieve
// the provided img.
func (r *registry) checkAccess(ctx context.Context, imgRepo ctrname.Repository, img v1.Image, authenticator authn.Authenticator) error {
	// Check if we have access to all the layers.
	layers, err := img.Layers()
	if err != nil {
		return status.UnknownErrorf("could not get layers for image: %s", err)
	}
	eg, egCtx := errgroup.WithContext(ctx)
	remoteOpts := []remote.Option{remote.WithContext(egCtx)}
	if authenticator != nil {
		remoteOpts = append(remoteOpts, remote.WithAuth(authenticator))
	}
	for _, layerInfo := range layers {
		layerInfo := layerInfo
		eg.Go(func() error {
			d, err := layerInfo.Digest()
			if err != nil {
				return err
			}
			layerRef := imgRepo.Digest(d.String())
			l, err := remote.Layer(layerRef, remoteOpts...)
			if err != nil {
				return err
			}
			// This issues a HEAD request for the layer.
			_, err = l.Size()
			return err
		})
	}
	if err := eg.Wait(); err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return status.PermissionDeniedErrorf("could not retrieve image layer from remote: %s", err)
		}
		return status.UnavailableErrorf("could not retrieve image layer from remote: %s", err)
	}

	return nil
}

// targetImageFromDescriptor returns the image instance described by the remote
// descriptor. If the remote descriptor is a manifest, then the manifest is
// returned directly. If the remote descriptor is an image index, a single
// manifest is selected from the index using the provided platform options.
func (r *registry) targetImageFromDescriptor(remoteDesc *remote.Descriptor, platform *rgpb.Platform) (v1.Image, error) {
	switch remoteDesc.MediaType {
	// This is an "image index", a meta-manifest that contains a list of
	// {platform props, manifest hash} properties to allow client to decide
	// which manifest they want to use based on platform.
	case types.OCIImageIndex, types.DockerManifestList:
		imgIdx, err := remoteDesc.ImageIndex()
		if err != nil {
			return nil, status.UnknownErrorf("could not get image index from descriptor: %s", err)
		}
		imgs, err := partial.FindImages(imgIdx, match.Platforms(v1.Platform{
			Architecture: platform.GetArch(),
			OS:           platform.GetOs(),
			Variant:      platform.GetVariant(),
		}))
		if err != nil {
			return nil, status.UnavailableErrorf("could not search image index: %s", err)
		}
		if len(imgs) == 0 {
			return nil, status.NotFoundErrorf("could not find suitable image in image index")
		}
		if len(imgs) > 1 {
			return nil, status.NotFoundErrorf("found multiple matching images in image index")
		}
		return imgs[0], nil
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
		img, err := remoteDesc.Image()
		if err != nil {
			return nil, status.UnknownErrorf("could not get image from descriptor: %s", err)
		}
		return img, nil
	default:
		return nil, status.UnknownErrorf("descriptor has unknown media type %q", remoteDesc.MediaType)
	}
}

func (r *registry) GetOptimizedImage(ctx context.Context, req *rgpb.GetOptimizedImageRequest) (*rgpb.GetOptimizedImageResponse, error) {
	log.CtxInfof(ctx, "GetOptimizedImage %q", req.GetImage())
	imageRef, err := ctrname.ParseReference(req.GetImage())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", req.GetImage())
	}

	var authenticator authn.Authenticator
	remoteOpts := []remote.Option{remote.WithContext(ctx)}
	if req.GetImageCredentials().GetUsername() != "" || req.GetImageCredentials().GetPassword() != "" {
		authenticator = &authn.Basic{
			Username: req.GetImageCredentials().GetUsername(),
			Password: req.GetImageCredentials().GetPassword(),
		}
		remoteOpts = append(remoteOpts, remote.WithAuth(authenticator))
	}

	remoteDesc, err := remote.Get(imageRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return nil, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return nil, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	targetImg, err := r.targetImageFromDescriptor(remoteDesc, req.GetPlatform())
	if err != nil {
		return nil, err
	}

	// Check whether the supplied credentials are sufficient to access the
	// remote image.
	if err := r.checkAccess(ctx, imageRef.Context(), targetImg, authenticator); err != nil {
		return nil, err
	}

	targetImgDigest, err := targetImg.Digest()
	if err != nil {
		return nil, err
	}
	targetImgRef := imageRef.Context().Digest(targetImgDigest.String())

	// If we got here then it means the credentials are valid for the remote
	// repo. Now we can return the optimized image ref to the client.

	manifest, err := r.getCachedManifest(ctx, targetImgDigest.String())
	if err != nil {
		return nil, status.UnavailableErrorf("could not check for cached manifest %s: %s", imageRef, err)
	}
	if manifest != nil {
		log.CtxInfof(ctx, "Using cached manifest information")
	} else {
		convertedManifest, err := r.convertImage(ctx, targetImgRef, req.GetImageCredentials())
		if err != nil {
			return nil, status.UnknownErrorf("could not convert image: %s", err)
		}
		manifest = convertedManifest
	}

	encodedImageName := strings.ToLower(imageNameEncoding.EncodeToString([]byte(imageRef.Context().Name())))

	return &rgpb.GetOptimizedImageResponse{
		OptimizedImage: fmt.Sprintf("%s@%s", encodedImageName, manifest.Digest),
	}, nil
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

	env.SetRegistryServer(r)
	return nil
}
