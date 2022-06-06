package main

import (
	"archive/tar"
	"bytes"
	"context"
	"encoding/base32"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/blobstore"
	"github.com/buildbuddy-io/buildbuddy/server/config"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/ssl"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/empty"
	"github.com/google/go-containerregistry/pkg/v1/mutate"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/tarball"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"

	regpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const (
	rangeHeaderBytesPrefix = "bytes="

	registryInstanceName = "registry_artifacts"
)

var (
	// Encoding used to pass the repo domain + image name between podman and
	// the stargz store. base32 to make it a valid image name.
	imageNameEncoding = base32.StdEncoding.WithPadding(base32.NoPadding)

	manifestReqRE    = regexp.MustCompile("/v2/(.+?)/manifests/(.+)")
	optManifestReqRE = regexp.MustCompile("/v2/(.+?)/optimizedManifests/(.+)")
	blobReqRE        = regexp.MustCompile("/v2/(.+?)/blobs/(.+)")

	port       = flag.Int("port", 8080, "The port to listen for HTTP traffic on")
	sslPort    = flag.Int("ssl_port", 8081, "The port to listen for HTTPS traffic on")
	serverType = flag.String("server_type", "registry-server", "The server type to match on health checks")

	casBackend = flag.String("registry.cas_backend", "", "")
)

// isEmptyLayer checks whether the given layer does not contain any files.
func isEmptyLayer(layer v1.Layer) (bool, error) {
	d, err := layer.Digest()
	if err != nil {
		return false, status.UnknownErrorf("could not get layer digest: %s", err)
	}

	size, err := layer.Size()
	if err != nil {
		return false, status.UnknownErrorf("could not determine layer size for layer %q: %s", d, err)
	}
	// An empty tar.gz archive contains an "end of file" record, so it's not 0
	// bytes.
	if size > 100 {
		return false, nil
	}
	lr, err := layer.Uncompressed()
	if err != nil {
		return false, status.UnknownErrorf("could not create uncompressed reader for layer %q: %s", d, err)
	}
	defer lr.Close()
	r := tar.NewReader(lr)
	// If the archive is empty, r.Next should immediately return an EOF error
	// to indicate there are no files contained inside.
	_, err = r.Next()
	if err == io.EOF {
		return true, nil
	}
	if err != nil {
		return false, status.UnknownErrorf("error reading tar layer %q: %s", d, err)
	}
	return false, nil
}

type convertedLayer struct {
	v1.Layer
	casDigest *repb.Digest
}

// convertLayer converts the layer to estargz format. May return nil if the
// layer should be skipped.
func (r *registry) convertLayer(ctx context.Context, l v1.Layer) (*convertedLayer, error) {
	// Lovely hack.
	// podman + stargzstore does not properly handle duplicate layers
	// in the same image. You would think this wouldn't happen in
	// practice, but it seems that some image generation libraries
	// can include multiple empty layers (why???) causing container
	// mounting to fail.
	// To get around this, we don't include any empty layers in the
	// converted image.
	emptyLayer, err := isEmptyLayer(l)
	if err != nil {
		return nil, err
	}
	if emptyLayer {
		return nil, nil
	}

	newLayer, err := tarball.LayerFromOpener(func() (io.ReadCloser, error) {
		layerReader, err := l.Uncompressed()
		if err != nil {
			return nil, status.UnknownErrorf("could not create uncompressed reader: %s", err)
		}
		return layerReader, nil
	}, tarball.WithEstargz)
	if err != nil {
		return nil, status.UnknownErrorf("could not create tarball layer reader: %s", err)
	}

	newLayerDigest, err := newLayer.Digest()
	if err != nil {
		return nil, status.UnknownErrorf("could not get digest for converted layer: %s", err)
	}
	newLayerSize, err := newLayer.Size()
	if err != nil {
		return nil, status.UnknownErrorf("could not get size for converted layer: %s", err)
	}

	newLayerReader, err := newLayer.Compressed()
	if err != nil {
		return nil, err
	}

	rn := digest.NewResourceName(&repb.Digest{Hash: newLayerDigest.Hex, SizeBytes: newLayerSize}, registryInstanceName)
	casDigest, err := cachetools.UploadFromReader(ctx, r.bsClient, rn, newLayerReader)
	if err != nil {
		return nil, status.UnknownErrorf("could not upload converted layer %q: %s", newLayerDigest, err)
	}

	return &convertedLayer{
		Layer:     newLayer,
		casDigest: casDigest,
	}, nil
}

// dedupeConvertImage waits for an existing conversion or starts a new one if no
// conversion is in progress for the given image.
func (r *registry) dedupeConvertImage(ctx context.Context, img v1.Image) (manifestLike, error) {
	d, err := img.Digest()
	if err != nil {
		return nil, err
	}
	v, err, _ := r.workDeduper.Do(d.String(), func() (interface{}, error) {
		return r.convertImage(ctx, img)
	})
	return v.(manifestLike), err
}

type convertedImage struct {
	v1.Image
	casDependencies []*repb.Digest
}

func (ci *convertedImage) CASDependencies() []*repb.Digest {
	return ci.casDependencies
}

func (r *registry) convertImage(ctx context.Context, img v1.Image) (manifestLike, error) {
	manifest, err := img.Manifest()
	if err != nil {
		return nil, status.UnknownErrorf("could not get image manifest: %s", err)
	}
	cnf, err := img.ConfigFile()
	if err != nil {
		return nil, status.UnknownErrorf("could not get image config; %s", err)
	}

	// This field will be populated with new layers as we do the conversion.
	// We need to clear this to remove references to the old layers.
	cnf.RootFS.DiffIDs = nil

	newImg, err := mutate.ConfigFile(empty.Image, cnf)
	if err != nil {
		return nil, status.UnknownErrorf("could not add config to image: %s", err)
	}

	eg, egCtx := errgroup.WithContext(ctx)

	var mu sync.Mutex
	convertedLayers := make([]*convertedLayer, len(manifest.Layers))

	for layerIdx, layerDesc := range manifest.Layers {
		layerIdx := layerIdx
		layerDesc := layerDesc

		eg.Go(func() error {
			l, err := img.LayerByDigest(layerDesc.Digest)
			if err != nil {
				return status.UnknownErrorf("could not lookup layer %q: %s", layerDesc.Digest, err)
			}

			newLayer, err := r.convertLayer(egCtx, l)
			if err != nil {
				return status.UnknownErrorf("could not convert layer %q: %s", layerDesc.Digest, err)
			}
			if newLayer == nil {
				return nil
			}

			mu.Lock()
			convertedLayers[layerIdx] = newLayer
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	var casDependencies []*repb.Digest
	for _, l := range convertedLayers {
		// Layer should be omitted.
		if l == nil {
			continue
		}
		newImg, err = mutate.AppendLayers(newImg, l)
		if err != nil {
			return nil, err
		}
		casDependencies = append(casDependencies, l.casDigest)
	}

	newConfigBytes, err := newImg.RawConfigFile()
	if err != nil {
		return nil, status.UnknownErrorf("could not get new config file: %s", err)
	}
	configCASDigest, err := cachetools.UploadBlob(ctx, r.bsClient, registryInstanceName, bytes.NewReader(newConfigBytes))
	if err != nil {
		return nil, status.UnknownErrorf("could not upload converted image config %s", err)
	}
	casDependencies = append(casDependencies, configCASDigest)

	ci := &convertedImage{
		Image:           newImg,
		casDependencies: casDependencies,
	}
	if err := r.writeManifest(ctx, img, ci); err != nil {
		return nil, status.UnknownErrorf("could not write converted manifest: %s", err)
	}

	return ci, nil
}

// dedupeConvertImageIndex waits for an existing conversion or starts a new one
// if no conversion is in progress for the given image index.
func (r *registry) dedupeConvertImageIndex(ctx context.Context, imgIdx v1.ImageIndex) (manifestLike, error) {
	d, err := imgIdx.Digest()
	if err != nil {
		return nil, err
	}
	v, err, _ := r.workDeduper.Do(d.String(), func() (interface{}, error) {
		return r.convertImageIndex(ctx, imgIdx)
	})
	return v.(manifestLike), err
}

type convertedImageIndex struct {
	v1.ImageIndex
	casDependencies []*repb.Digest
}

func (cii *convertedImageIndex) CASDependencies() []*repb.Digest {
	return cii.casDependencies
}

func (r *registry) convertImageIndex(ctx context.Context, imgIdx v1.ImageIndex) (manifestLike, error) {
	manifest, err := imgIdx.IndexManifest()
	if err != nil {
		return nil, status.UnknownErrorf("could not retrieve image index manifest: %s", err)
	}

	eg, egCtx := errgroup.WithContext(ctx)

	var mu sync.Mutex
	var convertedIndices []mutate.IndexAddendum
	var casDependencies []*repb.Digest
	for _, m := range manifest.Manifests {
		m := m
		img, err := imgIdx.Image(m.Digest)
		if err != nil {
			return nil, status.UnknownErrorf("could not construct image ref for manifest digest %q: %s", m.Digest, err)
		}
		eg.Go(func() error {
			newImg, err := r.dedupeConvertImage(egCtx, img)
			if err != nil {
				return status.UnknownErrorf("could not convert image %q: %s", m.Digest, err)
			}
			mu.Lock()
			convertedIndices = append(convertedIndices, mutate.IndexAddendum{
				Add: newImg,
				// The manifest properties are not copied automatically, so we need
				// to copy at least the platform properties for the image index
				// to be usable.
				Descriptor: v1.Descriptor{
					Platform: m.Platform,
				},
			})
			casDependencies = append(casDependencies, newImg.CASDependencies()...)
			mu.Unlock()
			return nil
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	newIdx := mutate.AppendManifests(v1.ImageIndex(empty.Index), convertedIndices...)

	cii := &convertedImageIndex{
		ImageIndex:      newIdx,
		casDependencies: casDependencies,
	}
	if err := r.writeManifest(ctx, imgIdx, cii); err != nil {
		return nil, status.UnknownErrorf("could not write converted manifest: %s", err)
	}

	return cii, nil
}

func blobKey(digest string) string {
	return "estargz-manifest-" + digest
}

func manifestProto(manifest manifestLike) (*regpb.Manifest, error) {
	manifestBytes, err := manifest.RawManifest()
	if err != nil {
		return nil, status.UnknownErrorf("could not get manifest: %s", err)
	}
	manifestMediaType, err := manifest.MediaType()
	if err != nil {
		return nil, status.UnknownErrorf("could not get media type: %s", err)
	}

	manifestDigest, err := manifest.Digest()
	if err != nil {
		return nil, status.UnknownErrorf("could not get digest: %s", err)
	}

	return &regpb.Manifest{
		Digest:          manifestDigest.String(),
		Data:            manifestBytes,
		ContentType:     string(manifestMediaType),
		CasDependencies: manifest.CASDependencies(),
	}, nil
}

func (r *registry) writeManifest(ctx context.Context, oldManifest partial.Describable, newManifest manifestLike) error {
	manifestProto, err := manifestProto(newManifest)
	if err != nil {
		return err
	}
	oldDigest, err := oldManifest.Digest()
	if err != nil {
		return status.UnknownErrorf("could not get digest for old manifest: %s", err)
	}
	newDigest, err := newManifest.Digest()
	if err != nil {
		return status.UnknownErrorf("could not get digest for new manifest: %s", err)
	}

	mfProtoBytes, err := proto.Marshal(manifestProto)
	if err != nil {
		return status.UnknownErrorf("could not marshal proto for manifest: %s", err)
	}

	// Store the manifest both at the "old" digest and the new digest.
	// If a client requests image by tag, we will retrieve the original digest
	// from the remote and use that for the lookup.
	// Client can also request the manifest by digest in which case it will do
	// so by the new/converted digest.
	if _, err := r.manifestStore.WriteBlob(ctx, blobKey(oldDigest.String()), mfProtoBytes); err != nil {
		return status.UnknownErrorf("could not write manifest: %s", err)
	}
	if _, err := r.manifestStore.WriteBlob(ctx, blobKey(newDigest.String()), mfProtoBytes); err != nil {
		return status.UnknownErrorf("could not write manifest: %s", err)
	}
	return nil
}

func (r *registry) convert(ctx context.Context, req *request, remoteDesc *remote.Descriptor) (*regpb.Manifest, error) {
	req.logger.Infof("Converting %q", remoteDesc.Ref)

	start := time.Now()

	var servable manifestLike
	switch remoteDesc.MediaType {
	// This is an "image index", a meta-manifest that contains a list of
	// {platform props, manifest hash} properties to allow client to decide
	// which manifest they want to use based on platform.
	case types.OCIImageIndex, types.DockerManifestList:
		imgIdx, err := remoteDesc.ImageIndex()
		if err != nil {
			return nil, status.UnknownErrorf("could not get image index from descriptor: %s", err)
		}
		newImgIdx, err := r.dedupeConvertImageIndex(ctx, imgIdx)
		if err != nil {
			return nil, status.UnknownErrorf("could not convert image index: %s", err)
		}
		servable = newImgIdx
	case types.OCIManifestSchema1, types.DockerManifestSchema2:
		img, err := remoteDesc.Image()
		if err != nil {
			return nil, status.UnknownErrorf("could not get image from descriptor: %s", err)
		}
		newImg, err := r.dedupeConvertImage(ctx, img)
		if err != nil {
			return nil, status.UnknownErrorf("could not convert image: %s", err)
		}
		servable = newImg
	default:
		return nil, status.UnknownErrorf("descriptor has unknown media type %q", remoteDesc.MediaType)
	}

	req.logger.Infof("Conversion for %q took %s", remoteDesc.Ref, time.Now().Sub(start))

	manifestProto, err := manifestProto(servable)
	if err != nil {
		return nil, err
	}
	return manifestProto, nil
}

func (r *registry) getCachedManifest(ctx context.Context, digest string) (*regpb.Manifest, error) {
	mfBytes, err := r.manifestStore.ReadBlob(ctx, blobKey(digest))
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, nil
		}
		return nil, err
	}
	mfProto := &regpb.Manifest{}
	if err := proto.Unmarshal(mfBytes, mfProto); err != nil {
		return nil, status.UnknownErrorf("could not unmarshal manifest proto %s: %s", digest, err)
	}

	rsp, err := r.casClient.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{
		InstanceName: registryInstanceName,
		BlobDigests:  mfProto.CasDependencies,
	})
	if err != nil {
		return nil, status.UnavailableErrorf("could not check blob existence in CAS: %s", err)
	}
	// If any of the CAS dependencies are missing then pretend we don't have a
	// manifest so that we trigger a new conversion and repopulate the data in
	// the CAS.
	if len(rsp.GetMissingBlobDigests()) > 0 {
		return nil, nil
	}
	return mfProto, nil
}

func (r *registry) getOptimizedManifest(ctx context.Context, req *request, imageName, refName string) (*regpb.Manifest, error) {
	// If the ref contains ":" then it's a digest (e.g. sha256:52f4...),
	// otherwise it's a tag reference.
	var nameRef string
	var byDigest bool
	if strings.Contains(refName, ":") {
		nameRef = fmt.Sprintf("%s@%s", imageName, refName)
		byDigest = true
	} else {
		// If this is a tag reference, we'll check upstream to determine where
		// this tag currently points to.
		nameRef = fmt.Sprintf("%s:%s", imageName, refName)
		byDigest = false
	}

	ref, err := ctrname.ParseReference(nameRef)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid reference %q", nameRef)
	}

	var remoteDesc *remote.Descriptor
	if byDigest {
		// If the manifest/image index exists in the store, it means we have
		// already converted it to estargz, and we can serve the converted
		// manifest.
		manifest, err := r.getCachedManifest(ctx, refName)
		if err != nil {
			return nil, status.UnavailableErrorf("could not check for cached manifest %s: %s", nameRef, err)
		}
		if manifest != nil {
			return manifest, nil
		}
		desc, err := remote.Get(ref, remote.WithContext(ctx))
		if err != nil {
			return nil, status.UnavailableErrorf("could not fetch remote manifest %q: %s", nameRef, err)
		}
		remoteDesc = desc
	} else {
		// Check where this tag points to and see if it's already cached.
		desc, err := remote.Get(ref, remote.WithContext(ctx))
		if err != nil {
			return nil, status.UnavailableErrorf("could not fetch remote manifest %q: %s", nameRef, err)
		}
		manifest, err := r.getCachedManifest(ctx, desc.Digest.String())
		if err != nil {
			return nil, status.UnavailableErrorf("could not check for cached manifest %s: %s", nameRef, err)
		}
		if manifest != nil {
			return manifest, nil
		}
		remoteDesc = desc
	}

	return r.convert(ctx, req, remoteDesc)
}

// HTTP request with a per-request logger.
type request struct {
	*http.Request
	logger log.Logger
}

// for json marshalling
type optRef struct {
	Ref string
}

func (r *registry) handleOptimizedManifestRequest(w http.ResponseWriter, req *request, encodedImageName, refName string) {
	ib, err := imageNameEncoding.DecodeString(strings.ToUpper(encodedImageName))
	if err != nil {
		http.Error(w, fmt.Sprintf("could not decode image image name %q: %s", encodedImageName, err), http.StatusBadRequest)
		return
	}
	realImageName := string(ib)

	manifest, err := r.getOptimizedManifest(req.Context(), req, realImageName, refName)
	if err != nil {
		http.Error(w, fmt.Sprintf("could not get optimized manifest: %s", err), http.StatusInternalServerError)
		return
	}

	resolved := fmt.Sprintf("%s@%s", encodedImageName, manifest.Digest)
	b, err := json.Marshal(&optRef{Ref: resolved})
	if err != nil {
		http.Error(w, fmt.Sprintf("could not get optimized manifest: %s", err), http.StatusInternalServerError)
	}
	w.Write(b)
	w.Write([]byte("\n"))
}

func (r *registry) handleManifestRequest(w http.ResponseWriter, req *request, imageName, refName string) {
	realName, err := imageNameEncoding.DecodeString(strings.ToUpper(imageName))
	if err != nil {
		http.Error(w, fmt.Sprintf("could not decode image image name %q: %s", imageName, err), http.StatusBadRequest)
		return
	}
	imageName = string(realName)

	manifest, err := r.getOptimizedManifest(req.Context(), req, imageName, refName)
	if err != nil {
		req.logger.Warningf("could not get optimized manifest: %s", err)
		http.Error(w, fmt.Sprintf("could not get optimized manifest: %s", err), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-type", manifest.ContentType)
	if _, err := w.Write(manifest.Data); err != nil {
		req.logger.Warningf("error serving cached manifest: %s", err)
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
	ranges := strings.Split(val, ", ")
	var parsedRanges []byteRange
	for _, r := range ranges {
		rParts := strings.Split(r, "-")
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

// TODO(vadim): extend CAS API to allow us to query blob size
func (r *registry) getBlobSize(ctx context.Context, h v1.Hash) (int64, error) {
	if v, ok := r.blobSizeLRU.Get(h.String()); ok {
		return v.(int64), nil
	}

	out := io.Discard
	rn := blobResourceName(h)
	n, err := cachetools.GetBlobChunk(ctx, r.bsClient, rn, &cachetools.StreamBlobOpts{Offset: 0, Limit: 0}, out)
	if err != nil {
		return 0, err
	}

	r.blobSizeLRU.Add(h.String(), n)

	return n, nil
}

func (r *registry) handleBlobRequest(w http.ResponseWriter, req *request, name, refName string) {
	h, err := v1.NewHash(refName)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid hash %q: %s", refName, err), http.StatusNotFound)
		return
	}

	blobSize, err := r.getBlobSize(req.Context(), h)
	if err != nil {
		if err != context.Canceled {
			req.logger.Warningf("could not determine blob size: %s", err)
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
		req.logger.Infof("%s %q %s", req.Method, req.RequestURI, r)
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
	}
	rn := blobResourceName(h)
	_, err = cachetools.GetBlobChunk(req.Context(), r.bsClient, rn, opts, w)
	if err != nil {
		if err != context.Canceled {
			req.logger.Warningf("error serving %q: %s", req.RequestURI, err)
		}
		return
	}
}

// covers both manifests and image indexes ("meta manifests")
type manifestLike interface {
	mutate.Appendable
	partial.WithRawManifest
	partial.Describable
	CASDependencies() []*repb.Digest
}

// TODO(vadim): investigate high memory usage during conversion
type registry struct {
	casClient     repb.ContentAddressableStorageClient
	bsClient      bspb.ByteStreamClient
	manifestStore interfaces.Blobstore
	blobSizeLRU   interfaces.LRU

	workDeduper singleflight.Group
}

func (r *registry) Start(ctx context.Context, hc interfaces.HealthChecker, env environment.Env) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v2/", func(w http.ResponseWriter, req *http.Request) {
		id, err := uuid.NewRandom()
		if err != nil {
			http.Error(w, fmt.Sprintf("could not generate request ID: %s", err), http.StatusInternalServerError)
			return
		}
		wReq := &request{Request: req, logger: log.NamedSubLogger(id.String())}

		wReq.logger.Infof("%s %q", req.Method, req.RequestURI)
		// Clients issue a GET /v2/ request to verify that this is a registry
		// endpoint.
		if req.RequestURI == "/v2/" {
			w.WriteHeader(http.StatusOK)
			return
		}
		// Request for a manifest or image index.
		if m := manifestReqRE.FindStringSubmatch(req.RequestURI); len(m) == 3 {
			r.handleManifestRequest(w, wReq, m[1], m[2])
			return
		}
		// Request for a blob (full layer or layer chunk).
		if m := blobReqRE.FindStringSubmatch(req.RequestURI); len(m) == 3 {
			r.handleBlobRequest(w, wReq, m[1], m[2])
			return
		}
		// This is a BuildBuddy specific endpoint that allows us to tell
		// the client which manifest to use for a given image reference.
		if m := optManifestReqRE.FindStringSubmatch(req.RequestURI); len(m) == 3 {
			r.handleOptimizedManifestRequest(w, wReq, m[1], m[2])
			return
		}
		http.NotFound(w, req)
	})
	mux.Handle("/readyz", hc.ReadinessHandler())
	mux.Handle("/healthz", hc.LivenessHandler())

	if env.GetSSLService().IsEnabled() {
		log.Infof("Starting HTTPS server on port %d", *sslPort)
		tlsConfig, mux := env.GetSSLService().ConfigureTLS(mux)
		sslSrv := &http.Server{
			Handler:   mux,
			TLSConfig: tlsConfig,
		}
		sslLis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *sslPort))
		if err != nil {
			log.Fatalf("could not listen on SSL port %d: %s", *sslPort, err)
		}
		go func() {
			_ = sslSrv.ServeTLS(sslLis, "", "")
		}()
	}

	log.Infof("Starting HTTP server on port %d", *port)
	srv := &http.Server{
		Handler: mux,
	}
	hc.RegisterShutdownFunction(func(ctx context.Context) error {
		log.Infof("Shutting down server...")
		return srv.Shutdown(ctx)
	})

	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", *port))
	if err != nil {
		log.Fatalf("could not listen on port %d: %s", *port, err)
	}

	_ = srv.Serve(lis)

}

func main() {
	flag.Parse()

	if err := flagutil.PopulateFlagsFromFile(config.Path()); err != nil {
		log.Fatalf("Error loading config from file: %s", err)
	}

	if err := log.Configure(); err != nil {
		fmt.Printf("Error configuring logging: %s", err)
		os.Exit(1)
	}

	healthChecker := healthcheck.NewHealthChecker(*serverType)
	env := real_environment.NewRealEnv(healthChecker)

	bs, err := blobstore.GetConfiguredBlobstore(env)
	if err != nil {
		log.Fatalf("could not configure blobstore: %s", err)
	}

	if err := ssl.Register(env); err != nil {
		log.Fatalf("could not configure SSL: %s", err)
	}

	conn, err := grpc_client.DialTarget(*casBackend)
	if err != nil {
		log.Fatalf("could not connect to cas: %s", err)
	}
	casClient := repb.NewContentAddressableStorageClient(conn)
	bsClient := bspb.NewByteStreamClient(conn)

	blobSizeLRU, err := lru.NewLRU(&lru.Config{
		MaxSize: 100_000,
		SizeFn:  func(value interface{}) int64 { return 1 },
	})
	if err != nil {
		log.Fatalf("could not initialize blob size LRU: %s", err)
	}

	r := &registry{
		casClient:     casClient,
		bsClient:      bsClient,
		manifestStore: bs,
		blobSizeLRU:   blobSizeLRU,
	}
	r.Start(env.GetServerContext(), healthChecker, env)
}
