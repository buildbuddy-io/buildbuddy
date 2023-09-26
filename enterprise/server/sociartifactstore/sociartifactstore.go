//go:build linux && !android
// +build linux,!android

package sociartifactstore

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/awslabs/soci-snapshotter/soci"
	"github.com/awslabs/soci-snapshotter/ztoc"
	"github.com/awslabs/soci-snapshotter/ztoc/compression"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/containerd/containerd/images"
	"github.com/google/go-containerregistry/pkg/authn"
	v1 "github.com/google/go-containerregistry/pkg/v1"
	"github.com/google/go-containerregistry/pkg/v1/match"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	godigest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

var (
	layerStorage       = flag.String("soci_artifact_store.layer_storage", "/tmp/", "Directory in which to store pulled container image layers for indexing by soci artifact store.")
	sociIndexCacheSeed = flag.String("soci_artifact_store.cache_seed", "socicache-06052023", "If set, this seed is hashed with container image IDs to generate cache keys storing soci indexes.")
)

const (
	// The span sized used to genereate the ZToCs. The ZToC generating code
	// will create access points into the ZToC approximately every this many
	// bytes.
	ztocSpanSize = 1 << 22 // about 4MB

	// The minimum layer size for generating ZToCs. Layers smaller than this won't be
	ztocMinLayerSize = 10 << 20 // about 10MB

	// The SOCI Index build tool. !!! WARNING !!! This is embedded in both the
	// ZToCs and SOCI index, so changing it will invalidate all previously
	// generated and stored soci artifacts, forcing a re-pull and re-index.
	buildToolIdentifier = "BuildBuddy SOCI Artifact Store v0.2"

	// Media type for binary data (e.g. ZTOC format).
	octetStreamMediaType = "application/octet-stream"

	// Annotation keys used in the soci index.
	sociImageLayerDigestKey    = "com.amazon.soci.image-layer-digest"
	sociImageLayerMediaTypeKey = "com.amazon.soci.image-layer-mediaType"
	sociBuildToolIdentifierKey = "com.amazon.soci.build-tool-identifier"
)

type SociArtifactStore struct {
	cache   interfaces.Cache
	deduper interfaces.SingleFlightDeduper
	env     environment.Env
}

func Register(env environment.Env) error {
	if env.GetSingleFlightDeduper() == nil {
		return nil
	}
	err, server := newSociArtifactStore(env)
	if err != nil {
		return err
	}
	env.SetSociArtifactStoreServer(server)
	return nil
}

func newSociArtifactStore(env environment.Env) (error, *SociArtifactStore) {
	if env.GetCache() == nil {
		return status.FailedPreconditionError("soci artifact server requires a cache"), nil
	}
	if env.GetSingleFlightDeduper() == nil {
		return status.FailedPreconditionError("soci artifact server requires a single-flight deduper"), nil
	}
	return nil, &SociArtifactStore{
		cache:   env.GetCache(),
		deduper: env.GetSingleFlightDeduper(),
		env:     env,
	}
}

// targetImageFromDescriptor returns the image instance described by the remote
// descriptor. If the remote descriptor is a manifest, then the manifest is
// returned directly. If the remote descriptor is an image index, a single
// manifest is selected from the index using the provided platform options.
func targetImageFromDescriptor(remoteDesc *remote.Descriptor, platform *rgpb.Platform) (v1.Image, error) {
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

// checkAccess whether the supplied credentials are sufficient to retrieve
// the provided img.
func checkAccess(ctx context.Context, imgRepo ctrname.Repository, img v1.Image, authenticator authn.Authenticator) error {
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

func getTargetImageInfo(ctx context.Context, image string, platform *rgpb.Platform, creds *rgpb.Credentials) (targetImage ctrname.Digest, manifestConfig v1.Hash, err error) {
	imageRef, err := ctrname.ParseReference(image)
	if err != nil {
		return ctrname.Digest{}, v1.Hash{}, status.InvalidArgumentErrorf("invalid image %q", image)
	}

	var authenticator authn.Authenticator
	remoteOpts := []remote.Option{remote.WithContext(ctx)}
	if creds.GetUsername() != "" || creds.GetPassword() != "" {
		authenticator = &authn.Basic{
			Username: creds.GetUsername(),
			Password: creds.GetPassword(),
		}
		remoteOpts = append(remoteOpts, remote.WithAuth(authenticator))
	}

	remoteDesc, err := remote.Get(imageRef, remoteOpts...)
	if err != nil {
		if t, ok := err.(*transport.Error); ok && t.StatusCode == http.StatusUnauthorized {
			return ctrname.Digest{}, v1.Hash{}, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return ctrname.Digest{}, v1.Hash{}, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	targetImg, err := targetImageFromDescriptor(remoteDesc, platform)
	if err != nil {
		return ctrname.Digest{}, v1.Hash{}, err
	}

	// Check whether the supplied credentials are sufficient to access the
	// remote image.
	if err := checkAccess(ctx, imageRef.Context(), targetImg, authenticator); err != nil {
		return ctrname.Digest{}, v1.Hash{}, err
	}

	manifest, err := targetImg.Manifest()
	if err != nil {
		return ctrname.Digest{}, v1.Hash{}, err
	}

	targetImgDigest, err := targetImg.Digest()
	if err != nil {
		return ctrname.Digest{}, v1.Hash{}, err
	}
	return imageRef.Context().Digest(targetImgDigest.String()), manifest.Config.Digest, nil
}

func (s *SociArtifactStore) GetArtifacts(ctx context.Context, req *socipb.GetArtifactsRequest) (*socipb.GetArtifactsResponse, error) {
	ctx = log.EnrichContext(ctx, "group_id", getGroupIdForDebugging(ctx, s.env))
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	targetImageRef, configHash, err := getTargetImageInfo(ctx, req.Image, req.Platform, req.Credentials)
	if err != nil {
		return nil, err
	}

	// Try to only read-pull-index-write once at a time to prevent hammering
	// the containter registry with a ton of parallel pull requests, and save
	// apps a bunch of parallel work.
	workKey := fmt.Sprintf("soci-artifact-store-image-%s", configHash.Hex)
	respBytes, err := s.deduper.Do(ctx, workKey, func() ([]byte, error) {
		resp, err := s.getArtifactsFromCache(ctx, configHash)
		if status.IsNotFoundError(err) {
			log.CtxDebugf(ctx, "soci artifacts for image %s missing from cache: %s", targetImageRef.DigestStr(), err)
			sociIndexDigest, ztocDigests, err := s.pullAndIndexImage(ctx, targetImageRef, configHash, req.Credentials)
			if err != nil {
				return nil, err
			}
			resp = getArtifactsResponse(configHash, sociIndexDigest, ztocDigests)
		} else if err != nil {
			return nil, err
		}
		proto, err := proto.Marshal(resp)
		if err != nil {
			return nil, err
		}
		return proto, nil
	})
	if err != nil {
		return nil, err
	}
	var resp socipb.GetArtifactsResponse
	if err := proto.Unmarshal(respBytes, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// Accepts a container image config hash 'h' which uniquely identifies the
// image and generates the repb.Digest that should be used as the cache key
// for storing the SOCI Index for that image.
func sociIndexKey(h v1.Hash) (*repb.Digest, error) {
	buf := bytes.NewBuffer([]byte(h.Hex + *sociIndexCacheSeed))
	return digest.Compute(buf, repb.DigestFunction_SHA256)
}

// TODO(iain): consider reading/writing soci artifacts from/to the cache using
// RPC clients for ByteStream read/write and UpdatActionResult APIs so that
// reading the action result automatically checks for existence of artifacts.
func (s *SociArtifactStore) getArtifactsFromCache(ctx context.Context, imageConfigHash v1.Hash) (*socipb.GetArtifactsResponse, error) {
	sociIndexCacheKey, err := sociIndexKey(imageConfigHash)
	if err != nil {
		recordOutcome("error_generating_soci_key")
		return nil, err
	}
	sociIndexNameResourceName := digest.NewResourceName(sociIndexCacheKey, "", rspb.CacheType_AC, repb.DigestFunction_SHA256).ToProto()
	exists, err := s.cache.Contains(ctx, sociIndexNameResourceName)
	if err != nil {
		recordOutcome("soci_index_pointer_contains_error")
		return nil, err
	}
	if !exists {
		recordOutcome("soci_index_pointer_missing")
		return nil, status.NotFoundErrorf("soci index for image with manifest config %s not found in cache", imageConfigHash.Hex)
	}
	bytes, err := s.cache.Get(ctx, sociIndexNameResourceName)
	if err != nil {
		recordOutcome("soci_index_pointer_read_error")
		return nil, err
	}
	sociIndexResourceName, err := digest.ParseDownloadResourceName(string(bytes))
	if err != nil {
		recordOutcome("malformed_soci_index_pointer")
		return nil, err
	}
	sociIndexBytes, err := s.cache.Get(ctx, sociIndexResourceName.ToProto())
	if err != nil {
		recordOutcome("soci_index_read_error")
		return nil, err
	}
	ztocDigests, err := getZtocDigests(sociIndexBytes)
	if err != nil {
		recordOutcome("soci_index_parse_error")
		return nil, err
	}
	for _, ztocDigest := range ztocDigests {
		exists, err := s.cache.Contains(ctx,
			digest.NewResourceName(ztocDigest, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto())
		if err != nil {
			recordOutcome("ztoc_contains_error")
			return nil, err
		}
		if !exists {
			recordOutcome("ztoc_missing")
			return nil, status.NotFoundErrorf("ztoc %s not found in cache", ztocDigest.Hash)
		}
	}
	recordOutcome("cached")
	return getArtifactsResponse(imageConfigHash, sociIndexResourceName.GetDigest(), ztocDigests), nil
}

func (s *SociArtifactStore) pullAndIndexImage(ctx context.Context, imageRef ctrname.Digest, configHash v1.Hash, credentials *rgpb.Credentials) (*repb.Digest, []*repb.Digest, error) {
	log.CtxInfof(ctx, "soci artifacts not found, generating them for image %s", imageRef.DigestStr())
	image, err := fetchImageDescriptor(ctx, imageRef, credentials)
	if err != nil {
		return nil, nil, err
	}
	index, ztocs, err := s.indexImage(ctx, image, configHash)
	if err != nil {
		log.CtxWarningf(ctx, "error indexing image %s : %s", imageRef.DigestStr(), err)
	}
	return index, ztocs, err
}

func fetchImageDescriptor(ctx context.Context, imageRef ctrname.Digest, credentials *rgpb.Credentials) (v1.Image, error) {
	remoteOpts := []remote.Option{remote.WithContext(ctx)}
	if credentials.GetUsername() != "" || credentials.GetPassword() != "" {
		authenticator := &authn.Basic{
			Username: credentials.GetUsername(),
			Password: credentials.GetPassword(),
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

	switch remoteDesc.MediaType {
	case types.OCIImageIndex, types.DockerManifestList:
		return nil, status.InvalidArgumentErrorf("image index not expected in conversion request")
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

func (s *SociArtifactStore) indexImage(ctx context.Context, image v1.Image, configHash v1.Hash) (*repb.Digest, []*repb.Digest, error) {
	layers, err := image.Layers()
	if err != nil {
		return nil, nil, err
	}
	// TODO(iain): parallelize layer ztoc generation:
	// https://github.com/buildbuddy-io/buildbuddy-internal/issues/2267
	ztocDigests := []*repb.Digest{}
	ztocDescriptors := []ocispec.Descriptor{}
	for _, layer := range layers {
		layerDigest, err := layer.Digest()
		if err != nil {
			return nil, nil, err
		}
		layerMediaType, err := layer.MediaType()
		if err != nil {
			return nil, nil, err
		}
		layerSize, err := layer.Size()
		if err != nil {
			return nil, nil, err
		}
		if layerSize < ztocMinLayerSize {
			log.Debugf("layer %s below minimum layer size, skipping ztoc", layerDigest.String())
			continue
		}

		start := time.Now()
		// These layers are lazily fetched, so this call includes the pull time.
		ztocDigest, err := s.indexLayer(ctx, layer)
		log.Infof("pulling and indexing layer %s took %s", layerDigest.String(), time.Since(start))
		if err != nil {
			return nil, nil, err
		}
		ztocDigests = append(ztocDigests, ztocDigest)
		ztocDescriptors = append(ztocDescriptors, *ztocDescriptor(layerDigest.String(), string(layerMediaType), ztocDigest))
	}

	imageDesc, err := imageDescriptor(image)
	if err != nil {
		return nil, nil, err
	}
	annotations := map[string]string{sociBuildToolIdentifierKey: buildToolIdentifier}
	index := soci.NewIndex(ztocDescriptors, imageDesc, annotations)
	indexBytes, err := soci.MarshalIndex(index)
	if err != nil {
		return nil, nil, err
	}
	indexHash, indexSizeBytes, err := v1.SHA256(bytes.NewReader(indexBytes))
	if err != nil {
		return nil, nil, err
	}
	indexDigest := repb.Digest{
		Hash:      indexHash.Hex,
		SizeBytes: indexSizeBytes,
	}
	indexResourceName := digest.NewResourceName(&indexDigest, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256)
	err = s.cache.Set(ctx, indexResourceName.ToProto(), indexBytes)
	if err != nil {
		return nil, nil, err
	}
	sociIndexCacheKey, err := sociIndexKey(configHash)
	if err != nil {
		return nil, nil, err
	}
	// Write the pointer from image identifier to SOCI Index in the ActionCache
	// because the entry isn't content-addressable.
	serializedIndexResourceName, err := indexResourceName.DownloadString()
	if err != nil {
		return nil, nil, err
	}
	err = s.cache.Set(ctx,
		digest.NewResourceName(sociIndexCacheKey, "", rspb.CacheType_AC, repb.DigestFunction_SHA256).ToProto(),
		[]byte(serializedIndexResourceName))
	if err != nil {
		return nil, nil, err
	}
	return &indexDigest, ztocDigests, nil
}

func ztocDescriptor(layerDigest, layerMediaType string, ztocDigest *repb.Digest) *ocispec.Descriptor {
	var annotations = map[string]string{
		sociImageLayerDigestKey:    layerDigest,
		sociImageLayerMediaTypeKey: layerMediaType,
	}
	return &ocispec.Descriptor{
		MediaType:   octetStreamMediaType,
		Digest:      godigest.NewDigestFromEncoded(godigest.SHA256, ztocDigest.Hash),
		Size:        ztocDigest.SizeBytes,
		Annotations: annotations,
	}
}

func imageDescriptor(image v1.Image) (*ocispec.Descriptor, error) {
	mediaType, err := image.MediaType()
	if err != nil {
		return nil, err
	}
	digest, err := image.Digest()
	if err != nil {
		return nil, err
	}
	size, err := image.Size()
	if err != nil {
		return nil, err
	}
	return &ocispec.Descriptor{
		MediaType: string(mediaType),
		Digest:    godigest.Digest(digest.String()),
		Size:      size,
	}, nil
}

func (s *SociArtifactStore) indexLayer(ctx context.Context, layer v1.Layer) (*repb.Digest, error) {
	mediaType, err := layer.MediaType()
	if err != nil {
		return nil, err
	}
	layerDigest, err := layer.Digest()
	if err != nil {
		return nil, err
	}
	layerSize, err := layer.Size()
	if err != nil {
		return nil, err
	}

	compressionAlgo, err := images.DiffCompression(ctx, string(mediaType))
	if err != nil {
		return nil, status.NotFoundErrorf("could not determine layer compression: %s", err)
	}
	if compressionAlgo != compression.Gzip {
		return nil, status.UnimplementedErrorf("layer %s (%s) cannot be indexed because it is compressed with %s",
			layerDigest.Hex, mediaType, compressionAlgo)
	}

	// Store layers in files with random names to prevent parallel indexing of
	// images sharing layers from interfering with each other.
	tmpFileName, err := random.RandomString(10)
	if err != nil {
		return nil, err
	}
	layerTmpFileName := filepath.Join(*layerStorage, tmpFileName)
	layerTmpFile, err := os.Create(layerTmpFileName)
	defer os.Remove(layerTmpFileName)
	if err != nil {
		return nil, err
	}
	layerReader, err := layer.Compressed()
	if err != nil {
		return nil, err
	}
	numBytes, err := io.Copy(layerTmpFile, layerReader)
	if err != nil {
		return nil, err
	}
	if numBytes != layerSize {
		return nil, status.DataLossErrorf("written layer size does not match that of the digest")
	}

	ztocBuilder := ztoc.NewBuilder(buildToolIdentifier)
	toc, err := ztocBuilder.BuildZtoc(layerTmpFile.Name(), ztocSpanSize, ztoc.WithCompression(compressionAlgo))
	if err != nil {
		return nil, err
	}

	ztocReader, ztocDesc, err := ztoc.Marshal(toc)
	if err != nil {
		return nil, err
	}
	ztocDigest := repb.Digest{
		Hash:      ztocDesc.Digest.Encoded(),
		SizeBytes: ztocDesc.Size,
	}
	cacheWriter, err := s.cache.Writer(ctx,
		digest.NewResourceName(&ztocDigest, "", rspb.CacheType_CAS, repb.DigestFunction_SHA256).ToProto())
	if err != nil {
		return nil, err
	}
	defer cacheWriter.Close()
	_, err = io.Copy(cacheWriter, ztocReader)
	if err != nil {
		return nil, err
	}
	if err = cacheWriter.Commit(); err != nil {
		return nil, err
	}
	return &ztocDigest, nil
}

type SociLayerIndexStruct struct {
	Digest string `json:"digest"`
	Size   int64  `json:"size"`
	// There are some other fields too that we don't need.
}
type SociIndexStruct struct {
	Layers []SociLayerIndexStruct `json:"layers"`
	// There are some other fields too that we don't need.
}

// Returns the digests of all ztocs mentioned in the provided soci index. These
// are in the layers[].digest and layers[].size fields of the json.
func getZtocDigests(sociIndexBytes []byte) ([]*repb.Digest, error) {
	var sociIndex SociIndexStruct
	if err := json.Unmarshal(sociIndexBytes, &sociIndex); err != nil {
		return nil, err
	}
	digests := []*repb.Digest{}
	for _, layerIndex := range sociIndex.Layers {
		digest, err := godigest.Parse(layerIndex.Digest)
		if err != nil {
			return nil, err
		}
		digests = append(digests, &repb.Digest{Hash: digest.Encoded(), SizeBytes: layerIndex.Size})
	}
	return digests, nil
}

func getArtifactsResponse(imageConfigHash v1.Hash, sociIndexDigest *repb.Digest, ztocDigests []*repb.Digest) *socipb.GetArtifactsResponse {
	var resp socipb.GetArtifactsResponse
	resp.ImageId = imageConfigHash.Hex
	resp.Artifacts = append(resp.Artifacts, &socipb.Artifact{Digest: sociIndexDigest, Type: socipb.Type_SOCI_INDEX})
	for _, ztocDigest := range ztocDigests {
		resp.Artifacts = append(resp.Artifacts, &socipb.Artifact{Digest: ztocDigest, Type: socipb.Type_ZTOC})
	}
	return &resp
}

// TODO(https://github.com/buildbuddy-io/buildbuddy-internal/issues/2673): delete debugging log statements
func getGroupIdForDebugging(ctx context.Context, env environment.Env) string {
	auth := env.GetAuthenticator()
	if auth == nil {
		return ""
	}
	userInfo, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return interfaces.AuthAnonymousUser
	}
	return userInfo.GetGroupID()
}

func recordOutcome(outcome string) {
	metrics.PodmanGetSociArtifactsOutcomes.With(prometheus.Labels{
		metrics.GetSociArtifactsOutcomeTag: outcome,
	}).Inc()
}
