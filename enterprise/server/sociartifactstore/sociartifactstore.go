//go:build linux && !android
// +build linux,!android

package sociartifactstore

import (
	"bytes"
	"context"
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
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/oci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/containerd/containerd/images"
	"github.com/google/go-containerregistry/pkg/authn"
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
	v1 "github.com/google/go-containerregistry/pkg/v1"
	godigest "github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
)

var (
	layerStorage       = flag.String("soci_artifact_store.layer_storage", "/tmp/", "Directory in which to store pulled container image layers for indexing by soci artifact store.")
	sociIndexCacheSeed = flag.String("soci_artifact_store.cache_seed", "socicache-092872023", "If set, this seed is hashed with container image IDs to generate cache keys storing soci indexes.")
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

	// ActionResult.OutputFiles.NodeProperties type definitions
	artifactTypeKey       = "type"
	sociIndexArtifactType = "soci"
	ztocArtifactType      = "ztoc"
)

type SociArtifactStore struct {
	deduper interfaces.SingleFlightDeduper
	env     environment.Env
}

func Register(env *real_environment.RealEnv) error {
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

func getTargetImageInfo(ctx context.Context, image string, platform *rgpb.Platform, creds oci.Credentials) (targetImage ctrname.Digest, manifestConfig v1.Hash, err error) {
	imageRef, err := ctrname.ParseReference(image)
	if err != nil {
		return ctrname.Digest{}, v1.Hash{}, status.InvalidArgumentErrorf("invalid image %q", image)
	}

	var authenticator authn.Authenticator
	remoteOpts := []remote.Option{remote.WithContext(ctx)}
	if !creds.IsEmpty() {
		authenticator = &authn.Basic{
			Username: creds.Username,
			Password: creds.Password,
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
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	creds, err := oci.CredentialsFromProto(req.Credentials)
	if err != nil {
		return nil, err
	}
	targetImageRef, configHash, err := getTargetImageInfo(ctx, req.Image, req.Platform, creds)
	if err != nil {
		return nil, err
	}

	resp, err := s.getArtifactsFromCache(ctx, configHash)
	if err == nil {
		return resp, nil
	}
	if err != nil && !status.IsNotFoundError(err) {
		return nil, err
	}

	log.CtxDebugf(ctx, "soci artifacts for image %s missing from cache: %s", targetImageRef.DigestStr(), err)
	// Try to only pull-index-write once at a time to prevent hammering
	// the containter registry with a ton of parallel pull requests, and save
	// apps a bunch of parallel work.
	workKey := fmt.Sprintf("soci-artifact-store-image-%s", configHash.Hex)
	respBytes, err := s.deduper.Do(ctx, workKey, func() ([]byte, error) {
		sociIndexDigest, ztocDigests, err := s.pullAndIndexImage(ctx, targetImageRef, configHash, creds)
		if err != nil {
			return nil, err
		}
		proto, err := proto.Marshal(getArtifactsResponse(configHash, sociIndexDigest, ztocDigests))
		if err != nil {
			return nil, err
		}
		return proto, nil
	})
	if err != nil {
		return nil, err
	}
	var unmarshalledResp socipb.GetArtifactsResponse
	if err := proto.Unmarshal(respBytes, &unmarshalledResp); err != nil {
		return nil, err
	}
	return &unmarshalledResp, nil
}

// Accepts a container image config hash 'h' which uniquely identifies the
// image and generates the repb.Digest that should be used as the cache key
// for storing the SOCI Index for that image.
func sociIndexKey(h v1.Hash) (*repb.Digest, error) {
	buf := bytes.NewBuffer([]byte(h.Hex + *sociIndexCacheSeed))
	return digest.Compute(buf, repb.DigestFunction_SHA256)
}

func (s *SociArtifactStore) getArtifactsFromCache(ctx context.Context, imageConfigHash v1.Hash) (*socipb.GetArtifactsResponse, error) {
	sociIndexCacheKey, err := sociIndexKey(imageConfigHash)
	if err != nil {
		recordOutcome("error_generating_soci_key")
		return nil, err
	}
	actionResult, err := s.env.GetActionCacheServer().GetActionResult(
		ctx,
		&repb.GetActionResultRequest{ActionDigest: sociIndexCacheKey})
	if err != nil {
		if status.IsNotFoundError(err) {
			recordOutcome("action_result_missing")
		} else {
			recordOutcome("action_result_error")
		}
		return nil, err
	}

	var sociIndexDigest *repb.Digest
	var ztocDigests []*repb.Digest
	for _, outputFile := range actionResult.OutputFiles {
		if len(outputFile.NodeProperties.Properties) != 1 {
			recordOutcome("action_result_missing_node_props")
			return nil, status.InternalErrorf(
				"malformed action result, expected exactly 1 node property, found %d",
				len(outputFile.NodeProperties.Properties))
		}
		if outputFile.NodeProperties.Properties[0].Name != artifactTypeKey {
			recordOutcome("action_result_bad_node_prop_key")
			return nil, status.InternalErrorf(
				"malformed action result, expected node property with key 'type', was %s",
				outputFile.NodeProperties.Properties[0].Name)
		}
		if outputFile.NodeProperties.Properties[0].Value == sociIndexArtifactType {
			sociIndexDigest = outputFile.Digest
		} else if outputFile.NodeProperties.Properties[0].Value == ztocArtifactType {
			ztocDigests = append(ztocDigests, outputFile.Digest)
		} else {
			recordOutcome("action_result_bad_node_prop_value")
			return nil, status.InternalErrorf(
				"malformed action result, unrecognized node property value: %s",
				outputFile.NodeProperties.Properties[0].Value)
		}
	}
	if sociIndexDigest == nil {
		recordOutcome("action_result_missing_soci_index")
		return nil, status.InternalError("malformed action result, missing soci index")
	}
	recordOutcome("cached")
	return getArtifactsResponse(imageConfigHash, sociIndexDigest, ztocDigests), nil
}

func (s *SociArtifactStore) pullAndIndexImage(ctx context.Context, imageRef ctrname.Digest, configHash v1.Hash, credentials oci.Credentials) (*repb.Digest, []*repb.Digest, error) {
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

func fetchImageDescriptor(ctx context.Context, imageRef ctrname.Digest, credentials oci.Credentials) (v1.Image, error) {
	remoteOpts := []remote.Option{remote.WithContext(ctx)}
	if !credentials.IsEmpty() {
		authenticator := &authn.Basic{
			Username: credentials.Username,
			Password: credentials.Password,
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
	err = s.env.GetCache().Set(ctx, indexResourceName.ToProto(), indexBytes)
	if err != nil {
		return nil, nil, err
	}
	sociIndexCacheKey, err := sociIndexKey(configHash)
	if err != nil {
		return nil, nil, err
	}

	// Store the SOCI Index as an ActionResult with all of the artifacts as
	// outputfiles, with a NodeProperty denoting their type.
	var artifacts []*repb.OutputFile
	artifacts = append(artifacts,
		&repb.OutputFile{
			Digest: indexResourceName.GetDigest(),
			NodeProperties: &repb.NodeProperties{
				Properties: []*repb.NodeProperty{&repb.NodeProperty{
					Name:  artifactTypeKey,
					Value: sociIndexArtifactType,
				}},
			},
		})
	for _, ztocDigest := range ztocDigests {
		artifacts = append(artifacts,
			&repb.OutputFile{
				Digest: ztocDigest,
				NodeProperties: &repb.NodeProperties{
					Properties: []*repb.NodeProperty{&repb.NodeProperty{
						Name:  artifactTypeKey,
						Value: ztocArtifactType,
					}},
				},
			})
	}
	req := repb.UpdateActionResultRequest{
		ActionDigest: sociIndexCacheKey,
		ActionResult: &repb.ActionResult{OutputFiles: artifacts},
	}
	if _, err = s.env.GetActionCacheServer().UpdateActionResult(ctx, &req); err != nil {
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
	cacheWriter, err := s.env.GetCache().Writer(ctx,
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

func getArtifactsResponse(imageConfigHash v1.Hash, sociIndexDigest *repb.Digest, ztocDigests []*repb.Digest) *socipb.GetArtifactsResponse {
	var resp socipb.GetArtifactsResponse
	resp.ImageId = imageConfigHash.Hex
	resp.Artifacts = append(resp.Artifacts, &socipb.Artifact{Digest: sociIndexDigest, Type: socipb.Type_SOCI_INDEX})
	for _, ztocDigest := range ztocDigests {
		resp.Artifacts = append(resp.Artifacts, &socipb.Artifact{Digest: ztocDigest, Type: socipb.Type_ZTOC})
	}
	return &resp
}

func recordOutcome(outcome string) {
	metrics.PodmanGetSociArtifactsOutcomes.With(prometheus.Labels{
		metrics.GetSociArtifactsOutcomeTag: outcome,
	}).Inc()
}
