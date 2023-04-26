//go:build linux && !android
// +build linux,!android

package sociartifactstore

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/awslabs/soci-snapshotter/compression"
	"github.com/awslabs/soci-snapshotter/soci"
	"github.com/awslabs/soci-snapshotter/ztoc"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/dsingleflight"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/containerd/containerd/images"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/match"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
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

const (
	// Prefix for soci artifacts store in blobstore
	blobKeyPrefix = "soci-index-"

	// The span sized used to genereate the ZToCs. The ZToC generating code
	// will create access points into the ZToC approximately every this many
	// bytes.
	ztocSpanSize = 1 << 22 // about 4MB

	// The minimum layer size for generating ZToCs. Layers smaller than this won't be
	ztocMinLayerSize = 10 << 20 // about 10MB

	// The SOCI Index build tool. !!! WARNING !!! This is embedded in both the
	// ZToCs and SOCI index, so changing it will invalidate all previously
	// generated and stored soci artifacts, forcing a re-pull and re-index.
	buildToolIdentifier = "AWS SOCI CLI v0.1"

	// Media type for binary data (e.g. ZTOC format).
	octetStreamMediaType = "application/octet-stream"

	// Annotation keys used in the soci index.
	sociImageLayerDigestKey    = "com.amazon.soci.image-layer-digest"
	sociImageLayerMediaTypeKey = "com.amazon.soci.image-layer-mediaType"
	sociBuildToolIdentifierKey = "com.amazon.soci.build-tool-identifier"
)

type SociArtifactStore struct {
	cache     interfaces.Cache
	blobstore interfaces.Blobstore
	env       environment.Env

	deduper *dsingleflight.Coordinator
}

func Register(env environment.Env) error {
	err, server := NewSociArtifactStore(env)
	if err != nil {
		return err
	}
	env.SetSociArtifactStoreServer(server)
	return nil
}

func NewSociArtifactStore(env environment.Env) (error, *SociArtifactStore) {
	if env.GetCache() == nil {
		return status.FailedPreconditionError("soci artifact server requires a cache"), nil
	}
	if env.GetBlobstore() == nil {
		return status.FailedPreconditionError("soci artifact server requires a blobstore"), nil
	}
	return nil, &SociArtifactStore{
		cache:     env.GetCache(),
		blobstore: env.GetBlobstore(),
		env:       env,
		deduper:   dsingleflight.New(env.GetDefaultRedisClient()),
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

func getTargetImageRef(ctx context.Context, image string, platform *rgpb.Platform, creds *rgpb.Credentials) (ctrname.Digest, error) {
	imageRef, err := ctrname.ParseReference(image)
	if err != nil {
		return ctrname.Digest{}, status.InvalidArgumentErrorf("invalid image %q", image)
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
			return ctrname.Digest{}, status.PermissionDeniedErrorf("could not retrieve image manifest: %s", err)
		}
		return ctrname.Digest{}, status.UnavailableErrorf("could not retrieve manifest from remote: %s", err)
	}

	targetImg, err := targetImageFromDescriptor(remoteDesc, platform)
	if err != nil {
		return ctrname.Digest{}, err
	}

	// Check whether the supplied credentials are sufficient to access the
	// remote image.
	if err := checkAccess(ctx, imageRef.Context(), targetImg, authenticator); err != nil {
		return ctrname.Digest{}, err
	}

	targetImgDigest, err := targetImg.Digest()
	if err != nil {
		return ctrname.Digest{}, err
	}
	return imageRef.Context().Digest(targetImgDigest.String()), nil
}

func (s *SociArtifactStore) GetArtifacts(ctx context.Context, req *socipb.GetArtifactsRequest) (*socipb.GetArtifactsResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	imageRef, err := getTargetImageRef(ctx, req.Image, req.Platform, req.Credentials)
	if err != nil {
		return nil, err
	}
	imageRefDigest := imageRef.DigestStr()

	// Try to only read-pull-index-write once at a time to prevent hammering
	// the containter registry with a ton of parallel pull requests, and save
	// apps a bunch of parallel work.
	workKey := fmt.Sprintf("soci-artifact-store-image-" + imageRefDigest)
	respBytes, err := s.deduper.Do(ctx, workKey, func() ([]byte, error) {
		exists, err := s.blobstore.BlobExists(ctx, blobKey(imageRefDigest))
		if err != nil {
			return nil, err
		}
		var resp *socipb.GetArtifactsResponse
		if exists {
			resp, err = s.getArtifactsFromCache(ctx, imageRefDigest)
			if err != nil {
				return nil, err
			}
		} else {
			sociIndexDigest, ztocDigests, err := s.pullAndIndexImage(ctx, req.Image, imageRef.DigestStr(), req.Credentials)
			if err != nil {
				return nil, err
			}
			resp = getArtifactsResponse(imageRefDigest, sociIndexDigest, ztocDigests)
		}
		proto, err := proto.Marshal(resp)
		if err != nil {
			return nil, err
		}
		return proto, nil
	})
	var resp socipb.GetArtifactsResponse
	if err := proto.Unmarshal(respBytes, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func blobKey(hash string) string {
	return blobKeyPrefix + hash
}

func resourceName(digest *repb.Digest) *rspb.ResourceName {
	return &rspb.ResourceName{
		Digest:         digest,
		InstanceName:   "soci",
		Compressor:     repb.Compressor_IDENTITY,
		CacheType:      rspb.CacheType_CAS,
		DigestFunction: repb.DigestFunction_SHA256,
	}
}

func (s *SociArtifactStore) getArtifactsFromCache(ctx context.Context, imageId string) (*socipb.GetArtifactsResponse, error) {
	bytes, err := s.blobstore.ReadBlob(ctx, blobKey(imageId))
	if err != nil {
		return nil, err
	}
	sociIndexDigest, err := deserializeDigest(string(bytes))
	if err != nil {
		return nil, err
	}
	sociIndexBytes, err := s.cache.Get(ctx, resourceName(sociIndexDigest))
	if err != nil {
		return nil, err
	}
	ztocDigests, err := getZtocDigests(sociIndexBytes)
	if err != nil {
		return nil, err
	}
	return getArtifactsResponse(imageId, sociIndexDigest, ztocDigests), nil
}

// Serializes a repb.Digest as "<digest>/<size-bytes">
func serializeDigest(d *repb.Digest) string {
	return d.Hash + "/" + strconv.FormatInt(d.SizeBytes, 10)
}

// Deserializes a digest ("<digest>/<site-bytes") into a repb.Digest
func deserializeDigest(s string) (*repb.Digest, error) {
	pieces := strings.Split(s, "/")
	if len(pieces) != 2 {
		return nil, status.InvalidArgumentErrorf("malformed serialized digest %s", s)
	}
	size, err := strconv.ParseInt(pieces[1], 10, 64)
	if err != nil {
		return nil, err
	}
	return &repb.Digest{
		Hash:      pieces[0],
		SizeBytes: size,
	}, nil

}

func (s *SociArtifactStore) pullAndIndexImage(ctx context.Context, imageName, imageRef string, credentials *rgpb.Credentials) (*repb.Digest, []*repb.Digest, error) {
	log.Infof("soci artifacts not found, generating them for image %s", imageName)
	image, err := pullImage(ctx, imageName, credentials)
	if err != nil {
		return nil, nil, err
	}
	return s.indexImage(ctx, image)
}

func pullImage(ctx context.Context, image string, credentials *rgpb.Credentials) (v1.Image, error) {
	imageRef, err := ctrname.ParseReference(image)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("invalid image %q", image)
	}

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

func (s *SociArtifactStore) indexImage(ctx context.Context, image v1.Image) (*repb.Digest, []*repb.Digest, error) {
	layers, err := image.Layers()
	if err != nil {
		return nil, nil, err
	}
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
		Hash:      rmSha256Prefix(indexHash.String()),
		SizeBytes: indexSizeBytes,
	}
	s.cache.Set(ctx, resourceName(&indexDigest), indexBytes)
	return &indexDigest, ztocDigests, nil
}

func ztocDescriptor(layerDigest, layerMediaType string, digest *repb.Digest) *ocispec.Descriptor {
	var annotations = map[string]string{
		sociImageLayerDigestKey:    layerDigest,
		sociImageLayerMediaTypeKey: layerMediaType,
	}
	return &ocispec.Descriptor{
		MediaType:   octetStreamMediaType,
		Digest:      godigest.Digest(digest.Hash),
		Size:        digest.SizeBytes,
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

	layerTmpFile, err := os.CreateTemp("", "layer.*")
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
		Hash:      rmSha256Prefix(ztocDesc.Digest.String()),
		SizeBytes: ztocDesc.Size,
	}
	ztocBytes, err := ioutil.ReadAll(ztocReader)
	s.cache.Set(ctx, resourceName(&ztocDigest), ztocBytes)
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
		digest := rmSha256Prefix(layerIndex.Digest)
		digests = append(digests, &repb.Digest{Hash: digest, SizeBytes: layerIndex.Size})
	}
	return digests, nil
}

func getArtifactsResponse(imageId string, sociIndexDigest *repb.Digest, ztocDigests []*repb.Digest) *socipb.GetArtifactsResponse {
	var resp socipb.GetArtifactsResponse
	resp.ImageId = imageId
	resp.Artifacts = append(resp.Artifacts, &socipb.Artifact{Digest: sociIndexDigest, Type: socipb.Type_SOCI_INDEX})
	for _, ztocDigest := range ztocDigests {
		resp.Artifacts = append(resp.Artifacts, &socipb.Artifact{Digest: ztocDigest, Type: socipb.Type_ZTOC})
	}
	return &resp
}

func rmSha256Prefix(s string) string {
	return strings.ReplaceAll(s, "sha256:", "")
}
