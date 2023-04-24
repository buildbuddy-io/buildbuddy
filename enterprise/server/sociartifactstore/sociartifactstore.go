//go:build linux && !android
// +build linux,!android

package sociartifactstore

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/awslabs/soci-snapshotter/soci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-containerregistry/pkg/authn"
	"github.com/google/go-containerregistry/pkg/v1/match"
	"github.com/google/go-containerregistry/pkg/v1/partial"
	"github.com/google/go-containerregistry/pkg/v1/remote"
	"github.com/google/go-containerregistry/pkg/v1/remote/transport"
	"github.com/google/go-containerregistry/pkg/v1/types"
	"golang.org/x/sync/errgroup"

	rgpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
	ctrname "github.com/google/go-containerregistry/pkg/name"
	v1 "github.com/google/go-containerregistry/pkg/v1"
)

var (
	// Prefix for soci artifacts store in blobstore
	blobKeyPrefix = "soci-index-"

	// The directory where soci artifact blobs are stored
	blobDirectory = "/var/lib/soci-snapshotter-grpc/content/blobs/sha256/"

	// The name of the soci artifact database file
	sociDbPath = "/var/lib/soci-snapshotter-grpc/artifacts.db"
)

type SociArtifactStore struct {
	cache     interfaces.Cache
	blobstore interfaces.Blobstore
	env       environment.Env
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
	exists, err := s.blobstore.BlobExists(ctx, blobKey(imageRefDigest))
	if err != nil {
		return nil, err
	}
	if exists {
		return s.getArtifactsFromCache(ctx, imageRefDigest)
	}
	// TODO(iain): add a mutex to prevent multiple parallel calls
	sociIndexDigest, ztocDigests, err := pullAndIndexImage(ctx, req.Image, imageRef.DigestStr())
	if err != nil {
		return nil, err
	}
	if err = s.writeArtifactsToCache(ctx, imageRefDigest, sociIndexDigest, ztocDigests); err != nil {
		return nil, err
	}
	return getArtifactsResponse(imageRefDigest, sociIndexDigest, ztocDigests), nil
}

func blobKey(hash string) string {
	return blobKeyPrefix + hash
}

func blobPath(hash string) string {
	return blobDirectory + hash
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

func pullAndIndexImage(ctx context.Context, imageName, imageRef string) (*repb.Digest, []*repb.Digest, error) {
	log.Infof("soci artifacts not found, generating them for image %s", imageName)
	if err := pullImage(ctx, imageName); err != nil {
		return nil, nil, err
	}
	if err := runSoci(ctx, imageName); err != nil {
		return nil, nil, err
	}
	sociIndex, err := findSociIndex(ctx, imageRef)
	if err != nil {
		return nil, nil, err
	}
	// The ztocs are associated with layers, not the image in the artifact
	// database, so read them from the index instead.
	sociIndexBytes, err := os.ReadFile(blobPath(sociIndex.Hash))
	if err != nil {
		return nil, nil, err
	}
	ztocDigests, err := getZtocDigests(sociIndexBytes)
	return sociIndex, ztocDigests, nil
}

// Pulls the requested image using containerd.
// TODO(iain): remove containerd from the mix.
func pullImage(ctx context.Context, imageName string) error {
	cmd := []string{"ctr", "i", "pull", imageName}
	start := time.Now()
	defer log.Infof("Pulling image %s took %s", imageName, time.Since(start))
	return commandutil.Run(ctx, &repb.Command{Arguments: cmd}, "" /*=workDir*/, nil /*=statsListener*/, nil /*=stdio*/).Error
}

// "soci create" generates two types of artifacts that we'll want to store:
//  1. A soci index -- this is a json file that contains the ztoc of each
//     layer of the indexed image.
//  2. A bunch of ztocs (<= 1 per layer) -- ztoc stands for Zip Table of
//     Contents. This is a json file that contains a map from filename to
//     the byte offset and size where that file exists in the indexed
//     layer. Note: only layers above a certain size are indexed, so there
//     may be fewer ztocs than layers.
//
// TODO(iain): create soci artifacts directly here instead of calling out.
func runSoci(ctx context.Context, imageName string) error {
	log.Debugf("indexing image %s", imageName)
	cmd := []string{"soci", "create", imageName}
	start := time.Now()
	defer log.Infof("Indexing image %s took %s", imageName, time.Since(start))
	return commandutil.Run(ctx, &repb.Command{Arguments: cmd}, "" /*=workDir*/, nil /*=statsListener*/, nil /*=stdio*/).Error

}

func findSociIndex(ctx context.Context, imageRef string) (*repb.Digest, error) {
	// TODO(iain): make the path a variable or something
	db, err := soci.NewDB(sociDbPath)
	if err != nil {
		return nil, err
	}

	var sociIndex *repb.Digest = nil
	err = db.Walk(func(entry *soci.ArtifactEntry) error {
		if entry.Type == soci.ArtifactEntryTypeIndex && entry.ImageDigest == imageRef {
			sociIndex = &repb.Digest{
				Hash:      strings.ReplaceAll(entry.Digest, "sha256:", ""),
				SizeBytes: entry.Size,
			}
			return nil
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return sociIndex, nil
}

func toDigest(hash string) (*repb.Digest, error) {
	stat, err := os.Stat(blobPath(hash))
	if err != nil {
		return nil, err
	}
	return &repb.Digest{
		Hash:      hash,
		SizeBytes: stat.Size(),
	}, nil
}

func (s *SociArtifactStore) writeArtifactsToCache(ctx context.Context, imageId string, sociIndexDigest *repb.Digest, ztocDigests []*repb.Digest) error {
	if _, err := s.blobstore.WriteBlob(ctx, blobKey(imageId), []byte(serializeDigest(sociIndexDigest))); err != nil {
		return err
	}
	if err := s.writeArtifactToCache(ctx, sociIndexDigest); err != nil {
		return err
	}
	for _, ztocDigest := range ztocDigests {
		if err := s.writeArtifactToCache(ctx, ztocDigest); err != nil {
			return err
		}
	}
	return nil
}

func (s *SociArtifactStore) writeArtifactToCache(ctx context.Context, digest *repb.Digest) error {
	bytes, err := os.ReadFile(blobPath(digest.Hash))
	if err != nil {
		return err
	}
	return s.cache.Set(ctx, resourceName(digest), bytes)
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
		digest := strings.ReplaceAll(layerIndex.Digest, "sha256:", "")
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
