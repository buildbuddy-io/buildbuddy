package sociartifactstore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/awslabs/soci-snapshotter/soci"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/registry"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
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

func (s *SociArtifactStore) GetArtifacts(ctx context.Context, req *socipb.GetArtifactsRequest) (*socipb.GetArtifactsResponse, error) {
	ctx, err := prefix.AttachUserPrefixToContext(ctx, s.env)
	if err != nil {
		return nil, err
	}
	imageRef, err := registry.GetTargetImageRef(ctx, req.Image, req.Platform, req.Credentials)
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
	return "soci-index-" + hash
}

func blobPath(hash string) string {
	return "/var/lib/soci-snapshotter-grpc/content/blobs/sha256/" + hash
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
	return findSociArtifacts(ctx, imageRef)
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

func findSociArtifacts(ctx context.Context, imageRef string) (*repb.Digest, []*repb.Digest, error) {
	// TODO(iain): make the path a variable or something
	db, err := soci.NewDB("/var/lib/soci-snapshotter-grpc/artifacts.db")
	if err != nil {
		return nil, nil, err
	}

	var sociIndex *repb.Digest = nil
	ztocIndexes := []*repb.Digest{}
	err = db.Walk(func(entry *soci.ArtifactEntry) error {
		if entry.Type == soci.ArtifactEntryTypeIndex && entry.ImageDigest == imageRef {
			sociIndex = &repb.Digest{
				Hash:      entry.Digest,
				SizeBytes: entry.Size,
			}
			fmt.Println("===== Found SOCI index =====")
			fmt.Println(sociIndex)
		} else if entry.Type == soci.ArtifactEntryTypeLayer && entry.ImageDigest == imageRef {
			ztocIndexes = append(ztocIndexes, &repb.Digest{
				Hash:      entry.Digest,
				SizeBytes: entry.Size,
			})
			fmt.Println("===== Found ZTOC =====")
			fmt.Println(sociIndex)
		}
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return sociIndex, ztocIndexes, nil
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
