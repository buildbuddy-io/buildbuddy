package soci

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/commandutil"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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
		return status.InternalError("soci artifact server requires a cache"), nil
	}
	if env.GetBlobstore() == nil {
		return status.InternalError("soci artifact server requires a blobstore"), nil
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
	imageId, err := imageId(ctx, req.ImageName)
	if err != nil {
		return nil, err
	}
	exists, err := s.blobstore.BlobExists(ctx, blobKey(imageId))
	if err != nil {
		return nil, err
	}
	if exists {
		return s.getArtifactsFromCache(ctx, imageId)
	}
	// TODO(iain): add a mutex to prevent multiple parallel calls
	sociIndexDigest, ztocDigests, err := pullAndIndexImage(ctx, req.ImageName, imageId)
	if err != nil {
		return nil, err
	}
	if err = s.writeArtifactsToCache(ctx, imageId, sociIndexDigest, ztocDigests); err != nil {
		return nil, err
	}
	return getArtifactsResponse(imageId, sociIndexDigest, ztocDigests), nil
}

type ImageInfo struct {
	Digest string `json:"Digest"`
}

// The image id is the digest of the actual image and changes as image tags are
// altered. Grab it by running `skopeo inspect`.
func imageId(ctx context.Context, imageName string) (string, error) {
	cmd := []string{"skopeo", "inspect", "docker://" + imageName}
	res := commandutil.Run(ctx, &repb.Command{Arguments: cmd}, "" /*=workDir*/, nil /*=statsListener*/, nil /*=stdio*/)
	if res.Error != nil {
		return "", res.Error
	}
	var info ImageInfo
	err := json.Unmarshal(res.Stdout, &info)
	if err != nil {
		log.Debugf("Error parsing json output of `skopeo inspect`: %v", err)
		return "", err
	} else if info.Digest == "" {
		log.Debugf("`skopeo inspect` returned no image info")
		return "", err
	}
	return strings.ReplaceAll(info.Digest, "sha256:", ""), nil
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

func pullAndIndexImage(ctx context.Context, imageName, imageId string) (*repb.Digest, []*repb.Digest, error) {
	log.Infof("soci artifacts not found, generating them for image %s", imageName)
	pullImage(ctx, imageName)
	return runSoci(ctx, imageName)
}

// Pulls the requested image using containerd.
// TODO(iain): remove containerd from the mix.
func pullImage(ctx context.Context, imageName string) error {
	cmd := []string{"ctr", "i", "pull", imageName}
	start := time.Now()
	res := commandutil.Run(ctx, &repb.Command{Arguments: cmd}, "" /*=workDir*/, nil /*=statsListener*/, nil /*=stdio*/)
	log.Infof("Pulling image %s took %s", imageName, time.Since(start))
	return res.Error
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
// This command writes output like this:
// layer sha256:9c72ff7b0a48cd9e951aea7375a148e0e57f05e56efbed1c39c6bc4758492b7b -> ztoc skipped
// layer sha256:48bc001ee0552d9afa5b9ca0dda2135a50d28604accfa9a43c7f664283c58351 -> ztoc skipped
// layer sha256:bda554bae2c8319953b95eeeccd1a5d960e0fed35217b22001d1576c053fc20f -> ztoc sha256:0a1c0c1f64b37bbd2f344820e1afdf7fa36d36da0caad2332b5492321246709b
// layer sha256:ed1a99fd3d74b1a636391a4f9e6dd4afcac9aa32f61d2325371192206e86d6fa -> ztoc sha256:58ef1c215e60a09aa9f347b813291b81fd0ab43884aa5884429b7f14318cd54f
// layer sha256:647d29b55f8f9e3af8adf57e17456ecc8874f42fe74764b0cae49f90de0091e7 -> ztoc sha256:19d4ed82863392b725b953fa219f96fd01049ec47db13ecd57b0005902ead8fd
// layer sha256:dd7a3bff4215db2b8ffd2f74ebb970596910cefdd98714fb8913e29b364dc75a -> ztoc sha256:d15d4f6b47331f124fb60c73a8209fd5e458044d597382faf6de5043f0fd14d3
// platform {amd64 linux  [] } -> soci index sha256:b8956274ef0fda6832bc3b2dfdafd14696534c27277e7a3294dcfb8a271131be
//
// Get the digests of all ztocs and the soci index (there should only be
// one index).
func runSoci(ctx context.Context, imageName string) (*repb.Digest, []*repb.Digest, error) {
	log.Debugf("indexing image %s", imageName)
	cmd := []string{"soci", "create", imageName}
	start := time.Now()
	res := commandutil.Run(ctx, &repb.Command{Arguments: cmd}, "" /*=workDir*/, nil /*=statsListener*/, nil /*=stdio*/)
	log.Infof("Indexing image %s took %s", imageName, time.Since(start))
	if res.Error != nil {
		return nil, nil, res.Error
	}
	return parseSociOutput(string(res.Stdout))
}

// Parses the command output described above into a digest of the soci index
// and a list of digests of the ztocs and returns those.
func parseSociOutput(output string) (*repb.Digest, []*repb.Digest, error) {
	sociIndexHash := ""
	ztocHashes := []string{}
	for _, line := range strings.Split(strings.TrimSuffix(output, "\n"), "\n") {
		fmt.Println(line)
		if strings.HasPrefix(line, "layer") {
			ztocHash := strings.Split(line, "-> ztoc ")[1]
			if ztocHash != "skipped" {
				ztocHashes = append(ztocHashes, strings.ReplaceAll(ztocHash, "sha256:", ""))
			}
		} else if strings.HasPrefix(line, "platform") {
			if sociIndexHash != "" {
				return nil, nil, status.InternalError("soci created indexes for multiple platforms -- expected exactly 1")
			}
			sociIndexHash = strings.ReplaceAll(strings.Split(line, "-> soci index ")[1], "sha256:", "")
		} else {
			log.Warningf("Unrecognized line in soci output: %s", line)
		}
	}
	if sociIndexHash == "" {
		return nil, nil, status.InternalError("no soci index found in output of `soci create`")
	}

	sociIndexDigest, err := toDigest(sociIndexHash)
	if err != nil {
		return nil, nil, err
	}
	ztocDigests := []*repb.Digest{}
	for _, ztocHash := range ztocHashes {
		ztocDigest, err := toDigest(ztocHash)
		if err != nil {
			return nil, nil, err
		}
		ztocDigests = append(ztocDigests, ztocDigest)
	}
	return sociIndexDigest, ztocDigests, nil
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
		fmt.Println(err)
		return err
	}
	err = s.cache.Set(ctx, resourceName(digest), bytes)
	if err != nil {
		fmt.Println(err)
	}
	return err
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
