package soci

import (
	"encoding/json"
	"io"
	"strconv"
	"strings"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

type SociArtifactStore struct {
	cache     interfaces.Cache
	blobstore interfaces.Blobstore
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
	}
}

func (s *SociArtifactStore) Read(req *socipb.ReadRequest, stream socipb.SociArtifactStore_ReadServer) error {
	ctx := stream.Context()

	// Search for the digest of the soci index in blobstore and fetch the
	// contents from the cache if found.
	bytes, err := s.blobstore.ReadBlob(ctx, blobKey(req.ImageManifestDigest))
	if err != nil {
		return err
	}
	sociIndexDigest, err := deserializeDigest(string(bytes))
	if err != nil {
		return err
	}
	sociIndexBytes, err := s.cache.Get(ctx, resourceName(sociIndexDigest))
	if err != nil {
		return err
	}

	// Send the soci index to the client and then read the digests of the ztocs
	// it refers to, pull those from the cache, and send to the client as well.
	if err := stream.Send(&socipb.Artifact{Digest: sociIndexDigest, Type: socipb.Type_SOCI_INDEX, Data: sociIndexBytes}); err != nil {
		return err
	}
	ztocDigests, err := getZtocDigests(sociIndexBytes)
	if err != nil {
		return err
	}
	for _, ztocDigest := range ztocDigests {
		ztocBytes, err := s.cache.Get(ctx, resourceName(ztocDigest))
		if err != nil {
			return err
		}
		if err := stream.Send(&socipb.Artifact{Digest: ztocDigest, Type: socipb.Type_ZTOC, Data: ztocBytes}); err != nil {
			return err
		}
	}
	return nil
}

func (s *SociArtifactStore) Write(stream socipb.SociArtifactStore_WriteServer) error {
	ctx := stream.Context()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if req.Artifact.Type == socipb.Type_UNKNOWN_TYPE {
			return status.InvalidArgumentError("received UNKNOWN soci artifact type")
		}
		if req.Artifact.Type == socipb.Type_SOCI_INDEX {
			// When we receive a soci index, write an entry in the blobstore
			// named with the image manifest's digest so we can look it up
			// later.
			s.blobstore.WriteBlob(ctx, blobKey(req.ImageManifestDigest), []byte(serializeDigest(req.Artifact.Digest)))
		}
		s.cache.Set(ctx, resourceName(req.Artifact.Digest), req.Artifact.Data)
	}
	return nil
}

func blobKey(digest string) string {
	return "soci-index-" + digest
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
		digest := strings.Trim(layerIndex.Digest, "sha256:")
		digests = append(digests, &repb.Digest{Hash: digest, SizeBytes: layerIndex.Size})
	}
	return digests, nil
}
