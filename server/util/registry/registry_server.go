package registry

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/docker/go-units"

	regpb "github.com/buildbuddy-io/buildbuddy/proto/registry"
)

type RegistryService struct {
	env   environment.Env
	cache interfaces.Cache
}

func Register(realEnv *real_environment.RealEnv) error {
	realEnv.SetCtrRegistryService(NewRegistryService(realEnv))
	return nil
}

func NewRegistryService(env environment.Env) *RegistryService {
	return &RegistryService{env: env, cache: env.GetCache()}
}

// TODO(iain): a lot of this is in common with stuff in manifest.go, factor out.
func (s *RegistryService) GetCatalog(ctx context.Context, req *regpb.GetCatalogRequest) (*regpb.GetCatalogResponse, error) {
	containsCatalog, _ := s.cache.Contains(ctx, &catalogResourceName)
	if !containsCatalog {
		return &regpb.GetCatalogResponse{}, nil
	}

	var catalog Catalog
	raw, err := s.cache.Get(ctx, &catalogResourceName)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &catalog); err != nil {
		return nil, err
	}

	resp := regpb.GetCatalogResponse{Repository: []*regpb.Repository{}}
	for _, repo := range catalog.Repositories {
		repoProto := regpb.Repository{Name: repo, Images: []*regpb.Image{}}
		repository := s.getRepo(ctx, repo)
		for _, image := range repository.Images {
			imageProto := regpb.Image{
				Repository:     repo,
				Digest:         image.Digest,
				Fullname:       hex.EncodeToString([]byte(fmt.Sprintf("%s:%s", repo, image.Digest))),
				Tags:           image.Tags,
				Size:           units.BytesSize(float64(image.SizeBytes)),
				UploadedTime:   image.UploadedTime.Format("2006-01-02 15:04:05"),
				LastAccessTime: image.LastAccessTime.Format("2006-01-02 15:04:05"),
				Accesses:       int64(image.Accesses),
				Checkpoint:     isCheckpoint(image),
			}
			repoProto.Images = append(repoProto.Images, &imageProto)
		}
		resp.Repository = append(resp.Repository, &repoProto)
	}
	return &resp, nil
}

func (s *RegistryService) GetImage(ctx context.Context, req *regpb.GetImageRequest) (*regpb.Image, error) {
	decodedFullNameBytes, err := hex.DecodeString(req.Fullname)
	if err != nil {
		return nil, err
	}
	decodedFullName := string(decodedFullNameBytes)
	split := strings.Split(decodedFullName, ":")
	repoName := split[0]
	tag := split[len(split)-1]
	log.Debugf("searching image repository %s for tag %s", repoName, tag)

	repository := s.getRepo(ctx, repoName)
	for _, image := range repository.Images {
		fmt.Println(image)
		if image.Digest == "sha256:"+tag {
			return &regpb.Image{
				Repository:     repoName,
				Digest:         image.Digest,
				Tags:           image.Tags,
				Size:           units.BytesSize(float64(image.SizeBytes)),
				UploadedTime:   image.UploadedTime.Format("2006-01-02 15:04:05"),
				LastAccessTime: image.LastAccessTime.Format("2006-01-02 15:04:05"),
				Accesses:       int64(image.Accesses),
				Checkpoint:     isCheckpoint(image),
			}, nil
		}
		for _, imageTag := range image.Tags {
			if imageTag == tag {
				return &regpb.Image{
					Repository:     repoName,
					Digest:         image.Digest,
					Tags:           image.Tags,
					Size:           units.BytesSize(float64(image.SizeBytes)),
					UploadedTime:   image.UploadedTime.Format("2006-01-02 15:04:05"),
					LastAccessTime: image.LastAccessTime.Format("2006-01-02 15:04:05"),
					Accesses:       int64(image.Accesses),
					Checkpoint:     isCheckpoint(image),
				}, nil
			}
		}
	}
	return &regpb.Image{}, status.NotFoundError("404 image not found!")
}

func isCheckpoint(image Image) bool {
	for _, tag := range image.Tags {
		if strings.HasPrefix(tag, "checkpoint") {
			return true
		}
	}
	return false
}

func (s *RegistryService) repoExists(ctx context.Context, repo string) bool {
	exists, _ := s.cache.Contains(ctx, repoResourceName(repo))
	return exists
}

func (s *RegistryService) getRepo(ctx context.Context, repo string) Repository {
	if !s.repoExists(ctx, repo) {
		return Repository{}
	}
	var repository Repository
	raw, _ := s.cache.Get(ctx, repoResourceName(repo))
	if err := json.Unmarshal(raw, &repository); err != nil {
		panic(err)
	}
	return repository
}
