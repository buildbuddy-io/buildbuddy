package registry

import (
	"context"
	"encoding/json"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
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
		for _, tag := range repository.Images {
			image := regpb.Image{
				Digest:         tag.Digest,
				Tags:           tag.Tags,
				Size:           units.BytesSize(float64(tag.SizeBytes)),
				UploadedTime:   tag.UploadedTime.Format("2006-01-02 15:04:05"),
				LastAccessTime: tag.LastAccessTime.Format("2006-01-02 15:04:05"),
				Accesses:       int64(tag.Accesses),
			}
			repoProto.Images = append(repoProto.Images, &image)
		}
		resp.Repository = append(resp.Repository, &repoProto)
	}
	return &resp, nil
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
