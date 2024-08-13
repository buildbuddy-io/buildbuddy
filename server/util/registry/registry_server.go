package registry

import (
	"context"
	"encoding/json"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"

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
	for _, repo := range catalog.Repos {
		repoProto := regpb.Repository{Name: repo, Tags: []string{}}
		tags := s.getRepo(ctx, repo)
		for _, tag := range tags.Tags {
			repoProto.Tags = append(repoProto.Tags, tag.Name)
		}
		resp.Repository = append(resp.Repository, &repoProto)
	}
	return &resp, nil
}

func (s *RegistryService) repoExists(ctx context.Context, repo string) bool {
	exists, _ := s.cache.Contains(ctx, repoResourceName(repo))
	return exists
}

func (s *RegistryService) getRepo(ctx context.Context, repo string) Tags {
	if !s.repoExists(ctx, repo) {
		return Tags{}
	}
	var tags Tags
	raw, _ := s.cache.Get(ctx, repoResourceName(repo))
	if err := json.Unmarshal(raw, &tags); err != nil {
		panic(err)
	}
	return tags
}
