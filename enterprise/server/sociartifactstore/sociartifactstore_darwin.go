//go:build darwin && !ios
// +build darwin,!ios

package sociartifactstore

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
	gcr "github.com/google/go-containerregistry/pkg/v1"
)

type SociArtifactStore struct{}

func Register(env environment.Env) error {
	return nil
}

func newSociArtifactStore(env environment.Env) (error, *SociArtifactStore) {
	return status.UnimplementedError("soci artifact server not supported on mac"), nil
}

func (s *SociArtifactStore) GetArtifacts(ctx context.Context, req *socipb.GetArtifactsRequest) (*socipb.GetArtifactsResponse, error) {
	return nil, status.UnimplementedError("soci artifact server not supported on mac")
}

func sociIndexKey(h gcr.Hash) (*repb.Digest, error) {
	return nil, status.UnimplementedError("soci artifact server not supported on mac")
}
