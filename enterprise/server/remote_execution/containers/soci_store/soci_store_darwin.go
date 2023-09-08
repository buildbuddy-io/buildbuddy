//go:build darwin && !ios
// +build darwin,!ios

package soci_store

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/container"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	sspb "github.com/awslabs/soci-snapshotter/proto"
	socipb "github.com/buildbuddy-io/buildbuddy/proto/soci"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type SociArtifactStore struct{}

func Register(env environment.Env) error {
	return nil
}

func RunSociStoreWithRetries(ctx context.Context, port int) {
	panic("soci_store not supported on mac")
}

func RunSociStore(ctx context.Context, port int) error {
	return status.UnimplementedError("soci_store not supported on mac")
}

func InitializeSociStoreKeychainClient(env environment.Env, target string) (sspb.LocalKeychainClient, error) {
	return nil, status.UnimplementedError("soci_store not supported on mac")
}

func PutCredentials(ctx context.Context, image string, creds container.PullCredentials, client sspb.LocalKeychainClient) error {
	return status.UnimplementedError("soci_store not supported on mac")
}

func WriteArtifacts(ctx context.Context, bsc bspb.ByteStreamClient, resp *socipb.GetArtifactsResponse) error {
	return status.UnimplementedError("soci_store not supported on mac")
}

func EnableStreamingStoreArg() string {
	return ""
}

func StorePath() string {
	return ""
}
