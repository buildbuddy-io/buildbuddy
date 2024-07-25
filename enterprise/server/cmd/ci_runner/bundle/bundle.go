package bundle

import (
	"context"
	_ "embed"
	"path/filepath"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const RunnerName = "buildbuddy_ci_runner"

//go:embed buildbuddy_ci_runner
var CiRunnerBytes []byte

// CanInitFromCache The apps are built for linux/amd64. If the ci_runner will run on linux/amd64
// as well, the apps can upload the ci_runner binary to the cache for the executors
// to pull down. This will ensure the executors are using the most up-to-date
// version of the binary, even for customers using self-hosted executors that aren't
// on the latest executor version.
//
// If this returns false, the executor is responsible for ensuring the
// buildbuddy_ci_runner binary exists at the workspace root when it sees
// a ci_runner task. This will guarantee the binary is built for the correct os/arch,
// but it will not update automatically when the apps are upgraded.
func CanInitFromCache(os, arch string) bool {
	return (os == "" || os == platform.LinuxOperatingSystemName) && (arch == "" || arch == platform.AMD64ArchitectureName)
}

func UploadToCache(ctx context.Context, bsClient bspb.ByteStreamClient, cache interfaces.Cache, instanceName string) (*repb.Digest, error) {
	if bsClient == nil {
		return nil, status.UnavailableError("no bytestream client configured")
	}
	if cache == nil {
		return nil, status.UnavailableError("no cache configured")
	}

	// Can we use BLAKE3 here?
	// Workflow service is currently using SHA256, but RB uses blake3
	runnerBinDigest, err := cachetools.UploadBlobToCAS(ctx, bsClient, instanceName, repb.DigestFunction_BLAKE3, CiRunnerBytes)
	if err != nil {
		return nil, status.WrapError(err, "upload runner bin")
	}
	runnerName := filepath.Base(RunnerName)
	dir := &repb.Directory{
		Files: []*repb.FileNode{{
			Name:         runnerName,
			Digest:       runnerBinDigest,
			IsExecutable: true,
		}},
	}
	return cachetools.UploadProtoToCAS(ctx, cache, instanceName, repb.DigestFunction_BLAKE3, dir)
}
