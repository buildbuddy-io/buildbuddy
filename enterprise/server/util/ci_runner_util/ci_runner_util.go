package ci_runner_util

import (
	"context"
	_ "embed"
	"flag"
	"path/filepath"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/bundle"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const ExecutableName = "buildbuddy_ci_runner"

var (
	RecycledCIRunnerMaxWait = flag.Duration("remote_execution.ci_runner_recycling_max_wait", 3*time.Second, "Max duration that a ci_runner task should wait for a warm runner before running on a potentially cold runner.")
	CIRunnerDefaultTimeout  = flag.Duration("remote_execution.ci_runner_default_timeout", 8*time.Hour, "Default timeout applied to all ci runners.")
)

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

// UploadInputRoot If the ci_runner can be properly initialized from the cache,
// upload the binary and return the input root digest containing the executable.
// If it can't, return an empty input root digest. In this case, the executor
// is responsible for ensuring the binary exists at the workspace root.
func UploadInputRoot(ctx context.Context, bsClient bspb.ByteStreamClient, cache interfaces.Cache, instanceName string, os string, arch string) (*repb.Digest, error) {
	if CanInitFromCache(os, arch) {
		if bsClient == nil {
			return nil, status.UnavailableError("no bytestream client configured")
		}
		if cache == nil {
			return nil, status.UnavailableError("no cache configured")
		}

		runnerBinDigest, err := cachetools.UploadBlobToCAS(ctx, bsClient, instanceName, repb.DigestFunction_BLAKE3, bundle.CiRunnerBytes)
		if err != nil {
			return nil, status.WrapError(err, "upload runner bin")
		}
		runnerName := filepath.Base(ExecutableName)
		dir := &repb.Directory{
			Files: []*repb.FileNode{{
				Name:         runnerName,
				Digest:       runnerBinDigest,
				IsExecutable: true,
			}},
		}
		return cachetools.UploadProtoToCAS(ctx, cache, instanceName, repb.DigestFunction_BLAKE3, dir)
	}
	return digest.ComputeForMessage(&repb.Directory{}, repb.DigestFunction_BLAKE3)
}
