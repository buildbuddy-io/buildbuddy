package ci_runner_util

import (
	"context"
	_ "embed"
	"flag"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/bundle"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/githubapp"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_env"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	cli_bundle "github.com/buildbuddy-io/buildbuddy/server/util/bb"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

const ExecutableName = "buildbuddy_ci_runner"
const CLIBinaryName = "bb"

var (
	RecycledCIRunnerMaxWait = flag.Duration("remote_execution.ci_runner_recycling_max_wait", 3*time.Second, "Max duration that a ci_runner task should wait for a warm runner before running on a potentially cold runner.")
	CIRunnerDefaultTimeout  = flag.Duration("remote_execution.ci_runner_default_timeout", 8*time.Hour, "Default timeout applied to all ci runners.")
	InitCIRunnerFromCache   = flag.Bool("remote_execution.init_ci_runner_from_cache", true, "Whether the apps should upload ci_runner binaries to the cache so executors can fetch the latest versions without upgrading.")
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
	return *InitCIRunnerFromCache &&
		(os == "" || os == platform.LinuxOperatingSystemName) &&
		(arch == "" || arch == platform.AMD64ArchitectureName)
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

		if len(bundle.CiRunnerBytes) == 0 {
			return nil, status.InternalError("CI runner binary not embedded")
		}
		if len(cli_bundle.CLIBytes) == 0 {
			return nil, status.InternalError("CLI binary not embedded")
		}

		var runnerBinDigest, bbBinDigest *repb.Digest
		eg, egCtx := errgroup.WithContext(ctx)
		eg.Go(func() error {
			var err error
			runnerBinDigest, err = cachetools.UploadBlobToCAS(egCtx, bsClient, instanceName, repb.DigestFunction_BLAKE3, bundle.CiRunnerBytes)
			return status.WrapError(err, "upload runner bin")
		})
		eg.Go(func() error {
			var err error
			bbBinDigest, err = cachetools.UploadBlobToCAS(egCtx, bsClient, instanceName, repb.DigestFunction_BLAKE3, cli_bundle.CLIBytes)
			return status.WrapError(err, "upload bb bin")
		})
		if err := eg.Wait(); err != nil {
			return nil, err
		}

		runnerName := filepath.Base(ExecutableName)
		dir := &repb.Directory{
			Files: []*repb.FileNode{
				{
					Name:         runnerName,
					Digest:       runnerBinDigest,
					IsExecutable: true,
				},
				{
					Name:         CLIBinaryName,
					Digest:       bbBinDigest,
					IsExecutable: true,
				},
			},
		}
		return cachetools.UploadProtoToCAS(ctx, cache, instanceName, repb.DigestFunction_BLAKE3, dir)
	}
	return digest.ComputeForMessage(&repb.Directory{}, repb.DigestFunction_BLAKE3)
}

// SetTaskRepositoryToken sets the GitHub repository token for a trusted remote runner task.
// It mutates the original task.
func SetTaskRepositoryToken(ctx context.Context, env environment.Env, task *repb.ExecutionTask, groupID string) error {
	if !(IsRemoteRunnerTask(task)) {
		return nil
	}
	if groupID == "" {
		return status.FailedPreconditionError("missing group ID")
	}

	envOverrides := envOverridesHeader(task)

	// Untrusted workflows won't have repo credentials. Trusted
	// workflows have BUILDBUDDY_API_KEY set by the workflow service.
	apiKey := envOverrides[ci_runner_env.BuildBuddyAPIKeyEnvVarName]
	if apiKey == "" {
		return nil
	}

	repoURL, err := remoteRunBaseRepoURL(task.GetCommand().GetArguments())
	if err != nil {
		return err
	}
	authCtx := env.GetAuthenticator().AuthContextFromAPIKey(ctx, apiKey)
	token, err := githubapp.GetRepositoryInstallationToken(authCtx, env, groupID, repoURL)
	if err != nil {
		return status.WrapError(err, "failed to refresh remote runner git token")
	}
	envOverrides["REPO_TOKEN"] = token
	applyEnvOverrides(task, envOverrides)
	return nil
}

func envOverridesHeader(task *repb.ExecutionTask) map[string]string {
	if task.GetPlatformOverrides() == nil {
		return nil
	}
	overrides := make(map[string]string)
	for _, prop := range task.GetPlatformOverrides().GetProperties() {
		if strings.EqualFold(prop.GetName(), platform.EnvOverridesPropertyName) || strings.EqualFold(prop.GetName(), platform.SecretEnvOverridesPropertyName) {
			for override := range strings.SplitSeq(prop.GetValue(), ",") {
				k, v, ok := strings.Cut(strings.TrimSpace(override), "=")
				if ok {
					overrides[k] = v
				}
			}
		}
	}
	return overrides
}

func remoteRunBaseRepoURL(args []string) (string, error) {
	pushedRepoURL, _ := commandArgValue(args, "--pushed_repo_url")
	targetRepoURL, _ := commandArgValue(args, "--target_repo_url")
	if targetRepoURL != "" && targetRepoURL != pushedRepoURL {
		return targetRepoURL, nil
	}
	if pushedRepoURL != "" {
		return pushedRepoURL, nil
	}
	if targetRepoURL != "" {
		return targetRepoURL, nil
	}
	return "", status.FailedPreconditionError("could not parse repo URL from command arguments")
}

func commandArgValue(args []string, name string) (string, bool) {
	for i, arg := range args {
		if arg == name && i+1 < len(args) {
			return args[i+1], true
		}
		prefix := name + "="
		if trimmed, hasPrefix := strings.CutPrefix(arg, prefix); hasPrefix {
			return trimmed, true
		}
	}
	return "", false
}

func applyEnvOverrides(task *repb.ExecutionTask, envOverrides map[string]string) {
	assignments := make([]string, 0, len(envOverrides))
	for k, v := range envOverrides {
		assignments = append(assignments, k+"="+v)
	}
	overridesVal := strings.Join(assignments, ",")

	// Clear any existing env-overrides properties and write the merged
	// result to a single secret-env-overrides property.
	found := false
	filtered := task.PlatformOverrides.Properties[:0]
	for _, prop := range task.PlatformOverrides.Properties {
		name := prop.GetName()
		if strings.EqualFold(name, platform.EnvOverridesPropertyName) {
			// Drop plain env-overrides; we'll write to secret-env-overrides.
			continue
		}
		if strings.EqualFold(name, platform.SecretEnvOverridesPropertyName) {
			prop.Value = overridesVal
			found = true
		}
		filtered = append(filtered, prop)
	}
	if !found {
		filtered = append(filtered, &repb.Platform_Property{
			Name:  platform.SecretEnvOverridesPropertyName,
			Value: overridesVal,
		})
	}
	task.PlatformOverrides.Properties = filtered
}

func IsRemoteRunnerTask(task *repb.ExecutionTask) bool {
	args := task.GetCommand().GetArguments()
	return len(args) > 0 && args[0] == "./"+ExecutableName
}
