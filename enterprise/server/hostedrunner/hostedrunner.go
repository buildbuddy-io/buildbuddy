package hostedrunner

import (
	"context"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/cache_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/events_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"golang.org/x/exp/slices"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/types/known/durationpb"

	ci_runner_bundle "github.com/buildbuddy-io/buildbuddy/enterprise/server/cmd/ci_runner/bundle"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	gstatus "google.golang.org/grpc/status"
)

const DefaultRunnerContainerImage = "docker://" + platform.Ubuntu20_04WorkflowsImage

type runnerService struct {
	env              environment.Env
	runnerBinaryPath string
}

func New(env environment.Env) (*runnerService, error) {
	return &runnerService{
		env: env,
	}, nil
}

// checkPreconditions verifies the RunRequest is not missing any required params.
func (r *runnerService) checkPreconditions(req *rnpb.RunRequest) error {
	if req.GetGitRepo().GetRepoUrl() == "" {
		return status.InvalidArgumentError("A repo url is required.")
	}
	if req.GetBazelCommand() == "" {
		return status.InvalidArgumentError("A bazel command is required.")
	}
	if req.GetRepoState().GetCommitSha() == "" && req.GetRepoState().GetBranch() == "" {
		return status.InvalidArgumentError("Either commit_sha or branch must be specified.")
	}
	return nil
}

// createAction creates and uploads an action that will trigger the CI runner
// to checkout the specified repo and execute the specified bazel action,
// uploading any logs to an invcocation page with the specified ID.
// TODO(Maggie): Refactor this function to use rexec.Prepare
func (r *runnerService) createAction(ctx context.Context, req *rnpb.RunRequest, invocationID string) (*repb.Digest, error) {
	cache := r.env.GetCache()
	if cache == nil {
		return nil, status.UnavailableError("No cache configured.")
	}
	runnerBinDigest, err := cachetools.UploadBlobToCAS(ctx, r.env.GetByteStreamClient(), req.GetInstanceName(), repb.DigestFunction_BLAKE3, ci_runner_bundle.CiRunnerBytes)
	if err != nil {
		return nil, status.WrapError(err, "upload runner bin")
	}
	// Save this to use when constructing the command to run below.
	runnerName := filepath.Base(ci_runner_bundle.RunnerName)
	dir := &repb.Directory{
		Files: []*repb.FileNode{{
			Name:         runnerName,
			Digest:       runnerBinDigest,
			IsExecutable: true,
		}},
	}
	inputRootDigest, err := cachetools.UploadProtoToCAS(ctx, cache, req.GetInstanceName(), repb.DigestFunction_BLAKE3, dir)
	if err != nil {
		return nil, status.WrapError(err, "upload input root")
	}

	var patchURIs []string
	for _, patch := range req.GetRepoState().GetPatch() {
		patchDigest, err := cachetools.UploadBlobToCAS(ctx, r.env.GetByteStreamClient(), req.GetInstanceName(), repb.DigestFunction_BLAKE3, patch)
		if err != nil {
			return nil, status.WrapError(err, "upload patch")
		}
		rn := digest.NewResourceName(patchDigest, req.GetInstanceName(), rspb.CacheType_CAS, repb.DigestFunction_BLAKE3)
		uri, err := rn.DownloadString()
		if err != nil {
			return nil, status.WrapError(err, "patch download string")
		}
		patchURIs = append(patchURIs, uri)
	}

	// Use https for git operations.
	repoURL, err := git.NormalizeRepoURL(req.GetGitRepo().GetRepoUrl())
	if err != nil {
		return nil, status.WrapError(err, "normalize git repo")
	}

	// NOTE: Be cautious when adding new flags. See
	// https://github.com/buildbuddy-io/buildbuddy-internal/issues/3101
	args := []string{
		"./" + runnerName,
		"--bes_backend=" + events_api_url.String(),
		"--cache_backend=" + cache_api_url.String(),
		"--bes_results_url=" + build_buddy_url.WithPath("/invocation/").String(),
		"--target_repo_url=" + repoURL.String(),
		"--bazel_sub_command=" + req.GetBazelCommand(),
		"--invocation_id=" + invocationID,
		"--commit_sha=" + req.GetRepoState().GetCommitSha(),
		"--target_branch=" + req.GetRepoState().GetBranch(),
	}
	if !req.GetRunRemotely() && strings.HasPrefix(req.GetBazelCommand(), "run ") {
		args = append(args, "--record_run_metadata")
	}
	if req.GetInstanceName() != "" {
		args = append(args, "--remote_instance_name="+req.GetInstanceName())
	}
	for _, patchURI := range patchURIs {
		args = append(args, "--patch_uri="+patchURI)
	}

	affinityKey := req.GetSessionAffinityKey()
	if affinityKey == "" {
		affinityKey = repoURL.String()
	}

	image := DefaultRunnerContainerImage
	if req.GetContainerImage() != "" {
		image = req.GetContainerImage()
	}

	// Hosted Bazel shares the same pool with workflows.
	cmd := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			// Run from the scratch disk, since the workspace disk is hot-swapped
			// between runs, which may not be very Bazel-friendly.
			{Name: "WORKDIR_OVERRIDE", Value: "/root/workspace"},
			{Name: "GIT_BRANCH", Value: req.GetRepoState().GetBranch()},
		},
		Arguments: args,
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "Pool", Value: r.env.GetWorkflowService().WorkflowsPoolName()},
				{Name: platform.HostedBazelAffinityKeyPropertyName, Value: affinityKey},
				{Name: "container-image", Value: image},
				{Name: "recycle-runner", Value: "true"},
				{Name: "workload-isolation-type", Value: "firecracker"},
				{Name: platform.EstimatedComputeUnitsPropertyName, Value: "2"},
				{Name: platform.EstimatedFreeDiskPropertyName, Value: "20000000000"}, // 20GB
			},
		},
	}

	if req.GetOs() != "" {
		cmd.Platform.Properties = append(cmd.Platform.Properties, &repb.Platform_Property{
			Name:  platform.OperatingSystemPropertyName,
			Value: req.GetOs(),
		})
	}
	if req.GetArch() != "" {
		cmd.Platform.Properties = append(cmd.Platform.Properties, &repb.Platform_Property{
			Name:  platform.CPUArchitecturePropertyName,
			Value: req.GetArch(),
		})
	}

	for k, v := range req.GetEnv() {
		cmd.EnvironmentVariables = append(cmd.EnvironmentVariables, &repb.Command_EnvironmentVariable{
			Name:  k,
			Value: v,
		})
	}

	cmd.Platform.Properties = append(cmd.Platform.Properties, req.GetExecProperties()...)
	cmd.Platform.Properties = normalizePlatform(cmd.Platform.Properties)

	cmdDigest, err := cachetools.UploadProtoToCAS(ctx, cache, req.GetInstanceName(), repb.DigestFunction_BLAKE3, cmd)
	if err != nil {
		return nil, status.WrapError(err, "upload command")
	}
	action := &repb.Action{
		CommandDigest:   cmdDigest,
		InputRootDigest: inputRootDigest,
		DoNotCache:      true,
	}

	if req.GetTimeout() != "" {
		d, err := time.ParseDuration(req.GetTimeout())
		if err != nil {
			return nil, status.WrapError(err, "parse timeout from request")
		}
		action.Timeout = durationpb.New(d)
	}

	actionDigest, err := cachetools.UploadProtoToCAS(ctx, cache, req.GetInstanceName(), repb.DigestFunction_BLAKE3, action)
	if err != nil {
		return nil, status.WrapError(err, "upload action")
	}
	return actionDigest, nil
}

func (r *runnerService) withCredentials(ctx context.Context, req *rnpb.RunRequest) (context.Context, error) {
	u, err := r.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	apiKey, err := r.env.GetAuthDB().GetAPIKeyForInternalUseOnly(ctx, u.GetGroupID())
	if err != nil {
		return nil, err
	}

	// If no access token is provided explicitly, try fetching the token.
	accessToken := req.GetGitRepo().GetAccessToken()
	if accessToken == "" {
		repoURL, err := git.NormalizeRepoURL(req.GetGitRepo().GetRepoUrl())
		if err != nil {
			return nil, status.WrapError(err, "normalize git repo url")
		}

		gitToken, err := r.getGitToken(ctx, repoURL.String())
		if err != nil {
			log.Warningf("Could not fetch git auth token for %s for hosted runner"+
				" (Note: The token is not needed for public repos): %s", repoURL, err)
		}
		accessToken = gitToken
	}

	// Use env override headers for credentials.
	envOverrides := []*repb.Command_EnvironmentVariable{
		{Name: "BUILDBUDDY_API_KEY", Value: apiKey.Value},
		{Name: "REPO_USER", Value: req.GetGitRepo().GetUsername()},
		{Name: "REPO_TOKEN", Value: accessToken},
	}
	ctx = withEnvOverrides(ctx, envOverrides)
	return ctx, nil
}

func (r *runnerService) getGitToken(ctx context.Context, repoURL string) (string, error) {
	app := r.env.GetGitHubApp()
	if app == nil {
		return "", status.UnimplementedError("GitHub App is not configured")
	}

	u, err := r.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return "", err
	}

	gitRepository := &tables.GitRepository{}
	err = r.env.GetDBHandle().NewQuery(ctx, "hosted_runner_get_for_repo").Raw(`
		SELECT *
		FROM "GitRepositories"
		WHERE group_id = ?
		AND repo_url = ?
	`, u.GetGroupID(), repoURL).Take(gitRepository)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return "", status.NotFoundErrorf("workflow not configured for %s", repoURL)
		}
		return "", status.InternalErrorf("failed to look up repo %s: %s", repoURL, err)
	}

	return app.GetRepositoryInstallationToken(ctx, gitRepository)
}

// Run creates and dispatches an execution that will call the CI-runner and run
// the (bazel) command specified in RunRequest. It ruturns as soon as an
// invocation has been created by the execution or an error has been
// encountered.
func (r *runnerService) Run(ctx context.Context, req *rnpb.RunRequest) (*rnpb.RunResponse, error) {
	if err := r.checkPreconditions(req); err != nil {
		return nil, status.WrapError(err, "check preconditions")
	}
	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.env)
	if err != nil {
		return nil, status.WrapError(err, "attach user prefix")
	}

	guid, err := uuid.NewRandom()
	if err != nil {
		return nil, status.WrapError(err, "uuid")
	}
	invocationID := guid.String()
	actionDigest, err := r.createAction(ctx, req, invocationID)
	if err != nil {
		return nil, status.WrapError(err, "create action")
	}
	log.Debugf("Uploaded runner action to cache. Digest: %s/%d", actionDigest.GetHash(), actionDigest.GetSizeBytes())

	execCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{ToolInvocationId: invocationID})
	if err != nil {
		return nil, status.WrapError(err, "add request metadata to ctx")
	}
	execCtx, err = r.withCredentials(execCtx, req)
	if err != nil {
		return nil, status.WrapError(err, "authenticate ctx")
	}

	executionClient := r.env.GetRemoteExecutionClient()
	if executionClient == nil {
		return nil, status.UnimplementedError("Missing remote execution client.")
	}
	opStream, err := executionClient.Execute(execCtx, &repb.ExecuteRequest{
		InstanceName:    req.GetInstanceName(),
		SkipCacheLookup: true,
		ActionDigest:    actionDigest,
		DigestFunction:  repb.DigestFunction_BLAKE3,
	})
	if err != nil {
		return nil, status.WrapError(err, "execute")
	}
	// Even for async requests, we must wait until the first operation has been
	// returned from the stream to guarantee the context isn't canceled too early
	// before the execution has been created
	op, err := opStream.Recv()
	if err != nil {
		return nil, status.WrapError(err, "opstream receive")
	}

	res := &rnpb.RunResponse{InvocationId: invocationID}
	if req.GetAsync() {
		return res, nil
	}

	executionID := op.GetName()
	if err := waitUntilInvocationExists(ctx, r.env, executionID, invocationID); err != nil {
		return nil, status.WrapError(err, "wait invocation exists")
	}

	return res, nil
}

// waitUntilInvocationExists waits until the specified invocationID exists or
// an error is encountered. Borrowed from workflow.go.
func waitUntilInvocationExists(ctx context.Context, env environment.Env, executionID, invocationID string) error {
	executionClient := env.GetRemoteExecutionClient()
	if executionClient == nil {
		return status.UnimplementedError("Missing remote execution client.")
	}
	invocationDB := env.GetInvocationDB()
	if invocationDB == nil {
		return status.UnimplementedError("Missing invocationDB.")
	}

	errCh := make(chan error)
	opCh := make(chan *longrunning.Operation)

	waitStream, err := executionClient.WaitExecution(ctx, &repb.WaitExecutionRequest{
		Name: executionID,
	})
	if err != nil {
		return err
	}

	// Listen on operation stream in the background
	go func() {
		for {
			op, err := waitStream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			opCh <- op
		}
	}()

	executing := true
	stage := repb.ExecutionStage_UNKNOWN
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case <-time.After(1 * time.Second):
			if executing {
				_, err := invocationDB.LookupInvocation(ctx, invocationID)
				if err == nil {
					return nil
				}
			}
		case op := <-opCh:
			stage = operation.ExtractStage(op)
			if stage == repb.ExecutionStage_EXECUTING || stage == repb.ExecutionStage_COMPLETED {
				executing = true
			}
			if stage == repb.ExecutionStage_COMPLETED {
				if execResponse := operation.ExtractExecuteResponse(op); execResponse != nil {
					if gstatus.FromProto(execResponse.Status).Err() != nil {
						return status.InternalErrorf("Failed to create runner invocation (execution ID: %q): %s", executionID, execResponse.GetStatus().GetMessage())
					}
				}
				return nil
			}
		}
	}
}

func withEnvOverrides(ctx context.Context, env []*repb.Command_EnvironmentVariable) context.Context {
	assignments := make([]string, 0, len(env))
	for _, e := range env {
		assignments = append(assignments, e.Name+"="+e.Value)
	}
	return platform.WithRemoteHeaderOverride(
		ctx, platform.EnvOverridesPropertyName, strings.Join(assignments, ","))
}

// normalizePlatform sorts platform properties alphabetically by name.
// If the same name is specified more than once, the last one wins.
func normalizePlatform(props []*repb.Platform_Property) []*repb.Platform_Property {
	sort.Slice(props, func(i, j int) bool {
		if props[i].Name == props[j].Name {
			// If there are multiple entries with the same name, sort them
			// so the later entry in the input slice is first in the sorted list
			// because slices.CompactFunc selects the first entry if there
			// are duplicates
			return j < i
		}
		return props[i].Name < props[j].Name
	})
	props = slices.CompactFunc(props, func(i, j *repb.Platform_Property) bool {
		return i.Name == j.Name
	})
	return props
}
