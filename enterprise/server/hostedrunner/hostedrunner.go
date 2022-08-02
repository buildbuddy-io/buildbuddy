package hostedrunner

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/cache_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/events_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"google.golang.org/genproto/googleapis/longrunning"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rnpb "github.com/buildbuddy-io/buildbuddy/proto/runner"
	gstatus "google.golang.org/grpc/status"
)

const (
	runnerPath           = "enterprise/server/cmd/ci_runner/buildbuddy_ci_runner"
	RunnerContainerImage = "docker://gcr.io/flame-public/buildbuddy-ci-runner@sha256:fbff6d1e88e9e1085c7e46bd0c5de4f478e97b630246631e5f9d7c720c968e2e"
)

type runnerService struct {
	env              environment.Env
	runnerBinaryPath string
}

func New(env environment.Env) (*runnerService, error) {
	f, err := env.GetFileResolver().Open(runnerPath)
	if err != nil {
		return nil, status.FailedPreconditionErrorf("could not open runner binary runfile: %s", err)
	}
	defer f.Close()
	return &runnerService{
		env: env,
	}, nil
}

func (r *runnerService) lookupAPIKey(ctx context.Context) (string, error) {
	auth := r.env.GetAuthenticator()
	if auth == nil {
		return "", status.FailedPreconditionError("Auth was not configured but is required")
	}
	u, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return "", err
	}
	q := query_builder.NewQuery(`SELECT * FROM APIKeys`)
	q.AddWhereClause("group_id = ?", u.GetGroupID())
	qStr, qArgs := q.Build()
	k := &tables.APIKey{}
	if err := r.env.GetDBHandle().DB(ctx).Raw(qStr, qArgs...).Take(&k).Error; err != nil {
		return "", err
	}
	return k.Value, nil
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
func (r *runnerService) createAction(ctx context.Context, req *rnpb.RunRequest, invocationID string) (*repb.Digest, error) {
	cache := r.env.GetCache()
	if cache == nil {
		return nil, status.UnavailableError("No cache configured.")
	}
	apiKey, err := r.lookupAPIKey(ctx)
	if err != nil {
		return nil, err
	}
	binaryBlob, err := fs.ReadFile(r.env.GetFileResolver(), runnerPath)
	if err != nil {
		return nil, err
	}
	runnerBinDigest, err := cachetools.UploadBlobToCAS(ctx, cache, req.GetInstanceName(), binaryBlob)
	if err != nil {
		return nil, err
	}
	// Save this to use when constructing the command to run below.
	runnerName := filepath.Base(runnerPath)
	dir := &repb.Directory{
		Files: []*repb.FileNode{{
			Name:         runnerName,
			Digest:       runnerBinDigest,
			IsExecutable: true,
		}},
	}
	inputRootDigest, err := cachetools.UploadProtoToCAS(ctx, cache, req.GetInstanceName(), dir)
	if err != nil {
		return nil, err
	}

	var patchDigests []string
	for _, patch := range req.GetRepoState().GetPatch() {
		patchDigest, err := cachetools.UploadBlobToCAS(ctx, cache, req.GetInstanceName(), patch)
		if err != nil {
			return nil, err
		}
		patchDigests = append(patchDigests, fmt.Sprintf("%s/%d", patchDigest.GetHash(), patchDigest.GetSizeBytes()))
	}

	// Use https for git operations.
	repoURL, err := git.NormalizeRepoURL(req.GetGitRepo().GetRepoUrl())
	if err != nil {
		return nil, err
	}

	args := []string{
		"./" + runnerName,
		"--bes_backend=" + events_api_url.String(),
		"--cache_backend=" + cache_api_url.String(),
		"--bes_results_url=" + build_buddy_url.WithPath("/invocation/").String(),
		"--target_repo_url=" + repoURL.String(),
		"--bazel_sub_command=" + req.GetBazelCommand(),
		"--invocation_id=" + invocationID,
	}
	if strings.HasPrefix(req.GetBazelCommand(), "run ") {
		args = append(args, "--record_run_metadata")
	}
	if req.GetRepoState().GetCommitSha() != "" {
		args = append(args, "--target_commit_sha="+req.GetRepoState().GetCommitSha())
	} else {
		args = append(args, "--target_branch="+req.GetRepoState().GetBranch())
	}
	if req.GetInstanceName() != "" {
		args = append(args, "--remote_instance_name="+req.GetInstanceName())
	}
	for _, patchDigest := range patchDigests {
		args = append(args, "--patch_digest="+patchDigest)
	}

	affinityKey := req.GetSessionAffinityKey()
	if affinityKey == "" {
		affinityKey = repoURL.String()
	}

	// Hosted Bazel shares the same pool with workflows.
	cmd := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "BUILDBUDDY_API_KEY", Value: apiKey},
			{Name: "REPO_USER", Value: req.GetGitRepo().GetUsername()},
			{Name: "REPO_TOKEN", Value: req.GetGitRepo().GetAccessToken()},
			// Run from the scratch disk, since the workspace disk is hot-swapped
			// between runs, which may not be very Bazel-friendly.
			{Name: "WORKDIR_OVERRIDE", Value: "/root/workspace"},
		},
		Arguments: args,
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "Pool", Value: r.env.GetWorkflowService().WorkflowsPoolName()},
				{Name: platform.HostedBazelAffinityKeyPropertyName, Value: affinityKey},
				{Name: "container-image", Value: RunnerContainerImage},
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

	cmdDigest, err := cachetools.UploadProtoToCAS(ctx, cache, req.GetInstanceName(), cmd)
	if err != nil {
		return nil, err
	}
	action := &repb.Action{
		CommandDigest:   cmdDigest,
		InputRootDigest: inputRootDigest,
		DoNotCache:      true,
	}
	actionDigest, err := cachetools.UploadProtoToCAS(ctx, cache, req.GetInstanceName(), action)
	return actionDigest, err
}

// Run creates and dispatches an execution that will call the CI-runner and run
// the (bazel) command specified in RunRequest. It ruturns as soon as an
// invocation has been created by the execution or an error has been
// encountered.
func (r *runnerService) Run(ctx context.Context, req *rnpb.RunRequest) (*rnpb.RunResponse, error) {
	if err := r.checkPreconditions(req); err != nil {
		return nil, err
	}
	ctx, err := prefix.AttachUserPrefixToContext(ctx, r.env)
	if err != nil {
		return nil, err
	}

	guid, err := uuid.NewRandom()
	if err != nil {
		return nil, err
	}
	invocationID := guid.String()
	actionDigest, err := r.createAction(ctx, req, invocationID)
	if err != nil {
		return nil, err
	}
	log.Debugf("Uploaded runner action to cache. Digest: %s/%d", actionDigest.GetHash(), actionDigest.GetSizeBytes())

	execCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{ToolInvocationId: invocationID})
	if err != nil {
		return nil, err
	}
	executionID, err := r.env.GetRemoteExecutionService().Dispatch(execCtx, &repb.ExecuteRequest{
		InstanceName:    req.GetInstanceName(),
		SkipCacheLookup: true,
		ActionDigest:    actionDigest,
	})
	if err != nil {
		return nil, err
	}
	if err := waitUntilInvocationExists(ctx, r.env, executionID, invocationID); err != nil {
		return nil, err
	}

	return &rnpb.RunResponse{InvocationId: invocationID}, nil
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
			break
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
