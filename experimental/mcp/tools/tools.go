package tools

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/shlex"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/psanford/memfs"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
)

const (
	digestFunction       = repb.DigestFunction_BLAKE3
	prodInvocationPrefix = "buildbuddy:///invocation/"
	prodTargetPrefix     = "buildbuddy:///target/"
)

type ToolHandler struct {
	env environment.Env
}

func NewHandler(env environment.Env) (*ToolHandler, error) {
	return &ToolHandler{
		env: env,
	}, nil
}

func (t *ToolHandler) Register(server *mcp.Server) error {
	// TODO(tylerw): as soon as https://github.com/modelcontextprotocol/go-sdk/pull/94 is in
	// consolidate this mess.
	server.AddTools(
		mcp.NewServerTool("echo_string", "echo back any string with emojis around it", t.Echo, mcp.Input(
			mcp.Property("value", mcp.Description("The string to echo back")),
		)),
		mcp.NewServerTool("get_invocation", "lookup a specific invocation by ID", t.GetInvocation),
		mcp.NewServerTool("search_invocation", "search invocations by various attributes", t.SearchInvocation, mcp.Input(
			mcp.Property("user", mcp.Description("The unix-user who performed the build")),
			mcp.Property("host", mcp.Description("The host this build was executed on")),
			mcp.Property("group_id", mcp.Description("The group to search. The user must be a member")),
			mcp.Property("repo_url", mcp.Description("The git repo the build was for")),
			mcp.Property("commit_sha", mcp.Description("The commit sha used for the build")),
			mcp.Property("roles", mcp.Description("The ROLE metadata set on the build. If multiple filters are specified, they are combined with OR")),
			mcp.Property("updated_after", mcp.Description("The timestamp on or after which the build was last updated (inclusive). RFC3339 format.")),
			mcp.Property("updated_before", mcp.Description("The timestamp up to which the build was last updated (exclusive). RFC3339 format.")),
			mcp.Property("branch_name", mcp.Description("The git branch used for the build.")),
			mcp.Property("command", mcp.Description("The bazel command that was used. Ex: 'build', 'test', 'run'.")),
			mcp.Property("minimum_duration", mcp.Description("The minimum invocation duration, in Golang duration string format.")),
			mcp.Property("maximum_duration", mcp.Description("The maximum invocation duration, in Golang duration string format.")),
			mcp.Property("pattern", mcp.Description("The pattern for the targets built (exact match). Ex: '//...'.")),
			mcp.Property("tags", mcp.Description("Plaintext tags for the targets built (exact match). Ex: 'my-cool-tag'")),
			mcp.Property("sort_field", mcp.Description("One of: CREATED_AT_USEC_SORT_FIELD, UPDATED_AT_USEC_SORT_FIELD, DURATION_SORT_FIELD, ACTION_CACHE_HIT_RATIO_SORT_FIELD, CONTENT_ADDRESSABLE_STORE_CACHE_HIT_RATIO_SORT_FIELD, CACHE_DOWNLOADED_SORT_FIELD, CACHE_UPLOADED_SORT_FIELD, CACHE_TRANSFERRED_SORT_FIELD")),
			mcp.Property("sort_ascending", mcp.Description("If true, sort results in ascending order, otherwise they will be sorted in descending order")),
		)),
		mcp.NewServerTool("search_flaky_target", "search for flaky targets by various attributes", t.SearchFlakyTarget, mcp.Input(
			mcp.Property("labels", mcp.Description("If specified, a list of targets for which we should fetch flake data. If this list is empty, this request will instead return a sorted list of the flakiest tests in the last 30 days.")),
			mcp.Property("started_after", mcp.Description("The timestamp on or after which the test was started. RFC3339 format.")),
			mcp.Property("started_before", mcp.Description("The timestamp up to which the test was started (exclusive). RFC3339 format.")),
			mcp.Property("repo", mcp.Description("If specified, stats will be restricted to invocations in this repo.")),
			mcp.Property("branch_name", mcp.Description("If specified, only return runs on this branch")),
		)),
		mcp.NewServerTool("run_command", "Execute a command. Use this when asked to run any commands.", t.Run, mcp.Input(
			mcp.Property("cmd", mcp.Description("the command to run")))),
		mcp.NewServerTool("file_write", "Create a new file with the provided contents in the working direcotry, overwriting the file if it already exists", t.FileWrite, mcp.Input(
			mcp.Property("path", mcp.Description("The path of the file you want to write")),
			mcp.Property("content", mcp.Description("Full text content of the file you want to write.")),
		)),
	)

	return nil
}

type EchoParams struct {
	Value string `json:"value"`
}

func (t *ToolHandler) Echo(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[EchoParams]) (*mcp.CallToolResultFor[any], error) {
	return &mcp.CallToolResultFor[any]{
		Content: []mcp.Content{
			&mcp.TextContent{Text: fmt.Sprintf("🎉🎉🎉 %s 🎉🎉🎉", params.Arguments.Value)},
		},
	}, nil
}

type RunParams struct {
	Cmd string `json:"cmd"`
}

func (t *ToolHandler) Run(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[RunParams]) (*mcp.CallToolResultFor[any], error) {
	log.Printf("Run() params: %+v", params)
	if params.Arguments.Cmd == "" {
		return nil, status.InvalidArgumentError("cmd must be set")
	}
	executionClient := t.env.GetRemoteExecutionClient()
	if executionClient == nil {
		return nil, status.InternalError("no registered execution client")
	}
	bytestreamClient := t.env.GetByteStreamClient()
	if bytestreamClient == nil {
		return nil, status.InternalError("no registered bytestream client")
	}
	instanceName := cc.ID()
	log.Printf("instanceName is: %q", instanceName)

	args, err := shlex.Split(params.Arguments.Cmd)
	if err != nil {
		return nil, status.InternalErrorf("invalid command: %s", err)
	}
	cmd := &repb.Command{
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "MCP", Value: "true"},
			{Name: "PATH", Value: "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
		},
		Arguments: args,
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "Pool", Value: "workflows"},
				{Name: "hosted-bazel-affinity-key", Value: instanceName}, // use sessionID as affinity key.
				{Name: "container-image", Value: "docker://gcr.io/flame-public/rbe-ubuntu24-04@sha256:f7db0d4791247f032fdb4451b7c3ba90e567923a341cc6dc43abfc283436791a"},
				{Name: "recycle-runner", Value: "true"},
				{Name: "preserve-workspace", Value: "true"},
				{Name: "workload-isolation-type", Value: "firecracker"},
				{Name: "retry", Value: "true"},
				{Name: "EstimatedComputeUnits", Value: "1"},
				{Name: "EstimatedFreeDiskBytes", Value: "2000000000"}, // 2GB
			},
		},
	}
	rexec.NormalizeCommand(cmd)
	action := &repb.Action{
		//InputRootDigest: inputRootDigest,
		DoNotCache: true,
		Timeout:    durationpb.New(10 * time.Second),
	}
	rn, err := rexec.Prepare(ctx, t.env, instanceName, digestFunction, action, cmd, "")
	if err != nil {
		log.Errorf("Prepare error: %s", err)
		return nil, status.WrapError(err, "prepare")
	}
	log.Printf("prepared rn: %s", rn)
	stream, err := rexec.Start(ctx, t.env, rn)
	if err != nil {
		log.Errorf("Start error: %s", err)
		return nil, status.WrapError(err, "start")
	}
	rsp, err := rexec.Wait(stream)
	if err != nil {
		log.Errorf("Wait error: %s", err)
		return nil, status.WrapError(err, "wait")
	}
	commandResult, err := rexec.GetResult(ctx, t.env, instanceName, digestFunction, rsp.ExecuteResponse.GetResult())
	if err != nil {
		log.Errorf("GetResult error: %s", err)
		return nil, status.WrapError(err, "get result")
	}
	log.Printf("commandResult: %+v", commandResult)
	return &mcp.CallToolResultFor[any]{
		Content: []mcp.Content{
			&mcp.TextContent{Text: string(commandResult.Stdout)},
		},
	}, nil
}

type GetInvocationParams struct {
	InvocationID *string `json:"invocation_id"`
}

func (t *ToolHandler) GetInvocation(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[GetInvocationParams]) (*mcp.CallToolResultFor[any], error) {
	if t.env.GetApiClient() == nil {
		return nil, status.InternalError("no registered api client")
	}
	log.Printf("params: %+v", params)
	req := &apipb.GetInvocationRequest{}
	if params.Arguments.InvocationID != nil {
		req.Selector = &apipb.InvocationSelector{
			InvocationId: *params.Arguments.InvocationID,
		}
	}
	log.Printf("req: %+v", req)
	rsp, err := t.env.GetApiClient().GetInvocation(ctx, req)
	if err != nil {
		return nil, err // TODO(tylerw): handle
	}
	log.Printf("rsp: %+v", rsp)
	return &mcp.CallToolResultFor[any]{
		Content: []mcp.Content{
			&mcp.EmbeddedResource{
				Resource: &mcp.ResourceContents{
					URI:  prodInvocationPrefix + *params.Arguments.InvocationID + "#details",
					Text: "resources/invocation/details",
				},
			},
			&mcp.EmbeddedResource{
				Resource: &mcp.ResourceContents{
					URI:  prodInvocationPrefix + *params.Arguments.InvocationID + "#logs",
					Text: "resources/invocation/logs",
				},
			},
		},
	}, nil
}

type SearchFlakyTargetParams struct {
	Labels        []string `json:"labels"`
	Repo          *string  `json:"repo"`
	BranchName    *string  `json:"branch_name"`
	StartedAfter  *string  `json:"updated_after"`
	StartedBefore *string  `json:"updated_before"`
}

func (t *ToolHandler) SearchFlakyTarget(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[SearchFlakyTargetParams]) (*mcp.CallToolResultFor[any], error) {
	if t.env.GetBuildBuddyClient() == nil {
		return nil, status.InternalError("no registered api client")
	}

	req := &trpb.GetTargetStatsRequest{
		Labels:     params.Arguments.Labels,
		Repo:       valueOrDefault(params.Arguments.Repo),
		BranchName: valueOrDefault(params.Arguments.BranchName),
	}

	if params.Arguments.StartedAfter != nil {
		if t, err := time.Parse(time.RFC3339, *params.Arguments.StartedAfter); err == nil {
			req.StartedAfter = timestamppb.New(t)
		}
	}
	if params.Arguments.StartedBefore != nil {
		if t, err := time.Parse(time.RFC3339, *params.Arguments.StartedBefore); err == nil {
			req.StartedBefore = timestamppb.New(t)
		}
	}

	log.Printf("GetTargetStats req: %+v", req)
	rsp, err := t.env.GetBuildBuddyClient().GetTargetStats(ctx, req)
	if err != nil {
		return nil, err // TODO(tylerw): handle
	}
	content := make([]mcp.Content, 0, len(rsp.GetStats()))
	for _, aggregateTargetStats := range rsp.GetStats() {
		data := aggregateTargetStats.GetData()

		totalFlakes := data.GetFlakyRuns() + data.GetLikelyFlakyRuns()
		flakePercentage := (float64(totalFlakes) / float64(data.GetTotalRuns())) * 100

		content = append(content, &mcp.TextContent{
			Text: fmt.Sprintf("Target: %q %2f %% flaky", aggregateTargetStats.GetLabel(), flakePercentage),
		})
		content = append(content, &mcp.EmbeddedResource{
			Resource: &mcp.ResourceContents{
				URI:  prodTargetPrefix + aggregateTargetStats.GetLabel() + "#flake_samples",
				Text: "resources/target/flake_samples",
			},
		})
	}
	return &mcp.CallToolResultFor[any]{
		Content: content,
	}, nil
}

type SearchInvocationParams struct {
	User            *string  `json:"user"`
	Host            *string  `json:"host"`
	GroupID         *string  `json:"group_id"`
	RepoURL         *string  `json:"repo_url"`
	CommitSHA       *string  `json:"commit_sha"`
	Roles           []string `json:"roles"`
	UpdatedAfter    *string  `json:"updated_after"`
	UpdatedBefore   *string  `json:"updated_before"`
	BranchName      *string  `json:"branch_name"`
	Command         *string  `json:"command"`
	MinimumDuration *string  `json:"minimum_duration"`
	MaximumDuration *string  `json:"maximum_duration"`
	Pattern         *string  `json:"pattern"`
	Tags            []string `json:"tags"`

	SortField     *string `json:"sort_field"`
	SortAscending *bool   `json:"sort_ascending"`
}

func valueOrDefault[T any](v *T) T {
	var defaultVal T
	if v == nil {
		return defaultVal
	}
	return *v
}

func (t *ToolHandler) SearchInvocation(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[SearchInvocationParams]) (*mcp.CallToolResultFor[any], error) {
	if t.env.GetBuildBuddyClient() == nil {
		return nil, status.InternalError("no registered api client")
	}

	query := &inpb.InvocationQuery{
		User:      valueOrDefault(params.Arguments.User),
		Host:      valueOrDefault(params.Arguments.Host),
		GroupId:   valueOrDefault(params.Arguments.GroupID),
		RepoUrl:   valueOrDefault(params.Arguments.RepoURL),
		CommitSha: valueOrDefault(params.Arguments.CommitSHA),
		Role:      params.Arguments.Roles,
		Status: []inspb.OverallStatus{
			inspb.OverallStatus_SUCCESS,
			inspb.OverallStatus_FAILURE,
		},
		BranchName: valueOrDefault(params.Arguments.BranchName),
		Command:    valueOrDefault(params.Arguments.Command),
		Pattern:    valueOrDefault(params.Arguments.Pattern),
		Tags:       params.Arguments.Tags,
	}

	if params.Arguments.UpdatedAfter != nil {
		if t, err := time.Parse(time.RFC3339, *params.Arguments.UpdatedAfter); err == nil {
			query.UpdatedAfter = timestamppb.New(t)
		}
	}
	if params.Arguments.UpdatedBefore != nil {
		if t, err := time.Parse(time.RFC3339, *params.Arguments.UpdatedBefore); err == nil {
			query.UpdatedBefore = timestamppb.New(t)
		}
	}
	if params.Arguments.MinimumDuration != nil {
		if d, err := time.ParseDuration(*params.Arguments.MinimumDuration); err == nil {
			query.MinimumDuration = durationpb.New(d)
		}
	}
	if params.Arguments.MaximumDuration != nil {
		if d, err := time.ParseDuration(*params.Arguments.MaximumDuration); err == nil {
			query.MaximumDuration = durationpb.New(d)
		}
	}
	req := &inpb.SearchInvocationRequest{Query: query}

	if params.Arguments.SortField != nil {
		if sf, ok := inpb.InvocationSort_SortField_value[*params.Arguments.SortField]; ok {
			req.Sort = &inpb.InvocationSort{
				SortField: inpb.InvocationSort_SortField(sf),
				Ascending: valueOrDefault(params.Arguments.SortAscending),
			}
		}
	}
	log.Printf("SearchInvocation req: %+v", req)
	rsp, err := t.env.GetBuildBuddyClient().SearchInvocation(ctx, req)
	if err != nil {
		return nil, err // TODO(tylerw): handle
	}
	content := make([]mcp.Content, 0, len(rsp.GetInvocation())*2)
	for _, inv := range rsp.GetInvocation() {
		content = append(content, &mcp.EmbeddedResource{
			Resource: &mcp.ResourceContents{
				URI:  prodInvocationPrefix + inv.GetInvocationId() + "#details",
				Text: "resources/invocation/details",
			},
		})

		content = append(content, &mcp.EmbeddedResource{
			Resource: &mcp.ResourceContents{
				URI:  prodInvocationPrefix + inv.GetInvocationId() + "#logs",
				Text: "resources/invocation/logs",
			},
		})
	}
	return &mcp.CallToolResultFor[any]{
		Content: content,
	}, nil
}

type FileWriteParams struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

func (t *ToolHandler) FileWrite(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[FileWriteParams]) (*mcp.CallToolResultFor[any], error) {
	if params.Arguments.Path == "" {
		return nil, status.InvalidArgumentError("A path is required")
	}
	sessionID := cc.ID()
	log.Printf("sessionID: %q", sessionID)
	fp := strings.TrimPrefix(params.Arguments.Path, "file://")
	fs := NewSerializableFS()
	if err := fs.MkdirAll(filepath.Dir(fp), 0o777); err != nil {
		return nil, err
	}
	if err := fs.WriteFile(fp, []byte(params.Arguments.Content), 0o777); err != nil {
		return nil, err
	}
	return &mcp.CallToolResultFor[any]{
		Content: []mcp.Content{
			&mcp.TextContent{Text: fmt.Sprintf("Wrote file: %s", params.Arguments.Path)},
		},
	}, nil
}

type SerializableMemFS struct {
	*memfs.FS
}

func NewSerializableFS() *SerializableMemFS {
	return &SerializableMemFS{
		FS: memfs.New(),
	}
}

func (fs *SerializableMemFS) Marshal() ([]byte, error) {
	return nil, status.UnimplementedError("no")
}

func Unmarshal(buf []byte) (*SerializableMemFS, error) {
	return nil, status.UnimplementedError("no")
}
