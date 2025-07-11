package invocation

import (
	"context"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
)

const (
	prodInvocationPrefix = "buildbuddy:///invocation/"
)

type Handler struct {
	env environment.Env
}

func NewHandler(env environment.Env) (*Handler, error) {
	return &Handler{
		env: env,
	}, nil
}

func (t *Handler) Register(server *mcp.Server) error {
	mcp.AddTool(server, &mcp.Tool{Name: "get_invocation", Description: "Lookup a specific invocation by ID"}, t.GetInvocation)
	mcp.AddTool(server, &mcp.Tool{Name: "search_invocation", Description: "Search for invocations by various attributes"}, t.SearchInvocation)

	server.AddResourceTemplate(
		&mcp.ResourceTemplate{
			Name:        "resources/invocation/logs",
			MIMEType:    "text/template",
			URITemplate: prodInvocationPrefix + "{invocation_id}#logs",
		},
		t.GetInvocationLogs,
	)
	return nil
}

type GetInvocationParams struct {
	InvocationID *string `json:"invocation_id" jsonschema:"A buildbuddy invocation ID"`
}

func (t *Handler) GetInvocation(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[GetInvocationParams]) (*mcp.CallToolResultFor[any], error) {
	if t.env.GetApiClient() == nil {
		return nil, status.InternalError("no registered api client")
	}
	req := &apipb.GetInvocationRequest{}
	if params.Arguments.InvocationID != nil {
		req.Selector = &apipb.InvocationSelector{
			InvocationId: *params.Arguments.InvocationID,
		}
	}
	rsp, err := t.env.GetApiClient().GetInvocation(ctx, req)
	if err != nil {
		return nil, err // TODO(tylerw): handle
	}
	buf, err := protojson.MarshalOptions{Multiline: true}.Marshal(rsp)
	if err != nil {
		return nil, err
	}

	return &mcp.CallToolResultFor[any]{
		Content: []mcp.Content{
			&mcp.TextContent{
				Text: string(buf),
			},
		},
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

	Count         *int    `json:"count"`
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

func (t *Handler) SearchInvocation(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[SearchInvocationParams]) (*mcp.CallToolResultFor[any], error) {
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
		Status: []inspb.OverallStatus{ // Don't search in-progress invocations.
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
	count := valueOrDefault(params.Arguments.Count)
	if count == 0 {
		count = 1
	}
	req := &inpb.SearchInvocationRequest{Query: query, Count: int32(count)}

	if params.Arguments.SortField != nil {
		if sf, ok := inpb.InvocationSort_SortField_value[*params.Arguments.SortField]; ok {
			req.Sort = &inpb.InvocationSort{
				SortField: inpb.InvocationSort_SortField(sf),
				Ascending: valueOrDefault(params.Arguments.SortAscending),
			}
		} else {
			log.Infof("unsupported sort value: %q", *params.Arguments.SortField)
		}
	}
	rsp, err := t.env.GetBuildBuddyClient().SearchInvocation(ctx, req)
	if err != nil {
		return nil, err
	}
	content := make([]mcp.Content, 0, len(rsp.GetInvocation())*2)
	for _, inv := range rsp.GetInvocation() {
		content = append(content, &mcp.TextContent{
			Text: "https://app.buildbuddy.io/invocation/" + inv.GetInvocationId(),
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

func (r *Handler) GetInvocationLogs(ctx context.Context, ss *mcp.ServerSession, params *mcp.ReadResourceParams) (*mcp.ReadResourceResult, error) {
	invocationID := strings.TrimSuffix(strings.TrimPrefix(params.URI, prodInvocationPrefix), "#logs")

	req := &apipb.GetLogRequest{
		Selector: &apipb.LogSelector{
			InvocationId: invocationID,
		},
	}
	rsp, err := r.env.GetApiClient().GetLog(ctx, req)
	if err != nil {
		if status.IsNotFoundError(err) {
			return nil, mcp.ResourceNotFoundError(params.URI)
		}
		return nil, err
	}
	return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{
		&mcp.ResourceContents{URI: params.URI, MIMEType: "text/plain", Blob: []byte(rsp.GetLog().GetContents())},
	}}, nil
}
