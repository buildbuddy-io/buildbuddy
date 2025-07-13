package invocation

import (
	"context"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/protobuf/encoding/protojson"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	sfpb "github.com/buildbuddy-io/buildbuddy/proto/stat_filter"
)

const (
	invocationURLPrefix = "https://app.buildbuddy.io/invocation/"
)

type Handler struct {
	env environment.Env
}

func NewHandler(env environment.Env) (*Handler, error) {
	return &Handler{
		env: env,
	}, nil
}

func (h *Handler) Register(server *mcp.Server) error {
	mcp.AddTool(server, &mcp.Tool{Name: "get_invocation", Description: "Get metadata for an invocation ID"}, h.GetInvocation)
	mcp.AddTool(server, &mcp.Tool{Name: "search_invocation", Description: "Search for invocations by various attributes"}, h.SearchInvocation)
	mcp.AddTool(server, &mcp.Tool{Name: "get_invocation_logs", Description: "Get logs for an invocation ID"}, h.GetInvocationLogs)
	return nil
}

type GetInvocationParams struct {
	InvocationID *string `json:"invocation_id" jsonschema:"A buildbuddy invocation URL or bare invocation ID"`
	Deprecated   *string `json:"dummy" jsonschema:"Deprecated."`  // woooo https://github.com/anthropics/claude-code/issues/2089
}

func (h *Handler) GetInvocation(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[GetInvocationParams]) (*mcp.CallToolResultFor[any], error) {
	iid := valueOrDefault(params.Arguments.InvocationID)
	if _, after, found := strings.Cut(iid, "/invocation/"); found {
		iid = after
	}
	if h.env.GetBuildBuddyClient() == nil {
		return nil, status.InternalError("no registered buildbuddy client")
	}
	req := &apipb.GetInvocationRequest{
		Selector: &apipb.InvocationSelector{
			InvocationId: iid,
		},
		IncludeMetadata: true,
		IncludeArtifacts: true,
	}
	log.Debugf("GetInvocation req: %+v", req)
	rsp, err := h.env.GetApiClient().GetInvocation(ctx, req)
	if err != nil {
		return nil, err
	}
	if len(rsp.GetInvocation()) > 1 {
		return nil, status.FailedPreconditionError("Found more than one invocation. Bailing!")
	}
	inv := rsp.GetInvocation()[0]

	buf, err := protojson.MarshalOptions{Multiline: true}.Marshal(inv)
	if err != nil {
		return nil, err
	}
	return &mcp.CallToolResultFor[any]{
		Content: []mcp.Content{
			&mcp.TextContent{
				Text: string(buf),  // TODO(tylerw): is this the best way to return structured data? I doubt it.
			},
		},
	}, nil
}

type FilterValue struct {
	String []string `json:"string" jsonschema:"The value or values (if they are strings)"`
	Int    []int64  `json:"int" jsonschema:"The value or values (if they are ints)"`
	Status []string `json:"status" jsonschema:"The value or values (if they are statuses). Allowed values are: SUCCESS, FAILURE, IN_PROGRESS, DISCONNECTED."`
}

func (fv FilterValue) Proto() *sfpb.FilterValue {
	pv := &sfpb.FilterValue{
		StringValue: fv.String,
		IntValue:    fv.Int,
	}
	for _, status := range fv.Status {
		if ps, ok := inspb.OverallStatus_value[status]; ok {
			pv.StatusValue = append(pv.StatusValue, inspb.OverallStatus(ps))
		}
	}
	return pv
}

type GenericFilter struct {
	Type    *string      `json:"type" jsonschema:"Required. The filter type. Allowed values are: TEXT_MATCH_FILTER_TYPE, REPO_URL_FILTER_TYPE, USER_FILTER_TYPE, INVOCATION_DURATION_USEC_FILTER_TYPE, PATTERN_FILTER_TYPE, COMMAND_FILTER_TYPE, HOST_FILTER_TYPE, COMMIT_SHA_FILTER_TYPE, BRANCH_FILTER_TYPE, WORKER_FILTER_TYPE, ROLE_FILTER_TYPE, INVOCATION_UPDATED_AT_USEC_FILTER_TYPE, INVOCATION_STATUS_FILTER_TYPE, TAG_FILTER_TYPE, INVOCATION_CAS_CACHE_MISSES_FILTER_TYPE, INVOCATION_ACTION_CACHE_MISSES_FILTER_TYPE, INVOCATION_CAS_CACHE_DOWNLOAD_BYTES_FILTER_TYPE, INVOCATION_CAS_CACHE_DOWNLOAD_BPS_FILTER_TYPE, INVOCATION_CAS_CACHE_UPLOAD_BYTES_FILTER_TYPE, INVOCATION_CAS_CACHE_UPLOAD_BPS_FILTER_TYPE, INVOCATION_TIME_SAVED_USEC_FILTER_TYPE, OUTPUT_UPLOAD_TIME_USEC_FILTER_TYPE, PEAK_MEMORY_BYTES_FILTER_TYPE, INPUT_DOWNLOAD_SIZE_BYTES_FILTER_TYPE, OUTPUT_UPLOAD_SIZE_BYTES_FILTER_TYPE."`
	Operand *string      `json:"operand" jsonschema:"Required. The filter operand. Allowed values are GREATER_THAN_OPERAND, LESS_THAN_OPERAND, IN_OPERAND, TEXT_MATCH_OPERAND, ARRAY_CONTAINS_OPERAND, STRING_CONTAINS_OPERAND."`
	Value   *FilterValue `json:"value" jsonschema:"Required. One-of. The value to filter against."`
	Negate  *bool        `json:"negate" jsonschema:"Optional. If true, negate this filter."`
}

func (gf GenericFilter) Proto() (*sfpb.GenericFilter, error) {
	pv := &sfpb.GenericFilter{
		Negate: valueOrDefault(gf.Negate),
	}
	if pt, ok := sfpb.FilterType_value[valueOrDefault(gf.Type)]; ok {
		pv.Type = sfpb.FilterType(pt)
	}
	if po, ok := sfpb.FilterOperand_value[valueOrDefault(gf.Operand)]; ok {
		pv.Operand = sfpb.FilterOperand(po)
	}
	if gf.Value != nil {
		pv.Value = gf.Value.Proto()
	}
	return pv, nil
}

type InvocationSort struct {
	Field     *string `json:"field" jsonschema:"Required. The field to sort on. Allowed values are: CREATED_AT_USEC_SORT_FIELD, UPDATED_AT_USEC_SORT_FIELD, DURATION_SORT_FIELD, ACTION_CACHE_HIT_RATIO_SORT_FIELD, CONTENT_ADDRESSABLE_STORE_CACHE_HIT_RATIO_SORT_FIELD, CACHE_DOWNLOADED_SORT_FIELD, CACHE_UPLOADED_SORT_FIELD, CACHE_TRANSFERRED_SORT_FIELD."`
	Ascending *bool   `json:"ascending" jsonschema:"Required. The sort direction, ascending if true, descending if false."`
}

type SearchInvocationParams struct {
	Filters []GenericFilter `json:"filters"`
	Sort    *InvocationSort `json:"sort"`

	Count     *int    `json:"count" jsonschema:"The number of results to return. Optional."`
	PageToken *string `json:"page_token" jsonschema:"The next_page_token value returned from a previous request, if any."`
}

func valueOrDefault[T any](v *T) T {
	var defaultVal T
	if v == nil {
		return defaultVal
	}
	return *v
}

func (h *Handler) SearchInvocation(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[SearchInvocationParams]) (*mcp.CallToolResultFor[any], error) {
	if h.env.GetBuildBuddyClient() == nil {
		return nil, status.InternalError("no registered api client")
	}

	query := &inpb.InvocationQuery{}
	for _, filter := range params.Arguments.Filters {
		// HACK: this works around our query filter not respecting only
		// generic filters being set with message: "At least one search
		// atom must be set". TODO(tylerw): fix server validation.
		if pf, err := filter.Proto(); err == nil {
			stringMatchOperand := pf.GetOperand() == sfpb.FilterOperand_IN_OPERAND || pf.GetOperand() == sfpb.FilterOperand_STRING_CONTAINS_OPERAND
			switch {
			case pf.GetType() == sfpb.FilterType_HOST_FILTER_TYPE && stringMatchOperand:
				query.Host = pf.GetValue().GetStringValue()[0]
			case pf.GetType() == sfpb.FilterType_USER_FILTER_TYPE && stringMatchOperand:
				query.User = pf.GetValue().GetStringValue()[0]
			case pf.GetType() == sfpb.FilterType_COMMIT_SHA_FILTER_TYPE && stringMatchOperand:
				query.CommitSha = pf.GetValue().GetStringValue()[0]
			case pf.GetType() == sfpb.FilterType_REPO_URL_FILTER_TYPE && stringMatchOperand:
				query.RepoUrl = pf.GetValue().GetStringValue()[0]
			}
			query.GenericFilters = append(query.GenericFilters, pf)
		} else {
			return nil, err
		}
	}
	count := valueOrDefault(params.Arguments.Count)
	if count == 0 {
		count = 1
	}
	req := &inpb.SearchInvocationRequest{Query: query, Count: int32(count)}

	if params.Arguments.Sort != nil {
		if sf, ok := inpb.InvocationSort_SortField_value[valueOrDefault(params.Arguments.Sort.Field)]; ok {
			req.Sort = &inpb.InvocationSort{
				SortField: inpb.InvocationSort_SortField(sf),
				Ascending: valueOrDefault(params.Arguments.Sort.Ascending),
			}
		} else {
			log.Errorf("unsupported sort value: %q", *params.Arguments.Sort.Field)
		}
	}
	log.Debugf("SearchInvocation req: %+v", req)
	rsp, err := h.env.GetBuildBuddyClient().SearchInvocation(ctx, req)
	if err != nil {
		return nil, err
	}
	content := make([]mcp.Content, 0, len(rsp.GetInvocation())*2)
	for _, inv := range rsp.GetInvocation() {
		content = append(content, &mcp.TextContent{
			Text: invocationURLPrefix + inv.GetInvocationId(),
		})
	}
	return &mcp.CallToolResultFor[any]{
		Content: content,
	}, nil
}

type GetInvocationLogsParams struct {
	InvocationID *string `json:"invocation_id" jsonschema:"A buildbuddy invocation URL or bare invocation ID"`
	PageToken    *string `json:"page_token" jsonschema:"Optional. A page token; return the next page of data and a new token if set"`
}

type GetInvocationLogsResponse struct {
	NextPageToken    string `json:"next_page_token" jsonschema:"The token to use to fetch the next page of data"`
	Buffer           []byte `json:"logs" jsonschema:"One page of logs"`
}

func (h *Handler) GetInvocationLogs(ctx context.Context, cc *mcp.ServerSession, params *mcp.CallToolParamsFor[GetInvocationLogsParams]) (*mcp.CallToolResultFor[GetInvocationLogsResponse], error) {
	iid := valueOrDefault(params.Arguments.InvocationID)
	if _, after, found := strings.Cut(iid, "/invocation/"); found {
		iid = after
	}
	if h.env.GetBuildBuddyClient() == nil {
		return nil, status.InternalError("no registered buildbuddy client")
	}
	req := &elpb.GetEventLogChunkRequest{
		InvocationId: iid,
		ChunkId:      valueOrDefault(params.Arguments.PageToken),
		MinLines:     500,
	}
	log.Debugf("GetEventLogChunk req: %+v", req)
	l, err := h.env.GetBuildBuddyClient().GetEventLogChunk(ctx, req)
	if err != nil {
		return nil, err
	}
	return &mcp.CallToolResultFor[GetInvocationLogsResponse]{
		Content: []mcp.Content{
			&mcp.TextContent{
				Text: string(l.GetBuffer()),
			},
		},
		//		StructuredContent: GetInvocationLogsResponse{
		//			NextPageToken: l.GetNextChunkId(),  // TODO(tylerw): make pagination work?
		//			Buffer: l.GetBuffer(),
		//		},
	}, nil
}
