package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/mcp/jsonrpc"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"

	apipb "github.com/buildbuddy-io/buildbuddy/proto/api/v1"
	cappb "github.com/buildbuddy-io/buildbuddy/proto/capability"
)

type fakeAPIService struct {
	interfaces.ApiService
	getInvocation func(context.Context, *apipb.GetInvocationRequest) (*apipb.GetInvocationResponse, error)
}

func (f *fakeAPIService) GetInvocation(ctx context.Context, req *apipb.GetInvocationRequest) (*apipb.GetInvocationResponse, error) {
	if f.getInvocation != nil {
		return f.getInvocation(ctx, req)
	}
	return nil, status.UnimplementedError("not implemented")
}

func TestMCPToolsList_OnlyGetAPIsAreExposed(t *testing.T) {
	server := newTestServer(t, testServerOptions{
		users: testUsers(
			testUser("developer-key", "GROUP1", []cappb.Capability{cappb.Capability_CACHE_WRITE}),
		),
		apiService: &fakeAPIService{},
		// Intentionally exposing a large set of RPCs here to verify that we
		// only expose a subset from tools/list.
		allowedRPCs: []string{
			"GetInvocation",
			"GetLog",
			"GetAuditLog",
			"GetTarget",
			"GetAction",
			"DeleteFile",
			"ExecuteWorkflow",
			"Run",
			"CreateUserApiKey",
		},
	})

	rsp := callJSONRPC[toolListResponse](t, server.URL, "developer-key", "tools/list", nil)
	require.Nil(t, rsp.Error)

	names := make([]string, 0, len(rsp.Result.Tools))
	for _, tool := range rsp.Result.Tools {
		names = append(names, tool.Name)
	}
	require.Equal(t, []string{
		"get_invocation",
		"get_log",
		"get_target",
		"get_action",
	}, names)

	// Should not be able to call an RPC that isn't exposed as a tool even if
	// it's in the allowedRPCs list.
	errorRsp := callJSONRPC[json.RawMessage](t, server.URL, "developer-key", "tools/call", map[string]any{
		"name": "run",
		"arguments": map[string]any{
			"gitRepo": map[string]any{
				"repoUrl": "https://example.com/repo.git",
			},
		},
	})
	require.NotNil(t, errorRsp.Error)
	require.Equal(t, jsonrpc.InvalidParamsCode, errorRsp.Error.Code)
	require.Equal(t, "unknown tool", errorRsp.Error.Message)
}

func TestMCPToolsList_GeneratesDescriptionsFromProtoComments(t *testing.T) {
	server := newTestServer(t, testServerOptions{
		users: testUsers(
			testUser("developer-key", "GROUP1", []cappb.Capability{cappb.Capability_CACHE_WRITE}),
		),
		apiService:  &fakeAPIService{},
		allowedRPCs: []string{"GetInvocation"},
	})

	rsp := callJSONRPC[toolListResponse](t, server.URL, "developer-key", "tools/list", nil)
	require.Nil(t, rsp.Error)
	require.Len(t, rsp.Result.Tools, 1)

	tool := rsp.Result.Tools[0]
	require.Equal(t, "get_invocation", tool.Name)
	require.Regexp(t, `invocation.*match.*selector`, tool.Description)

	var inputSchema map[string]any
	unmarshalJSON(t, tool.InputSchema, &inputSchema)
	_, hasDescription := inputSchema["description"]
	require.False(t, hasDescription)

	properties, ok := inputSchema["properties"].(map[string]any)
	require.True(t, ok)
	selector, ok := properties["selector"].(map[string]any)
	require.True(t, ok)
	selectorDescription, ok := selector["description"].(string)
	require.True(t, ok)
	require.Regexp(t, `which.*invocations`, selectorDescription)

	selectorProperties, ok := selector["properties"].(map[string]any)
	require.True(t, ok)
	invocationID, ok := selectorProperties["invocationId"].(map[string]any)
	require.True(t, ok)
	require.Contains(t, invocationID["description"], "Invocation ID")
}

func TestMCPToolCall_GetInvocation(t *testing.T) {
	var gotInvocationID string
	server := newTestServer(t, testServerOptions{
		users: testUsers(
			testUser("developer-key", "GROUP1", []cappb.Capability{cappb.Capability_CACHE_WRITE}),
		),
		apiService: &fakeAPIService{
			getInvocation: func(ctx context.Context, req *apipb.GetInvocationRequest) (*apipb.GetInvocationResponse, error) {
				if req.GetSelector() != nil {
					gotInvocationID = req.GetSelector().GetInvocationId()
				}
				return &apipb.GetInvocationResponse{
					Invocation: []*apipb.Invocation{{
						Id:      &apipb.Invocation_Id{InvocationId: "inv-1"},
						User:    "alice@example.com",
						Command: "build",
					}},
				}, nil
			},
		},
		allowedRPCs: []string{"GetInvocation"},
	})

	rsp := callJSONRPC[toolCallResponse](t, server.URL, "developer-key", "tools/call", map[string]any{
		"name": "get_invocation",
		"arguments": map[string]any{
			"selector": map[string]any{
				"invocationId": "inv-1",
			},
		},
	})
	require.Nil(t, rsp.Error)
	require.Equal(t, "inv-1", gotInvocationID)

	require.False(t, rsp.Result.IsError)
	require.NotEmpty(t, rsp.Result.Content)
	require.Contains(t, rsp.Result.Content[0].Text, "inv-1")

	var structured map[string]any
	unmarshalJSON(t, rsp.Result.StructuredContent, &structured)
	invocations, ok := structured["invocation"].([]any)
	require.True(t, ok)
	require.Len(t, invocations, 1)
}

// testUser constructs one authenticated caller identity keyed by its API key.
func testUser(apiKey, groupID string, capabilities []cappb.Capability) interfaces.UserInfo {
	return &testauth.TestUser{
		UserID:        apiKey,
		GroupID:       groupID,
		AllowedGroups: []string{groupID},
		GroupMemberships: []*interfaces.GroupMembership{{
			GroupID:      groupID,
			Capabilities: capabilities,
		}},
		Capabilities: capabilities,
	}
}

func testUsers(users ...interfaces.UserInfo) map[string]interfaces.UserInfo {
	out := make(map[string]interfaces.UserInfo, len(users))
	for _, user := range users {
		out[user.GetUserID()] = user
	}
	return out
}

type testServerOptions struct {
	users       map[string]interfaces.UserInfo
	apiService  interfaces.ApiService
	allowedRPCs []string
}

func newTestServer(t *testing.T, opts testServerOptions) *httptest.Server {
	authenticator := testauth.NewTestAuthenticator(t, opts.users)
	service := NewService(
		authenticator,
		opts.apiService,
		func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := authenticator.AuthenticatedHTTPContext(w, r)
				next.ServeHTTP(w, r.WithContext(ctx))
			})
		},
		func(ctx context.Context, groupID string) []string {
			return opts.allowedRPCs
		},
	)

	mux := http.NewServeMux()
	service.RegisterHandlers(mux)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server
}

type toolListResponse struct {
	Tools []struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		InputSchema json.RawMessage `json:"inputSchema"`
	} `json:"tools"`
}

type toolCallResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	StructuredContent json.RawMessage `json:"structuredContent,omitempty"`
	IsError           bool            `json:"isError,omitempty"`
}

type rpcResponse[T any] struct {
	JSONRPC string         `json:"jsonrpc"`
	ID      int            `json:"id"`
	Result  T              `json:"result,omitempty"`
	Error   *jsonrpc.Error `json:"error,omitempty"`
}

func callJSONRPC[T any](t *testing.T, baseURL, apiKey, method string, params any) *rpcResponse[T] {
	request := jsonrpc.Request{
		JSONRPC: jsonrpc.Version,
		ID:      json.RawMessage("1"),
		Method:  method,
	}
	if params != nil {
		paramsJSON, err := json.Marshal(params)
		require.NoError(t, err)
		request.Params = paramsJSON
	}
	body, err := json.Marshal(request)
	require.NoError(t, err)

	req, err := http.NewRequest(http.MethodPost, baseURL+mcpPath, bytes.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(authutil.APIKeyHeader, apiKey)

	rsp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer rsp.Body.Close()

	var decoded rpcResponse[T]
	require.NoError(t, json.NewDecoder(rsp.Body).Decode(&decoded))
	return &decoded
}

// unmarshalJSON decodes one JSON blob into the provided test struct.
func unmarshalJSON(t *testing.T, data []byte, out any) {
	t.Helper()
	require.NoError(t, json.Unmarshal(data, out))
}
