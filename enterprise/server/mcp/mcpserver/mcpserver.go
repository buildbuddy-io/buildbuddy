// Package mcpserver provides an MCP server implementation for BuildBuddy.
//
// The MCP server exposes the developer-only APIs from BuildBuddy's public API
// (/api/v1) as MCP tools. The tool schemas are auto-generated from the protobuf
// service definition (see mcptoolgen).
package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/mcp/jsonrpc"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/mcp/mcptools"
	"github.com/buildbuddy-io/buildbuddy/server/capabilities_filter"
	"github.com/buildbuddy-io/buildbuddy/server/http/interceptors"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	_ "embed"
)

var (
	mcpEnabled = flag.Bool("api.enable_mcp", false, "If true, expose the API over MCP at /mcp. Requires 'api.enable_api' to also be set to true.")
)

var (
	//go:embed api_tools_manifest.json
	apiToolsManifestJSON []byte

	// API tool manifest parsed from the embedded JSON.
	apiToolsManifest mcptools.Manifest
	apiToolsByName   map[string]mcptools.Tool
)

const (
	// Path where we serve the MCP API (on the HTTP port).
	mcpPath = "/mcp"

	// Latest protocol version that we support.
	latestProtocolVersion = "2025-11-25"

	// Maximum size of a single MCP JSON-RPC request body.
	maxRequestBodyBytes = 4 * 1024 * 1024 // 4 MiB
)

var supportedProtocolVersions = []string{
	latestProtocolVersion,
	"2025-06-18",
	"2025-03-26",
	"2024-11-05",
}

// init loads the embedded API tool manifest and indexes tools by name.
func init() {
	if err := json.Unmarshal(apiToolsManifestJSON, &apiToolsManifest); err != nil {
		panic("parse mcp api tools manifest: " + err.Error())
	}
	apiToolsByName = make(map[string]mcptools.Tool, len(apiToolsManifest.Tools))
	for _, tool := range apiToolsManifest.Tools {
		apiToolsByName[tool.Name] = tool
	}
}

// Service exposes a BuildBuddy MCP endpoint behind BuildBuddy's standard HTTP
// authentication path.
//
// Example usage:
//  1. A client sends POST /mcp with normal BuildBuddy auth and a JSON-RPC
//     initialize request.
//  2. The server returns the negotiated protocol version plus the tools
//     capability exposed by this endpoint.
//  3. The client sends tools/list and sees any unary public /api/v1 tools the
//     authenticated caller is allowed to access.
//  4. The client sends tools/call for a generated tool such as
//     get_invocation.
//  5. The server invokes the matching ApiService method (using the same auth
//     middleware as the /api/v1 endpoint) and returns the proto response as MCP
//     structured content.
type Service struct {
	authenticator      interfaces.Authenticator
	apiService         interfaces.ApiService
	authInterceptor    func(http.Handler) http.Handler
	allowedAPIRPCNames func(ctx context.Context, groupID string) []string
}

// initializeParams contains the subset of initialize parameters this server
// currently needs to negotiate protocol version support.
type initializeParams struct {
	ProtocolVersion string `json:"protocolVersion"`
}

// toolListResult wraps the tool descriptors returned from tools/list.
type toolListResult struct {
	Tools []tool `json:"tools"`
}

// tool describes a single MCP tool exposed by this server.
type tool struct {
	Name        string           `json:"name"`
	Description string           `json:"description,omitempty"`
	InputSchema any              `json:"inputSchema"`
	Annotations *toolAnnotations `json:"annotations,omitempty"`
}

// toolAnnotations declares optional hints about a tool's behavior.
type toolAnnotations struct {
	ReadOnlyHint bool `json:"readOnlyHint,omitempty"`
}

// toolCallParams identifies the tool being invoked and its JSON arguments.
type toolCallParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// toolCallResult is the successful result payload returned from tools/call.
type toolCallResult struct {
	Content           []toolContent `json:"content"`
	StructuredContent any           `json:"structuredContent,omitempty"`
	IsError           bool          `json:"isError,omitempty"`
}

// toolContent is a single human-readable content item returned by a tool.
type toolContent struct {
	Type string `json:"type"`
	Text string `json:"text"`
}

// Register installs the MCP service in the environment when app-level MCP
// support is enabled.
func Register(env *real_environment.RealEnv) error {
	if !*mcpEnabled {
		return nil
	}
	if env.GetAPIService() == nil {
		return status.FailedPreconditionError("MCP requires the public API service")
	}
	env.SetMCPService(NewService(
		env.GetAuthenticator(),
		env.GetAPIService(),
		func(handler http.Handler) http.Handler {
			return interceptors.WrapAuthenticatedExternalHandler(env, handler)
		},
		func(ctx context.Context, groupID string) []string {
			return capabilities_filter.AllowedRPCs(ctx, env, groupID)
		},
	))
	return nil
}

// NewService constructs a new MCP service.
func NewService(
	authenticator interfaces.Authenticator,
	apiService interfaces.ApiService,
	authInterceptor func(http.Handler) http.Handler,
	allowedAPIRPCNames func(ctx context.Context, groupID string) []string,
) *Service {
	return &Service{
		authenticator:      authenticator,
		apiService:         apiService,
		authInterceptor:    authInterceptor,
		allowedAPIRPCNames: allowedAPIRPCNames,
	}
}

// RegisterHandlers registers the MCP HTTP endpoint on the provided mux.
func (s *Service) RegisterHandlers(mux interfaces.HttpServeMux) {
	mux.Handle(mcpPath, s.authInterceptor(http.HandlerFunc(s.ServeHTTP)))
}

// ServeHTTP handles streamable HTTP MCP requests for the /mcp endpoint.
func (s *Service) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodOptions:
		w.Header().Set("Allow", "POST, OPTIONS")
		w.WriteHeader(http.StatusNoContent)
		return
	case http.MethodPost:
	default:
		w.Header().Set("Allow", "POST, OPTIONS")
		http.Error(w, "MCP requires POST /mcp", http.StatusMethodNotAllowed)
		return
	}

	groupID, err := s.authenticatedGroupID(r.Context())
	if err != nil {
		writeHTTPError(w, err)
		return
	}

	if r.ContentLength > maxRequestBodyBytes {
		http.Error(w, "MCP request body is too large", http.StatusRequestEntityTooLarge)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			http.Error(w, "MCP request body is too large", http.StatusRequestEntityTooLarge)
			return
		}
		writeJSON(w, http.StatusBadRequest, jsonrpc.Failure(nil, jsonrpc.ParseErrorCode, "read request body"))
		return
	}
	defer r.Body.Close()

	response, httpStatus := s.handleMessage(r.Context(), groupID, bytes.TrimSpace(body))
	if response == nil {
		w.WriteHeader(http.StatusAccepted)
		return
	}
	writeJSON(w, httpStatus, response)
}

// authenticatedGroupID resolves the selected group for the already-authenticated
// MCP request context.
func (s *Service) authenticatedGroupID(ctx context.Context) (string, error) {
	user, err := s.authenticator.AuthenticatedUser(ctx)
	if err != nil {
		return "", err
	}
	return user.GetGroupID(), nil
}

// handleMessage routes one JSON-RPC request or batch and returns the HTTP
// response payload plus status code to write back to the caller.
func (s *Service) handleMessage(ctx context.Context, groupID string, body []byte) (any, int) {
	parsedBody, response, httpStatus := jsonrpc.ParseBody(body)
	if response != nil {
		return response, httpStatus
	}
	if parsedBody.IsBatch {
		responses := make([]jsonrpc.Response, 0, len(parsedBody.Requests))
		for _, rawRequest := range parsedBody.Requests {
			response, _ := s.handleSingleMessage(ctx, groupID, rawRequest)
			if response != nil {
				responses = append(responses, *response)
			}
		}
		if len(responses) == 0 {
			// A batch containing only notifications is intentionally response-free.
			return nil, http.StatusAccepted
		}
		return responses, http.StatusOK
	}
	response, httpStatus = s.handleSingleMessage(ctx, groupID, parsedBody.Requests[0])
	if response == nil {
		// Single notifications follow the same convention: no JSON-RPC payload.
		return nil, http.StatusAccepted
	}
	return response, httpStatus
}

// handleSingleMessage validates and dispatches one JSON-RPC request.
func (s *Service) handleSingleMessage(ctx context.Context, groupID string, raw []byte) (*jsonrpc.Response, int) {
	request, response, httpStatus := jsonrpc.ParseRequest(raw)
	if response != nil {
		return response, httpStatus
	}

	switch request.Method {
	case "initialize":
		return s.handleInitialize(request)
	case "notifications/initialized":
		return nil, http.StatusAccepted
	case "ping":
		return jsonrpc.Success(request.ID, map[string]any{}), http.StatusOK
	case "tools/list":
		return jsonrpc.Success(request.ID, toolListResult{Tools: s.toolsForUser(ctx, groupID)}), http.StatusOK
	case "tools/call":
		return s.handleToolCall(ctx, groupID, request)
	default:
		if jsonrpc.IsNotification(request.ID) {
			return nil, http.StatusAccepted
		}
		return jsonrpc.Failure(request.ID, jsonrpc.MethodNotFoundCode, "method not found"), http.StatusOK
	}
}

// handleInitialize negotiates the protocol version and advertises this
// endpoint's MCP capabilities.
func (s *Service) handleInitialize(request *jsonrpc.Request) (*jsonrpc.Response, int) {
	var params initializeParams
	if len(request.Params) > 0 {
		if err := json.Unmarshal(request.Params, &params); err != nil {
			return jsonrpc.Failure(request.ID, jsonrpc.InvalidParamsCode, "invalid initialize params"), http.StatusOK
		}
	}
	protocolVersion, err := negotiateProtocolVersion(params.ProtocolVersion)
	if err != nil {
		return jsonrpc.Failure(request.ID, jsonrpc.InvalidParamsCode, status.Message(err)), http.StatusOK
	}
	return jsonrpc.Success(request.ID, map[string]any{
		"protocolVersion": protocolVersion,
		"capabilities": map[string]any{
			"tools": map[string]any{
				"listChanged": false,
			},
		},
		"serverInfo": map[string]any{
			"name":        "BuildBuddy",
			"description": "MCP server for accessing BuildBuddy invocations",
			"version":     version.Tag(),
			"websiteUrl":  "https://buildbuddy.io",
			// TODO: icons
		},
	}), http.StatusOK
}

// toolsForUser returns the generated API tools the authenticated caller is
// currently allowed to use.
func (s *Service) toolsForUser(ctx context.Context, groupID string) []tool {
	if s.apiService == nil {
		return nil
	}
	allowed := s.allowedRPCs(ctx, groupID)
	tools := make([]tool, 0, len(apiToolsManifest.Tools))
	for _, generatedTool := range apiToolsManifest.Tools {
		if _, ok := allowed[generatedTool.RPCName]; !ok {
			continue
		}
		tools = append(tools, tool{
			Name:        generatedTool.Name,
			Description: generatedTool.Description,
			InputSchema: generatedTool.InputSchema,
			Annotations: &toolAnnotations{
				ReadOnlyHint: generatedTool.ReadOnly,
			},
		})
	}
	return tools
}

// handleToolCall validates a tools/call request and dispatches it to a
// generated public API tool.
func (s *Service) handleToolCall(ctx context.Context, groupID string, request *jsonrpc.Request) (*jsonrpc.Response, int) {
	var params toolCallParams
	if err := json.Unmarshal(request.Params, &params); err != nil {
		return jsonrpc.Failure(request.ID, jsonrpc.InvalidParamsCode, "invalid tool call params"), http.StatusOK
	}
	generatedTool, ok := apiToolsByName[params.Name]
	if !ok {
		return jsonrpc.Failure(request.ID, jsonrpc.InvalidParamsCode, "unknown tool"), http.StatusOK
	}
	if _, ok := s.allowedRPCs(ctx, groupID)[generatedTool.RPCName]; !ok {
		return jsonrpc.Success(request.ID, toolErrorResult(status.PermissionDeniedError("permission denied"))), http.StatusOK
	}
	messageType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(generatedTool.RequestType))
	if err != nil {
		return jsonrpc.Failure(request.ID, jsonrpc.InternalErrorCode, status.Message(status.InternalErrorf("unknown message type %q", generatedTool.RequestType))), http.StatusOK
	}
	requestProto := messageType.New().Interface()
	arguments := bytes.TrimSpace(params.Arguments)
	if len(arguments) == 0 {
		arguments = []byte("{}")
	}
	if err := (protojson.UnmarshalOptions{}).Unmarshal(arguments, requestProto); err != nil {
		return jsonrpc.Failure(request.ID, jsonrpc.InvalidParamsCode, status.Message(status.InvalidArgumentErrorf("invalid tool arguments: %s", err))), http.StatusOK
	}
	responseProto, err := s.invokeAPITool(ctx, generatedTool, requestProto)
	if err != nil {
		log.Warningf("mcp %s: %s", generatedTool.RPCName, err)
		return jsonrpc.Success(request.ID, toolErrorResult(err)), http.StatusOK
	}
	result, err := protoToolResult(responseProto)
	if err != nil {
		return jsonrpc.Failure(request.ID, jsonrpc.InternalErrorCode, status.Message(err)), http.StatusOK
	}
	return jsonrpc.Success(request.ID, result), http.StatusOK
}

// invokeAPITool uses reflection to find and invoke the generated public API
// method that matches the requested MCP tool.
func (s *Service) invokeAPITool(ctx context.Context, generatedTool mcptools.Tool, requestProto proto.Message) (proto.Message, error) {
	if s.apiService == nil {
		return nil, status.FailedPreconditionError("BuildBuddy public API is not enabled")
	}
	method := reflect.ValueOf(s.apiService).MethodByName(generatedTool.RPCName)
	if !method.IsValid() {
		return nil, status.UnimplementedErrorf("API method %s is not available", generatedTool.RPCName)
	}
	results := method.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(requestProto),
	})
	if len(results) != 2 {
		return nil, status.InternalErrorf("unexpected API method signature for %s", generatedTool.RPCName)
	}
	if errValue := results[1]; !errValue.IsNil() {
		return nil, errValue.Interface().(error)
	}
	if results[0].IsNil() {
		return nil, status.InternalErrorf("API method %s returned no response", generatedTool.RPCName)
	}
	responseProto, ok := results[0].Interface().(proto.Message)
	if !ok {
		return nil, status.InternalErrorf("API method %s returned a non-proto response", generatedTool.RPCName)
	}
	return responseProto, nil
}

// allowedRPCs returns the public API RPCs the caller may invoke through MCP.
func (s *Service) allowedRPCs(ctx context.Context, groupID string) map[string]struct{} {
	allowed := make(map[string]struct{})
	for _, rpcName := range s.allowedAPIRPCNames(ctx, groupID) {
		if !mcptools.AllowsRPC(rpcName) {
			continue
		}
		allowed[rpcName] = struct{}{}
	}
	return allowed
}

// negotiateProtocolVersion picks a mutually supported MCP protocol version.
func negotiateProtocolVersion(requested string) (string, error) {
	if requested == "" {
		return latestProtocolVersion, nil
	}
	for _, supported := range supportedProtocolVersions {
		if requested == supported {
			return requested, nil
		}
	}
	return "", status.InvalidArgumentErrorf("unsupported protocol version %q", requested)
}

// protoToolResult renders a proto response as both human-readable text and MCP
// structured content.
func protoToolResult(message proto.Message) (*toolCallResult, error) {
	rawJSON, err := protojson.Marshal(message)
	if err != nil {
		return nil, status.InternalErrorf("marshal API response: %s", err)
	}
	var pretty bytes.Buffer
	if err := json.Indent(&pretty, rawJSON, "", "  "); err != nil {
		pretty.Write(rawJSON)
	}
	return &toolCallResult{
		Content: []toolContent{{
			Type: "text",
			Text: pretty.String(),
		}},
		StructuredContent: json.RawMessage(rawJSON),
	}, nil
}

// toolErrorResult converts an application error into an MCP tool error result.
func toolErrorResult(err error) *toolCallResult {
	return &toolCallResult{
		Content: []toolContent{{
			Type: "text",
			Text: status.Message(err),
		}},
		IsError: true,
	}
}

// writeJSON writes the JSON response body returned from one MCP request.
func writeJSON(w http.ResponseWriter, httpStatus int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		log.Warningf("mcp write json: %s", err)
	}
}

// writeHTTPError maps a BuildBuddy auth or request error onto an HTTP status
// code for the outer /mcp transport.
func writeHTTPError(w http.ResponseWriter, err error) {
	httpStatus := http.StatusInternalServerError
	switch {
	case status.IsUnauthenticatedError(err):
		httpStatus = http.StatusUnauthorized
	case status.IsPermissionDeniedError(err):
		httpStatus = http.StatusForbidden
	case status.IsNotFoundError(err):
		httpStatus = http.StatusNotFound
	case status.IsInvalidArgumentError(err), status.IsFailedPreconditionError(err):
		httpStatus = http.StatusBadRequest
	}
	http.Error(w, status.Message(err), httpStatus)
}
