package mcptools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/cli/login"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// HTTP header used for BuildBuddy API key authentication.
	apiKeyHeader = "x-buildbuddy-api-key"
	// Env var overriding base URL used for /file/download fetches.
	fileDownloadBaseURLEnv = "BUILDBUDDY_FILE_DOWNLOAD_BASE_URL"
	// Default app URL used for /file/download when no override is provided.
	defaultFileDownloadBaseURL = "https://app.buildbuddy.io"
	// Supported scheme name for insecure gRPC targets.
	defaultResourceSchemeGRPC = "grpc"
	// Supported scheme name for TLS gRPC targets.
	defaultResourceSchemeGRPCS = "grpcs"
	// Supported scheme name for insecure HTTP targets.
	defaultResourceSchemeHTTP = "http"
	// Supported scheme name for TLS HTTP targets.
	defaultResourceSchemeHTTPS = "https"
	// Prefix added when parsing bare targets as gRPCS URLs.
	defaultResourceTargetPrefix = "grpcs://"
	// Remote subdomain prefix replaced when deriving app hostnames.
	defaultResourceRemoteSubdomain = "remote."
	// App subdomain prefix used when deriving /file/download hostnames.
	defaultResourceAppSubdomain = "app."
	// Returned when MCP tools are called without a configured API key.
	authRequiredErrorMessage = "BuildBuddy agent tools require auth - run 'bb login'"
)

var (
	invocationPathPattern = regexp.MustCompile(`/invocation/([^/?#]+)`)
)

func (s *Service) Tools() map[string]*Tool {
	return s.tools
}

func (s *Service) EnsureClients() error {
	return s.ensureClients()
}

func (s *Service) EnsureAuthenticated() error {
	_, err := requiredAPIKey()
	return err
}

func (s *Service) Close() error {
	return s.close()
}

func (s *Service) ensureClients() error {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	if s.bbClient != nil && s.bsClient != nil {
		return nil
	}
	// DialSimple returns a pooled ClientConn by default, which improves
	// throughput for highly parallel MCP tool calls.
	conn, err := grpc_client.DialSimple(s.target)
	if err != nil {
		return fmt.Errorf("dial target %q: %w", s.target, err)
	}
	s.conn = conn
	s.bbClient = bbspb.NewBuildBuddyServiceClient(conn)
	s.bsClient = bspb.NewByteStreamClient(conn)
	return nil
}

func (s *Service) close() error {
	s.clientsMu.Lock()
	defer s.clientsMu.Unlock()

	if s.conn == nil {
		return nil
	}
	err := s.conn.Close()
	s.conn = nil
	s.bbClient = nil
	s.bsClient = nil
	return err
}

func (s *Service) fetchInvocation(ctx context.Context, invocationID string) (*inpb.Invocation, error) {
	authCtx, err := s.authenticatedContext(ctx)
	if err != nil {
		return nil, err
	}
	req := &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{
			InvocationId: invocationID,
		},
	}
	rsp, err := s.bbClient.GetInvocation(authCtx, req)
	if err != nil {
		return nil, apiRequestError(fmt.Sprintf("get invocation %q", invocationID), req, err)
	}
	if len(rsp.GetInvocation()) == 0 {
		return nil, fmt.Errorf("invocation %q not found", invocationID)
	}
	return rsp.GetInvocation()[0], nil
}

func (s *Service) authenticatedContext(ctx context.Context) (context.Context, error) {
	key, err := requiredAPIKey()
	if err != nil {
		return nil, err
	}
	return metadata.AppendToOutgoingContext(ctx, apiKeyHeader, key), nil
}

func requiredAPIKey() (string, error) {
	key, err := login.GetAPIKey()
	if err != nil {
		return "", errors.New(authRequiredErrorMessage)
	}
	key = strings.TrimSpace(key)
	if key == "" {
		return "", errors.New(authRequiredErrorMessage)
	}
	return key, nil
}

func apiRequestError(prefix string, request any, err error) error {
	return fmt.Errorf("%s: %w; request=%s", prefix, err, marshalAPIRequest(request))
}

func marshalAPIRequest(request any) string {
	if request == nil {
		return "null"
	}
	if message, ok := request.(proto.Message); ok {
		b, err := protojson.MarshalOptions{
			UseProtoNames: true,
		}.Marshal(message)
		if err == nil {
			return string(b)
		}
		return fmt.Sprintf("<proto marshal error: %s>", err)
	}
	b, err := json.Marshal(request)
	if err != nil {
		return fmt.Sprintf("<json marshal error: %s>", err)
	}
	return string(b)
}

func invocationMetadata(inv *inpb.Invocation) map[string]any {
	tags := make([]string, 0, len(inv.GetTags()))
	for _, t := range inv.GetTags() {
		if name := strings.TrimSpace(t.GetName()); name != "" {
			tags = append(tags, name)
		}
	}

	return map[string]any{
		"invocation_id":                inv.GetInvocationId(),
		"success":                      inv.GetSuccess(),
		"invocation_status":            inv.GetInvocationStatus().String(),
		"run_status":                   inv.GetRunStatus().String(),
		"user":                         inv.GetUser(),
		"host":                         inv.GetHost(),
		"command":                      inv.GetCommand(),
		"repo_url":                     inv.GetRepoUrl(),
		"branch_name":                  inv.GetBranchName(),
		"commit_sha":                   inv.GetCommitSha(),
		"role":                         inv.GetRole(),
		"attempt":                      inv.GetAttempt(),
		"bazel_exit_code":              inv.GetBazelExitCode(),
		"created_at_usec":              inv.GetCreatedAtUsec(),
		"updated_at_usec":              inv.GetUpdatedAtUsec(),
		"duration_usec":                inv.GetDurationUsec(),
		"action_count":                 inv.GetActionCount(),
		"target_configured_count":      inv.GetTargetConfiguredCount(),
		"download_outputs_option":      inv.GetDownloadOutputsOption().String(),
		"upload_local_results_enabled": inv.GetUploadLocalResultsEnabled(),
		"remote_execution_enabled":     inv.GetRemoteExecutionEnabled(),
		"run_id":                       inv.GetRunId(),
		"parent_run_id":                inv.GetParentRunId(),
		"tags":                         tags,
	}
}

func timestampString(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	t := ts.AsTime()
	if t.IsZero() {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func filePath(f *bespb.File) string {
	path := append([]string{}, f.GetPathPrefix()...)
	path = append(path, f.GetName())
	return strings.Join(path, "/")
}

func extractInvocationID(input string) (string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return "", fmt.Errorf("invocation_id is required")
	}
	if matches := invocationPathPattern.FindStringSubmatch(input); len(matches) == 2 {
		return matches[1], nil
	}

	parsedURL, err := url.Parse(input)
	if err == nil && parsedURL.Path != "" {
		if matches := invocationPathPattern.FindStringSubmatch(parsedURL.Path); len(matches) == 2 {
			return matches[1], nil
		}
	}

	if strings.ContainsAny(input, "/?#") {
		return "", fmt.Errorf("could not extract invocation ID from %q", input)
	}
	return input, nil
}

func requiredString(args map[string]any, field string) (string, error) {
	value, ok := args[field]
	if !ok {
		return "", fmt.Errorf("%q is required", field)
	}
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%q must be a string", field)
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return "", fmt.Errorf("%q must be non-empty", field)
	}
	return s, nil
}

func optionalString(args map[string]any, field string) (string, error) {
	value, ok := args[field]
	if !ok || value == nil {
		return "", nil
	}
	s, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%q must be a string", field)
	}
	return strings.TrimSpace(s), nil
}

func optionalBool(args map[string]any, field string, defaultValue bool) (bool, error) {
	value, ok := args[field]
	if !ok || value == nil {
		return defaultValue, nil
	}
	switch typed := value.(type) {
	case bool:
		return typed, nil
	case string:
		parsed, err := strconv.ParseBool(strings.TrimSpace(typed))
		if err != nil {
			return false, fmt.Errorf("parse %q: %w", field, err)
		}
		return parsed, nil
	default:
		return false, fmt.Errorf("%q must be a boolean", field)
	}
}

func optionalPositiveInt(args map[string]any, field string, defaultValue int, maxValue int) (int, error) {
	value, ok := args[field]
	if !ok {
		return defaultValue, nil
	}

	var parsed int
	switch typed := value.(type) {
	case float64:
		if math.Trunc(typed) != typed {
			return 0, fmt.Errorf("%q must be an integer", field)
		}
		parsed = int(typed)
	case string:
		n, err := strconv.Atoi(strings.TrimSpace(typed))
		if err != nil {
			return 0, fmt.Errorf("parse %q: %w", field, err)
		}
		parsed = n
	default:
		return 0, fmt.Errorf("%q must be an integer", field)
	}
	if parsed < 1 || parsed > maxValue {
		return 0, fmt.Errorf("%q must be between 1 and %d", field, maxValue)
	}
	return parsed, nil
}

// inferFileDownloadBaseURLFromTarget derives the BuildBuddy app base URL used
// to call /file/download from a configured gRPC/HTTP target.
//
// The mapping rules are:
//   - Bare hostnames are treated as grpcs:// targets.
//   - grpc:// maps to http://, and grpcs:// maps to https://.
//   - A leading "remote." subdomain is rewritten to "app.".
//   - If parsing fails, https://app.buildbuddy.io is returned.
//
// Examples:
//   - "grpcs://remote.buildbuddy.io" -> "https://app.buildbuddy.io"
//   - "grpc://remote.buildbuddy.dev:8080" -> "http://app.buildbuddy.dev:8080"
//   - "remote.buildbuddy.io" -> "https://app.buildbuddy.io"
//   - "https://app.buildbuddy.io" -> "https://app.buildbuddy.io"
func inferFileDownloadBaseURLFromTarget(target string) string {
	target = strings.TrimSpace(target)
	if target == "" {
		return defaultFileDownloadBaseURL
	}
	if !strings.Contains(target, "://") {
		target = defaultResourceTargetPrefix + target
	}
	parsedTarget, err := url.Parse(target)
	if err != nil || parsedTarget.Host == "" {
		return defaultFileDownloadBaseURL
	}

	scheme := defaultResourceSchemeHTTPS
	switch parsedTarget.Scheme {
	case defaultResourceSchemeGRPC:
		scheme = defaultResourceSchemeHTTP
	case defaultResourceSchemeGRPCS:
		scheme = defaultResourceSchemeHTTPS
	case defaultResourceSchemeHTTP, defaultResourceSchemeHTTPS:
		scheme = parsedTarget.Scheme
	}

	host := parsedTarget.Hostname()
	if after, ok := strings.CutPrefix(host, defaultResourceRemoteSubdomain); ok {
		host = defaultResourceAppSubdomain + after
	}
	port := parsedTarget.Port()
	if port != "" {
		host = host + ":" + port
	}
	if host == "" {
		return defaultFileDownloadBaseURL
	}
	return (&url.URL{Scheme: scheme, Host: host}).String()
}
