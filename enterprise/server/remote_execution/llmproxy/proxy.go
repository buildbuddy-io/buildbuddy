package llmproxy

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/klauspost/compress/zstd"
	"google.golang.org/protobuf/types/known/timestamppb"

	aspb "github.com/buildbuddy-io/buildbuddy/proto/agent_security"
)

const (
	anthropicPrefix = "/anthropic"
	openAIPrefix    = "/openai"

	defaultMaxCompressedBodySize   = 16 << 20
	defaultMaxUncompressedBodySize = 64 << 20
	maxRecordedEventsPerExecution  = 1000
)

var hopByHopHeaders = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// Session contains the credentials and redaction values authorized for one
// execution. Implementations must return a session based only on trusted
// executor-side connection identity, never on a group or execution ID supplied
// by the runner.
type Session struct {
	RedactionValues      []string
	NamedRedactionValues []NamedRedactionValue
	AnthropicAPIKey      string
	AnthropicAuthToken   string
	OpenAIAPIKey         string
	EventCollector       *EventCollector
}

type NamedRedactionValue struct {
	Name  string
	Value string
}

type ProtectionLayer string

const (
	AgentContextHook  ProtectionLayer = "agent_context_hook"
	ModelRequestProxy ProtectionLayer = "model_request_proxy"
)

type Provider string

const (
	Claude Provider = "claude"
	Codex  Provider = "codex"
)

type Surface string

const (
	ToolOutput    Surface = "tool_output"
	RequestBody   Surface = "request_body"
	RequestHeader Surface = "request_header"
	RequestQuery  Surface = "request_query"
)

// EventCollector records only event metadata. It intentionally cannot carry
// secret values or request content.
type EventCollector struct {
	mu     sync.Mutex
	events []*aspb.SecretRedactionEvent
}

func NewEventCollector() *EventCollector {
	return &EventCollector{}
}

func (c *EventCollector) Record(secretName string, layer ProtectionLayer, provider Provider, surface Surface, eventTime time.Time) {
	if c == nil || secretName == "" {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.events) >= maxRecordedEventsPerExecution {
		return
	}
	c.events = append(c.events, &aspb.SecretRedactionEvent{
		SecretName:      secretName,
		ProtectionLayer: protectionLayerProto(layer),
		Provider:        providerProto(provider),
		Surface:         surfaceProto(surface),
		EventTime:       timestamppb.New(eventTime),
	})
}

func (c *EventCollector) Events() []*aspb.SecretRedactionEvent {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	events := make([]*aspb.SecretRedactionEvent, 0, len(c.events))
	for _, event := range c.events {
		events = append(events, event.CloneVT())
	}
	return events
}

// SessionResolver maps a proxy connection to its execution-scoped session.
type SessionResolver interface {
	ResolveSession(ctx context.Context, req *http.Request) (*Session, error)
}

type Options struct {
	SessionResolver SessionResolver
	HTTPClient      *http.Client
	AnthropicURL    *url.URL
	OpenAIURL       *url.URL

	MaxCompressedBodySize   int64
	MaxUncompressedBodySize int64
}

type Handler struct {
	sessionResolver         SessionResolver
	httpClient              *http.Client
	anthropicURL            *url.URL
	openAIURL               *url.URL
	maxCompressedBodySize   int64
	maxUncompressedBodySize int64
}

func NewHandler(opts Options) (*Handler, error) {
	if opts.SessionResolver == nil {
		return nil, errors.New("session resolver is required")
	}
	anthropicURL := opts.AnthropicURL
	if anthropicURL == nil {
		anthropicURL = &url.URL{Scheme: "https", Host: "api.anthropic.com"}
	}
	openAIURL := opts.OpenAIURL
	if openAIURL == nil {
		openAIURL = &url.URL{Scheme: "https", Host: "api.openai.com"}
	}
	for name, u := range map[string]*url.URL{
		"Anthropic": anthropicURL,
		"OpenAI":    openAIURL,
	} {
		if u.Scheme != "http" && u.Scheme != "https" {
			return nil, fmt.Errorf("%s upstream must use HTTP or HTTPS", name)
		}
		if u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
			return nil, fmt.Errorf("%s upstream URL is invalid", name)
		}
	}
	client := opts.HTTPClient
	if client == nil {
		client = &http.Client{
			Transport: &http.Transport{
				Proxy:                 http.ProxyFromEnvironment,
				ForceAttemptHTTP2:     true,
				MaxIdleConns:          100,
				IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   10 * time.Second,
				ResponseHeaderTimeout: 60 * time.Second,
			},
		}
	}
	maxCompressedBodySize := opts.MaxCompressedBodySize
	if maxCompressedBodySize == 0 {
		maxCompressedBodySize = defaultMaxCompressedBodySize
	}
	maxUncompressedBodySize := opts.MaxUncompressedBodySize
	if maxUncompressedBodySize == 0 {
		maxUncompressedBodySize = defaultMaxUncompressedBodySize
	}
	if maxCompressedBodySize < 0 || maxUncompressedBodySize < 0 {
		return nil, errors.New("request body limits must be positive")
	}
	return &Handler{
		sessionResolver:         opts.SessionResolver,
		httpClient:              client,
		anthropicURL:            anthropicURL,
		openAIURL:               openAIURL,
		maxCompressedBodySize:   maxCompressedBodySize,
		maxUncompressedBodySize: maxUncompressedBodySize,
	}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	session, err := h.sessionResolver.ResolveSession(req.Context(), req)
	if err != nil || session == nil {
		http.Error(w, "unauthorized proxy request", http.StatusUnauthorized)
		return
	}
	if strings.HasPrefix(req.URL.Path, "/hooks/") {
		h.serveHook(w, req, session)
		return
	}

	upstream, upstreamPath, err := h.resolveUpstream(req, session)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	provider := providerForPath(req.URL.Path)
	body, bodyMatches, err := h.redactRequestBody(req, session)
	if err != nil {
		http.Error(w, "invalid proxy request body", http.StatusBadRequest)
		return
	}
	h.reportMatches(session, ModelRequestProxy, provider, RequestBody, bodyMatches)

	target := *upstream
	target.Path = strings.TrimRight(upstream.Path, "/") + upstreamPath
	target.RawPath = ""
	redactedQuery, queryMatches := redactQueryWithMatches(req.URL.Query(), session)
	target.RawQuery = redactedQuery.Encode()
	h.reportMatches(session, ModelRequestProxy, provider, RequestQuery, queryMatches)

	upstreamReq, err := http.NewRequestWithContext(req.Context(), req.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		http.Error(w, "could not construct upstream request", http.StatusInternalServerError)
		return
	}
	headerMatches := copyRequestHeadersWithMatches(upstreamReq.Header, req.Header, session)
	h.reportMatches(session, ModelRequestProxy, provider, RequestHeader, headerMatches)
	upstreamReq.Header.Del("Authorization")
	upstreamReq.Header.Del("X-Api-Key")
	upstreamReq.Header.Del("Content-Encoding")
	upstreamReq.Header.Del("Content-Length")
	upstreamReq.ContentLength = int64(len(body))

	switch {
	case strings.HasPrefix(req.URL.Path, anthropicPrefix):
		if session.AnthropicAPIKey != "" {
			upstreamReq.Header.Set("X-Api-Key", session.AnthropicAPIKey)
		}
		if session.AnthropicAuthToken != "" {
			upstreamReq.Header.Set("Authorization", "Bearer "+session.AnthropicAuthToken)
		}
	case strings.HasPrefix(req.URL.Path, openAIPrefix):
		upstreamReq.Header.Set("Authorization", "Bearer "+session.OpenAIAPIKey)
	}

	rsp, err := h.httpClient.Do(upstreamReq)
	if err != nil {
		http.Error(w, "provider request failed", http.StatusBadGateway)
		return
	}
	defer rsp.Body.Close()

	copyResponseHeaders(w.Header(), rsp.Header)
	w.WriteHeader(rsp.StatusCode)
	_, _ = io.Copy(w, rsp.Body)
}

func (h *Handler) resolveUpstream(req *http.Request, session *Session) (*url.URL, string, error) {
	path := req.URL.Path
	switch {
	case path == "/anthropic/v1/messages" && req.Method == http.MethodPost:
		if session.AnthropicAPIKey == "" && session.AnthropicAuthToken == "" {
			return nil, "", errors.New("Anthropic credentials are unavailable")
		}
		return h.anthropicURL, strings.TrimPrefix(path, anthropicPrefix), nil
	case path == "/anthropic/v1/messages/count_tokens" && req.Method == http.MethodPost:
		if session.AnthropicAPIKey == "" && session.AnthropicAuthToken == "" {
			return nil, "", errors.New("Anthropic credentials are unavailable")
		}
		return h.anthropicURL, strings.TrimPrefix(path, anthropicPrefix), nil
	case (path == "/anthropic/v1/models" || strings.HasPrefix(path, "/anthropic/v1/models/")) && req.Method == http.MethodGet:
		if session.AnthropicAPIKey == "" && session.AnthropicAuthToken == "" {
			return nil, "", errors.New("Anthropic credentials are unavailable")
		}
		return h.anthropicURL, strings.TrimPrefix(path, anthropicPrefix), nil
	case path == "/openai/v1/responses" && req.Method == http.MethodPost:
		if session.OpenAIAPIKey == "" {
			return nil, "", errors.New("OpenAI credentials are unavailable")
		}
		return h.openAIURL, strings.TrimPrefix(path, openAIPrefix), nil
	case path == "/openai/v1/models" && req.Method == http.MethodGet:
		if session.OpenAIAPIKey == "" {
			return nil, "", errors.New("OpenAI credentials are unavailable")
		}
		return h.openAIURL, strings.TrimPrefix(path, openAIPrefix), nil
	default:
		return nil, "", errors.New("unsupported provider endpoint")
	}
}

func (h *Handler) redactRequestBody(req *http.Request, session *Session) ([]byte, []string, error) {
	value, err := h.decodeJSONRequestBody(req)
	if err != nil {
		return nil, nil, err
	}
	if value == nil {
		return nil, nil, nil
	}
	value, _, matches := redactJSONStringsWithMatches(value, session.RedactionValues, session.NamedRedactionValues)
	body, err := json.Marshal(value)
	return body, matches, err
}

func (h *Handler) decodeJSONRequestBody(req *http.Request) (any, error) {
	if req.Body == nil || req.Body == http.NoBody {
		return nil, nil
	}
	mediaType, _, err := mime.ParseMediaType(req.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		return nil, errors.New("only JSON request bodies are supported")
	}
	compressed, err := readAtMost(req.Body, h.maxCompressedBodySize)
	if err != nil {
		return nil, err
	}
	body, err := decompress(compressed, req.Header.Get("Content-Encoding"), h.maxUncompressedBodySize)
	if err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, errors.New("request body must contain exactly one JSON value")
	}
	return value, nil
}

func (h *Handler) serveHook(w http.ResponseWriter, req *http.Request, session *Session) {
	if req.Method != http.MethodPost {
		http.Error(w, "unsupported hook method", http.StatusMethodNotAllowed)
		return
	}
	value, err := h.decodeJSONRequestBody(req)
	if err != nil {
		http.Error(w, "invalid hook body", http.StatusBadRequest)
		return
	}
	event, ok := value.(map[string]any)
	if !ok {
		http.Error(w, "invalid hook event", http.StatusBadRequest)
		return
	}
	toolResponse, ok := event["tool_response"]
	if !ok {
		http.Error(w, "hook event is missing tool_response", http.StatusBadRequest)
		return
	}
	toolResponse, changed, matches := redactJSONStringsWithMatches(
		toolResponse, session.RedactionValues, session.NamedRedactionValues)
	if !changed {
		writeJSON(w, map[string]any{})
		return
	}

	switch req.URL.Path {
	case "/hooks/claude/post-tool-use":
		h.reportMatches(session, AgentContextHook, Claude, ToolOutput, matches)
		writeJSON(w, map[string]any{
			"hookSpecificOutput": map[string]any{
				"hookEventName":     "PostToolUse",
				"updatedToolOutput": toolResponse,
			},
		})
	case "/hooks/codex/post-tool-use":
		h.reportMatches(session, AgentContextHook, Codex, ToolOutput, matches)
		sanitized, err := json.Marshal(toolResponse)
		if err != nil {
			http.Error(w, "could not encode hook response", http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{
			"decision": "block",
			"reason":   string(sanitized),
		})
	default:
		http.Error(w, "unsupported hook endpoint", http.StatusNotFound)
	}
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		return
	}
}

func readAtMost(r io.Reader, limit int64) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > limit {
		return nil, errors.New("request body exceeds size limit")
	}
	return b, nil
}

func decompress(body []byte, contentEncoding string, limit int64) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(contentEncoding)) {
	case "", "identity":
		if int64(len(body)) > limit {
			return nil, errors.New("request body exceeds size limit")
		}
		return body, nil
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return readAtMost(reader, limit)
	case "zstd":
		reader, err := zstd.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return readAtMost(reader, limit)
	default:
		return nil, errors.New("unsupported content encoding")
	}
}

func redactJSONStrings(value any, values []string) (any, bool) {
	redacted, changed, _ := redactJSONStringsWithMatches(value, values, nil)
	return redacted, changed
}

func redactJSONStringsWithMatches(value any, values []string, namedValues []NamedRedactionValue) (any, bool, []string) {
	changed := false
	matches := make(map[string]struct{})
	switch value := value.(type) {
	case string:
		addStringMatches(matches, value, namedValues)
		redacted := redact.RedactTextWithValues(value, values)
		return redacted, redacted != value, sortedMatchNames(matches)
	case map[string]any:
		for key, child := range value {
			redacted, childChanged, childMatches := redactJSONStringsWithMatches(child, values, namedValues)
			value[key] = redacted
			changed = childChanged || changed
			addMatchNames(matches, childMatches)
		}
	case []any:
		for i, child := range value {
			redacted, childChanged, childMatches := redactJSONStringsWithMatches(child, values, namedValues)
			value[i] = redacted
			changed = childChanged || changed
			addMatchNames(matches, childMatches)
		}
	}
	return value, changed, sortedMatchNames(matches)
}

func redactQuery(query url.Values, values []string) url.Values {
	redacted, _ := redactQueryWithMatches(query, &Session{RedactionValues: values})
	return redacted
}

func redactQueryWithMatches(query url.Values, session *Session) (url.Values, []string) {
	redacted := make(url.Values, len(query))
	matches := make(map[string]struct{})
	for key, queryValues := range query {
		addStringMatches(matches, key, session.NamedRedactionValues)
		redactedKey := redact.RedactTextWithValues(key, session.RedactionValues)
		for _, value := range queryValues {
			addStringMatches(matches, value, session.NamedRedactionValues)
			redacted[redactedKey] = append(redacted[redactedKey], redact.RedactTextWithValues(value, session.RedactionValues))
		}
	}
	return redacted, sortedMatchNames(matches)
}

func copyRequestHeaders(dst, src http.Header, values []string) {
	copyRequestHeadersWithMatches(dst, src, &Session{RedactionValues: values})
}

func copyRequestHeadersWithMatches(dst, src http.Header, session *Session) []string {
	matches := make(map[string]struct{})
	for key, headerValues := range src {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range headerValues {
			addStringMatches(matches, value, session.NamedRedactionValues)
			dst.Add(key, redact.RedactTextWithValues(value, session.RedactionValues))
		}
	}
	return sortedMatchNames(matches)
}

func providerForPath(path string) Provider {
	if strings.HasPrefix(path, anthropicPrefix) {
		return Claude
	}
	return Codex
}

func (h *Handler) reportMatches(session *Session, layer ProtectionLayer, provider Provider, surface Surface, names []string) {
	if session.EventCollector == nil || len(names) == 0 {
		return
	}
	now := time.Now()
	for _, name := range names {
		session.EventCollector.Record(name, layer, provider, surface, now)
	}
}

func protectionLayerProto(layer ProtectionLayer) aspb.ProtectionLayer {
	switch layer {
	case AgentContextHook:
		return aspb.ProtectionLayer_AGENT_CONTEXT_HOOK
	case ModelRequestProxy:
		return aspb.ProtectionLayer_MODEL_REQUEST_PROXY
	default:
		return aspb.ProtectionLayer_PROTECTION_LAYER_UNKNOWN
	}
}

func providerProto(provider Provider) aspb.AgentProvider {
	switch provider {
	case Claude:
		return aspb.AgentProvider_CLAUDE
	case Codex:
		return aspb.AgentProvider_CODEX
	default:
		return aspb.AgentProvider_AGENT_PROVIDER_UNKNOWN
	}
}

func surfaceProto(surface Surface) aspb.RedactionSurface {
	switch surface {
	case ToolOutput:
		return aspb.RedactionSurface_TOOL_OUTPUT
	case RequestBody:
		return aspb.RedactionSurface_REQUEST_BODY
	case RequestHeader:
		return aspb.RedactionSurface_REQUEST_HEADER
	case RequestQuery:
		return aspb.RedactionSurface_REQUEST_QUERY
	default:
		return aspb.RedactionSurface_REDACTION_SURFACE_UNKNOWN
	}
}

func addStringMatches(matches map[string]struct{}, value string, namedValues []NamedRedactionValue) {
	for _, secret := range namedValues {
		if secret.Name != "" && secret.Value != "" && strings.Contains(value, secret.Value) {
			matches[secret.Name] = struct{}{}
		}
	}
}

func addMatchNames(matches map[string]struct{}, names []string) {
	for _, name := range names {
		matches[name] = struct{}{}
	}
}

func sortedMatchNames(matches map[string]struct{}) []string {
	names := make([]string, 0, len(matches))
	for name := range matches {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func copyResponseHeaders(dst, src http.Header) {
	for key, values := range src {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range values {
			dst.Add(key, value)
		}
	}
}

func isHopByHopHeader(header string) bool {
	for _, hopByHop := range hopByHopHeaders {
		if strings.EqualFold(header, hopByHop) {
			return true
		}
	}
	return false
}
