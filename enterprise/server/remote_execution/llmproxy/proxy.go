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
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/redact"
	"github.com/klauspost/compress/zstd"
)

const (
	anthropicPrefix = "/anthropic"
	openAIPrefix    = "/openai"

	defaultMaxCompressedBodySize   = 16 << 20
	defaultMaxUncompressedBodySize = 64 << 20
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
	GroupID            string
	ExecutionID        string
	RedactionValues    []string
	AnthropicAPIKey    string
	AnthropicAuthToken string
	OpenAIAPIKey       string
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

	upstream, upstreamPath, err := h.resolveUpstream(req, session)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	body, err := h.redactRequestBody(req, session.RedactionValues)
	if err != nil {
		http.Error(w, "invalid proxy request body", http.StatusBadRequest)
		return
	}

	target := *upstream
	target.Path = strings.TrimRight(upstream.Path, "/") + upstreamPath
	target.RawPath = ""
	target.RawQuery = redactQuery(req.URL.Query(), session.RedactionValues).Encode()

	upstreamReq, err := http.NewRequestWithContext(req.Context(), req.Method, target.String(), bytes.NewReader(body))
	if err != nil {
		http.Error(w, "could not construct upstream request", http.StatusInternalServerError)
		return
	}
	copyRequestHeaders(upstreamReq.Header, req.Header, session.RedactionValues)
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

func (h *Handler) redactRequestBody(req *http.Request, values []string) ([]byte, error) {
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
	redactJSONStrings(value, values)
	return json.Marshal(value)
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

func redactJSONStrings(value any, values []string) {
	switch value := value.(type) {
	case map[string]any:
		for key, child := range value {
			if text, ok := child.(string); ok {
				value[key] = redact.RedactTextWithValues(text, values)
				continue
			}
			redactJSONStrings(child, values)
		}
	case []any:
		for i, child := range value {
			if text, ok := child.(string); ok {
				value[i] = redact.RedactTextWithValues(text, values)
				continue
			}
			redactJSONStrings(child, values)
		}
	}
}

func redactQuery(query url.Values, values []string) url.Values {
	redacted := make(url.Values, len(query))
	for key, queryValues := range query {
		redactedKey := redact.RedactTextWithValues(key, values)
		for _, value := range queryValues {
			redacted[redactedKey] = append(redacted[redactedKey], redact.RedactTextWithValues(value, values))
		}
	}
	return redacted
}

func copyRequestHeaders(dst, src http.Header, values []string) {
	for key, headerValues := range src {
		if isHopByHopHeader(key) {
			continue
		}
		for _, value := range headerValues {
			dst.Add(key, redact.RedactTextWithValues(value, values))
		}
	}
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
