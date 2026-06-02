// Package metronome implements a client for ingesting usage events into
// Metronome (https://metronome.com).
//
// Events are identified by a deterministic transaction ID derived from the
// (group, region, period, sku, labels) tuple. Re-ingesting the same event is
// safe: Metronome dedupes server-side by transaction_id.
package metronome

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/rand/v2"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/region"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	enabled = flag.Bool("billing.metronome.enabled", false, "If true, usage data will be reported to Metronome.")
	apiKey  = flag.String("billing.metronome.api_key", "", "Metronome bearer token used to ingest usage events.", flag.Secret)
	apiURL  = flag.String("billing.metronome.api_url", "https://api.metronome.com", "Metronome API base URL.", flag.Internal)
)

const (
	ingestPath                = "/v1/ingest"
	maxEventsPerIngestRequest = 100

	defaultMaxAttempts     = 5
	defaultInitialBackoff  = 1 * time.Second
	defaultMaxBackoff      = 30 * time.Second
	defaultBackoffMultiple = 2.0
)

// Event is a single per-SKU usage event to ingest into Metronome.
type Event struct {
	GroupID     string
	PeriodStart time.Time
	SKU         sku.SKU
	Labels      map[sku.LabelName]sku.LabelValue
	Count       int64
	Unit        string
}

type usageEvent struct {
	TransactionID string            `json:"transaction_id"`
	CustomerID    string            `json:"customer_id"`
	EventType     string            `json:"event_type"`
	Timestamp     string            `json:"timestamp"`
	Properties    map[string]string `json:"properties"`
}

// IsConfigured returns whether Metronome usage reporting is enabled.
func IsConfigured() bool {
	return *enabled
}

// RetryConfig controls retry behavior for ingest requests.
type RetryConfig struct {
	MaxAttempts     int
	InitialBackoff  time.Duration
	MaxBackoff      time.Duration
	BackoffMultiple float64
}

func (r *RetryConfig) withDefaults() RetryConfig {
	out := RetryConfig{
		MaxAttempts:     defaultMaxAttempts,
		InitialBackoff:  defaultInitialBackoff,
		MaxBackoff:      defaultMaxBackoff,
		BackoffMultiple: defaultBackoffMultiple,
	}
	if r == nil {
		return out
	}
	if r.MaxAttempts > 0 {
		out.MaxAttempts = r.MaxAttempts
	}
	if r.InitialBackoff > 0 {
		out.InitialBackoff = r.InitialBackoff
	}
	if r.MaxBackoff > 0 {
		out.MaxBackoff = r.MaxBackoff
	}
	if r.BackoffMultiple > 0 {
		out.BackoffMultiple = r.BackoffMultiple
	}
	return out
}

// ClientConfig configures a Client.
type ClientConfig struct {
	// HTTPClient is the HTTP client used to call Metronome. If nil, a default
	// BuildBuddy HTTP client is used.
	HTTPClient *http.Client
	// Retry overrides retry defaults.
	Retry *RetryConfig
}

// Client sends usage events to Metronome.
type Client struct {
	httpClient *http.Client
	retry      RetryConfig
}

// NewClient returns a Metronome client.
func NewClient(config ClientConfig) *Client {
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = httpclient.New(nil, "metronome")
		httpClient.Timeout = 30 * time.Second
	}
	return &Client{
		httpClient: httpClient,
		retry:      config.Retry.withDefaults(),
	}
}

// IngestEvents posts events to Metronome in batches of maxEventsPerIngestRequest,
// with retries on transient errors (5xx, 429, network failures).
//
// Events are idempotent: the deterministic transaction_id means re-ingesting
// the same event is a no-op on Metronome's side.
func (c *Client) IngestEvents(ctx context.Context, events []Event) error {
	if !*enabled {
		return nil
	}
	if len(events) == 0 {
		return nil
	}
	encoded := make([]usageEvent, 0, len(events))
	for _, e := range events {
		ue, err := encodeEvent(e)
		if err != nil {
			return err
		}
		encoded = append(encoded, ue)
	}
	for start := 0; start < len(encoded); start += maxEventsPerIngestRequest {
		end := start + maxEventsPerIngestRequest
		if end > len(encoded) {
			end = len(encoded)
		}
		if err := c.ingestWithRetry(ctx, encoded[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func encodeEvent(e Event) (usageEvent, error) {
	if strings.TrimSpace(e.GroupID) == "" {
		return usageEvent{}, status.InvalidArgumentError("group ID is required")
	}
	properties := map[string]string{
		"group_id": e.GroupID,
		"sku":      e.SKU.String(),
		"count":    strconv.FormatInt(e.Count, 10),
		"unit":     e.Unit,
	}
	for k, v := range e.Labels {
		properties[k] = v
	}
	return usageEvent{
		TransactionID: transactionID(e),
		CustomerID:    e.GroupID,
		EventType:     e.SKU.String(),
		Timestamp:     e.PeriodStart.UTC().Format(time.RFC3339),
		Properties:    properties,
	}, nil
}

// transactionID is a deterministic SHA-256 derived from (group, region,
// period, sku, sorted labels). Stable across runs, so Metronome can dedupe
// retries and overlapping export windows.
func transactionID(e Event) string {
	var b strings.Builder
	writeField := func(s string) {
		b.WriteString(strconv.Itoa(len(s)))
		b.WriteString(":")
		b.WriteString(s)
	}
	writeField(e.GroupID)
	writeField(region.ConfiguredAppRegion())
	writeField(e.PeriodStart.UTC().Format(time.RFC3339))
	writeField(e.SKU.String())
	keys := make([]string, 0, len(e.Labels))
	for k := range e.Labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		writeField(k)
		writeField(e.Labels[k])
	}
	sum := sha256.Sum256([]byte(b.String()))
	return "bb:" + hex.EncodeToString(sum[:])
}

func (c *Client) ingestWithRetry(ctx context.Context, events []usageEvent) error {
	backoff := c.retry.InitialBackoff
	var lastErr error
	for attempt := 1; attempt <= c.retry.MaxAttempts; attempt++ {
		err := c.ingest(ctx, events)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isRetryable(err) {
			return err
		}
		if attempt == c.retry.MaxAttempts {
			break
		}
		sleep := jittered(backoff)
		log.CtxWarningf(ctx, "Metronome ingest attempt %d/%d failed (retrying in %s): %s", attempt, c.retry.MaxAttempts, sleep, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}
		backoff = time.Duration(float64(backoff) * c.retry.BackoffMultiple)
		if backoff > c.retry.MaxBackoff {
			backoff = c.retry.MaxBackoff
		}
	}
	return status.WrapErrorf(lastErr, "Metronome ingest failed after %d attempts", c.retry.MaxAttempts)
}

// jittered returns d ± up to 25% to avoid thundering-herd on shared outages.
func jittered(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	delta := time.Duration(rand.Int64N(int64(d) / 2))
	return d - d/4 + delta
}

// isRetryable returns true for errors that are likely transient: 5xx
// (Unavailable), 429 (ResourceExhausted), and raw network errors from the HTTP
// client. 4xx errors (bad request, auth, etc.) are not retried.
func isRetryable(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if status.IsInvalidArgumentError(err) ||
		status.IsUnauthenticatedError(err) ||
		status.IsPermissionDeniedError(err) ||
		status.IsNotFoundError(err) ||
		status.IsFailedPreconditionError(err) {
		return false
	}
	return true
}

func (c *Client) ingest(ctx context.Context, events []usageEvent) error {
	// The ingest endpoint expects a bare JSON array of events.
	// See: https://docs.metronome.com/api-reference/usage/ingest-events
	body, err := json.Marshal(events)
	if err != nil {
		return err
	}
	req, err := c.newRequest(ctx, http.MethodPost, ingestPath, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return errorForStatusCode(resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (c *Client) newRequest(ctx context.Context, method, requestPath string, body io.Reader) (*http.Request, error) {
	if !*enabled {
		return nil, status.FailedPreconditionError("billing.metronome.enabled is required")
	}
	if strings.TrimSpace(*apiKey) == "" {
		return nil, status.FailedPreconditionError("billing.metronome.api_key is required")
	}
	baseURL := strings.TrimRight(strings.TrimSpace(*apiURL), "/")
	if baseURL == "" {
		return nil, status.FailedPreconditionError("billing.metronome.api_url is required")
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+requestPath, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+strings.TrimSpace(*apiKey))
	return req, nil
}

func errorForStatusCode(statusCode int, body string) error {
	message := "Metronome API error: status " + strconv.Itoa(statusCode) + ": " + body
	switch statusCode {
	case http.StatusBadRequest:
		return status.InvalidArgumentError(message)
	case http.StatusUnauthorized:
		return status.UnauthenticatedError(message)
	case http.StatusForbidden:
		return status.PermissionDeniedError(message)
	case http.StatusNotFound:
		return status.NotFoundError(message)
	case http.StatusTooManyRequests:
		return status.ResourceExhaustedError(message)
	}
	if statusCode >= http.StatusInternalServerError {
		return status.UnavailableError(message)
	}
	return status.FailedPreconditionError(message)
}
