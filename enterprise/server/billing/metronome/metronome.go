package metronome

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/region"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

var (
	apiKey = flag.String("billing.metronome.api_key", "", "Metronome bearer token used to ingest usage events.", flag.Secret)
	apiURL = flag.String("billing.metronome.api_url", "https://api.metronome.com", "Metronome API base URL.", flag.Internal)
)

const (
	ingestPath                = "/v1/ingest"
	MaxEventsPerIngestRequest = 100

	// Caution: Do not change this value!
	// Each Metronome event should cover a window of this size, aligned to the nearest interval of this duration.
	//
	// We include the period start and end timestamps in the transaction ID for deduplication.
	// If this window size is changed, events will not be deduplicated correctly, and users may be over-billed.
	WindowSize = 1 * time.Minute
)

func defaultRetryOptions() *retry.Options {
	return &retry.Options{
		MaxRetries:     5,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		Multiplier:     2,
		Name:           "metronome ingest",
	}
}

// UsageEvent represents a usage event expressed in BB vocabulary (SKUs, labels).
// encodeEvent translates it to Metronome's expected format.
type UsageEvent struct {
	GroupID     string
	PeriodStart time.Time
	PeriodEnd   time.Time
	SKU         sku.SKU
	Labels      map[sku.LabelName]sku.LabelValue
	Count       int64
}

// MetronomeEvent is the JSON payload Metronome's /v1/ingest endpoint expects.
type MetronomeEvent struct {
	TransactionID string            `json:"transaction_id"`
	CustomerID    string            `json:"customer_id"`
	EventType     string            `json:"event_type"`
	Timestamp     string            `json:"timestamp"`
	Properties    map[string]string `json:"properties"`
}

type Client struct {
	httpClient   *http.Client
	retryOptions *retry.Options
}

func NewClient(httpClient *http.Client, retryOpts *retry.Options) (*Client, error) {
	if *apiKey == "" {
		return nil, status.FailedPreconditionError("billing.metronome.api_key is required")
	}
	if *apiURL == "" {
		return nil, status.FailedPreconditionError("billing.metronome.api_url is required")
	}
	if httpClient == nil {
		httpClient = httpclient.New(nil, "metronome")
		httpClient.Timeout = 30 * time.Second
	}
	if retryOpts == nil {
		retryOpts = defaultRetryOptions()
	}
	return &Client{
		httpClient:   httpClient,
		retryOptions: retryOpts,
	}, nil
}

// ReportUsage posts usage events to Metronome.
//
// Events are idempotent: the deterministic transaction_id means re-ingesting
// the same event is a no-op on Metronome's side.
func (c *Client) ReportUsage(ctx context.Context, events []UsageEvent) error {
	if len(events) == 0 {
		return nil
	}
	encoded := make([]MetronomeEvent, 0, len(events))
	for _, e := range events {
		ue, err := encodeEvent(e)
		if err != nil {
			return err
		}
		encoded = append(encoded, *ue)
	}
	for start := 0; start < len(encoded); start += MaxEventsPerIngestRequest {
		end := start + MaxEventsPerIngestRequest
		if end > len(encoded) {
			end = len(encoded)
		}
		batch := encoded[start:end]
		err := retry.DoVoid(ctx, c.retryOptions, func(ctx context.Context) error {
			err := c.ingestToMetronome(ctx, batch)
			if err == nil {
				return nil
			}
			if !isRetryable(err) {
				return retry.NonRetryableError(err)
			}
			return err
		})
		if err != nil {
			return err
		}
	}
	return nil
}

func encodeEvent(e UsageEvent) (*MetronomeEvent, error) {
	if e.GroupID == "" {
		return nil, status.InvalidArgumentError("group ID is required")
	}
	if !IsWindowAligned(e.PeriodStart) || !IsWindowAligned(e.PeriodEnd) {
		return nil, status.InvalidArgumentErrorf("period start and end [%s, %s] must be aligned to window size %s", e.PeriodStart, e.PeriodEnd, WindowSize)
	}
	if e.PeriodEnd.Sub(e.PeriodStart) != WindowSize {
		return nil, status.InvalidArgumentErrorf("[%s, %s] must be of window size %s", e.PeriodStart, e.PeriodEnd, WindowSize)
	}

	properties := map[string]string{
		"group_id":     e.GroupID,
		"sku":          e.SKU.String(),
		"count":        strconv.FormatInt(e.Count, 10),
		"period_start": e.PeriodStart.UTC().Format(time.RFC3339),
		"period_end":   e.PeriodEnd.UTC().Format(time.RFC3339),
	}
	for k, v := range e.Labels {
		properties[k] = v
	}
	return &MetronomeEvent{
		TransactionID: transactionID(e),
		CustomerID:    e.GroupID,
		EventType:     e.SKU.String(),
		Timestamp:     e.PeriodStart.UTC().Format(time.RFC3339),
		Properties:    properties,
	}, nil
}

// transactionID is a deterministic ID used so Metronome can dedupe
// retries and overlapping export windows.
func transactionID(e UsageEvent) string {
	var b strings.Builder
	writeField := func(s string) {
		b.WriteString(strconv.Itoa(len(s)))
		b.WriteString(":")
		b.WriteString(s)
	}
	writeField(e.GroupID)
	writeField(region.ConfiguredAppRegion())
	writeField(e.PeriodStart.UTC().Format(time.RFC3339))
	writeField(e.PeriodEnd.UTC().Format(time.RFC3339))
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

func isRetryable(err error) bool {
	if err == nil {
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

func (c *Client) ingestToMetronome(ctx context.Context, events []MetronomeEvent) error {
	// The ingest endpoint expects a bare JSON array of events.
	// See: https://docs.metronome.com/api-reference/usage/ingest-events
	body, err := json.Marshal(events)
	if err != nil {
		return err
	}
	req, err := c.newRequest(ctx, http.MethodPost, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return errorForStatusCode(resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return nil
}

func (c *Client) newRequest(ctx context.Context, method string, body io.Reader) (*http.Request, error) {
	endpoint, err := url.JoinPath(*apiURL, ingestPath)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
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
	return status.InternalError(message)
}

func IsWindowAligned(t time.Time) bool {
	return t.Truncate(WindowSize).Equal(t)
}
