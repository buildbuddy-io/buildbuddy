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
	apiKey         = flag.String("billing.metronome.api_key", "", "Metronome API bearer token.", flag.Secret)
	readOnlyAPIKey = flag.String("billing.metronome.read_only_api_key", "", "Metronome API bearer token with read-only access, used by the app to read bills.", flag.Secret)
	apiURL         = flag.String("billing.metronome.api_url", "https://api.metronome.com", "Metronome API base URL.", flag.Internal)
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
	TransactionID string          `json:"transaction_id"`
	CustomerID    string          `json:"customer_id"`
	EventType     string          `json:"event_type"`
	Timestamp     string          `json:"timestamp"`
	Properties    EventProperties `json:"properties"`
}

// EventProperties are the event properties billable metrics filter and
// aggregate on: the usage count plus one field per usage label. Count is a
// number so metrics can sum it and SQL metrics can do math on it.
//
// DEPLOYMENT NOTE: adding a label to sku.go requires a field here. The
// exporter fails on rows with labels it does not know instead of sending
// events that could not be amended later, so deploy the exporter before or
// with the app that writes a new label.
type EventProperties struct {
	GroupID     string `json:"group_id"`
	SKU         string `json:"sku"`
	Count       int64  `json:"count"`
	PeriodStart string `json:"period_start"`
	PeriodEnd   string `json:"period_end"`

	Client        string `json:"client,omitempty"`
	Server        string `json:"server,omitempty"`
	Origin        string `json:"origin,omitempty"`
	Proxy         string `json:"proxy,omitempty"`
	OS            string `json:"os,omitempty"`
	Arch          string `json:"arch,omitempty"`
	SelfHosted    string `json:"self_hosted,omitempty"`
	IsolationType string `json:"isolation_type,omitempty"`
}

type Client struct {
	httpClient   *http.Client
	retryOptions *retry.Options
	apiKey       string
}

func ReadOnlyConfigured() bool {
	return *readOnlyAPIKey != ""
}

func NewClient(httpClient *http.Client, retryOpts *retry.Options) (*Client, error) {
	return newClient(*apiKey, "billing.metronome.api_key", httpClient, retryOpts)
}

func NewReadOnlyClient(httpClient *http.Client, retryOpts *retry.Options) (*Client, error) {
	return newClient(*readOnlyAPIKey, "billing.metronome.read_only_api_key", httpClient, retryOpts)
}

func newClient(key, keyFlag string, httpClient *http.Client, retryOpts *retry.Options) (*Client, error) {
	if key == "" {
		return nil, status.FailedPreconditionErrorf("%s is required", keyFlag)
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
		apiKey:       strings.TrimSpace(key),
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
		end := min(start+MaxEventsPerIngestRequest, len(encoded))
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

	properties := EventProperties{
		GroupID:     e.GroupID,
		SKU:         e.SKU.String(),
		Count:       e.Count,
		PeriodStart: e.PeriodStart.UTC().Format(time.RFC3339),
		PeriodEnd:   e.PeriodEnd.UTC().Format(time.RFC3339),
	}
	for name, value := range e.Labels {
		switch name {
		case sku.Client:
			properties.Client = value
		case sku.Server:
			properties.Server = value
		case sku.Origin:
			properties.Origin = value
		case sku.Proxy:
			properties.Proxy = value
		case sku.OS:
			properties.OS = value
		case sku.Arch:
			properties.Arch = value
		case sku.SelfHosted:
			properties.SelfHosted = value
		case sku.IsolationType:
			properties.IsolationType = value
		default:
			return nil, status.InvalidArgumentErrorf("usage label %q has no event property", name)
		}
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
	return c.do(ctx, http.MethodPost, ingestPath, nil, events, nil)
}

// FindCustomerID returns "" if no customer has the ingest alias.
func (c *Client) FindCustomerID(ctx context.Context, ingestAlias string) (string, error) {
	var resp struct {
		Data []struct {
			ID string `json:"id"`
		} `json:"data"`
	}
	if err := c.do(ctx, http.MethodGet, "/v1/customers", url.Values{"ingest_alias": {ingestAlias}}, nil, &resp); err != nil {
		return "", err
	}
	if len(resp.Data) == 0 {
		return "", nil
	}
	return resp.Data[0].ID, nil
}

// Invoice amounts are in the invoice's credit type, US cents for USD.
type Invoice struct {
	StartTimestamp time.Time         `json:"start_timestamp"`
	EndTimestamp   time.Time         `json:"end_timestamp"`
	Total          float64           `json:"total"`
	LineItems      []InvoiceLineItem `json:"line_items"`
}

type InvoiceLineItem struct {
	Name      string  `json:"name"`
	Quantity  float64 `json:"quantity"`
	UnitPrice float64 `json:"unit_price"`
	Total     float64 `json:"total"`
}

// GetCurrentInvoice returns nil if the customer has no draft usage invoice for
// the period containing now.
func (c *Client) GetCurrentInvoice(ctx context.Context, customerID string, now time.Time) (*Invoice, error) {
	var resp struct {
		Data []Invoice `json:"data"`
	}
	query := url.Values{"status": {"DRAFT"}, "type": {"USAGE"}, "sort": {"date_desc"}}
	if err := c.do(ctx, http.MethodGet, "/v1/customers/"+url.PathEscape(customerID)+"/invoices", query, nil, &resp); err != nil {
		return nil, err
	}
	for i := range resp.Data {
		inv := &resp.Data[i]
		if !now.Before(inv.StartTimestamp) && now.Before(inv.EndTimestamp) {
			return inv, nil
		}
	}
	return nil, nil
}

// do sends a JSON request and decodes the response into out, if non-nil.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body, out any) error {
	endpoint, err := url.JoinPath(*apiURL, path)
	if err != nil {
		return err
	}
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reqBody)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
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
	if out == nil {
		return nil
	}
	return json.Unmarshal(respBody, out)
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
