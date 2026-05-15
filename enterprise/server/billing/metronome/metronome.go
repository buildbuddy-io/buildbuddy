package metronome

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/http/httpclient"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/region"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
)

var (
	enabled = flag.Bool("billing.metronome.enabled", false, "If true, usage data will be reported to Metronome.")
	apiKey  = flag.String("billing.metronome.api_key", "", "Metronome bearer token used to ingest usage events.", flag.Secret)
	apiURL  = flag.String("billing.metronome.api_url", "https://api.metronome.com", "Metronome API base URL.", flag.Internal)
)

const (
	ingestPath = "/v1/ingest"

	maxEventsPerIngestRequest = 100
)

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

// ClientConfig configures a Client.
type ClientConfig struct {
	// HTTPClient is the HTTP client used to call Metronome. If nil, a default
	// BuildBuddy HTTP client is used.
	HTTPClient *http.Client
}

// Client sends usage events to Metronome.
type Client struct {
	httpClient *http.Client
}

// NewClient returns a Metronome client.
func NewClient(config ClientConfig) *Client {
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = httpclient.New(nil, "metronome")
		httpClient.Timeout = 30 * time.Second
	}
	return &Client{httpClient: httpClient}
}

func (c *Client) ReportUsage(ctx context.Context, groupID string, periodStart time.Time, labels *tables.UsageLabels, counts *tables.UsageCounts) error {
	if !*enabled {
		return nil
	}
	events, err := eventsForUsage(groupID, periodStart, labels, counts)
	if err != nil {
		return err
	}
	for start := 0; start < len(events); start += maxEventsPerIngestRequest {
		end := start + maxEventsPerIngestRequest
		if end > len(events) {
			end = len(events)
		}
		if err := c.ingest(ctx, events[start:end]); err != nil {
			return err
		}
	}
	return nil
}

func eventsForUsage(groupID string, periodStart time.Time, labels *tables.UsageLabels, counts *tables.UsageCounts) ([]usageEvent, error) {
	if strings.TrimSpace(groupID) == "" {
		return nil, status.InvalidArgumentError("group ID is required to report usage")
	}
	if labels == nil {
		labels = &tables.UsageLabels{}
	}
	if counts == nil {
		return nil, nil
	}

	timestamp := periodStart.UTC().Format(time.RFC3339)
	items := usageutil.LabeledSKUCountsFromUsageCounts(usageLabels(labels), counts)
	events := make([]usageEvent, 0, len(items))
	for _, item := range items {
		properties := map[string]string{
			"group_id": groupID,
			"sku":      item.SKU.String(),
			"count":    strconv.FormatInt(item.Count, 10),
			"unit":     item.Unit,
		}
		for key, value := range item.Labels {
			properties[key] = value
		}
		events = append(events, usageEvent{
			TransactionID: transactionID(groupID, periodStart, item),
			CustomerID:    groupID,
			EventType:     item.SKU.String(),
			Timestamp:     timestamp,
			Properties:    properties,
		})
	}
	return events, nil
}

func usageLabels(labels *tables.UsageLabels) map[sku.LabelName]sku.LabelValue {
	m := map[sku.LabelName]sku.LabelValue{
		sku.Origin: sku.OriginExternal,
	}
	if labels.Origin != "" {
		m[sku.Origin] = labels.Origin
	}
	if labels.Client != "" {
		m[sku.Client] = labels.Client
	}
	if labels.Server != "" {
		m[sku.Server] = labels.Server
	}
	return m
}

func transactionID(groupID string, periodStart time.Time, item usageutil.LabeledSKUCount) string {
	var b strings.Builder
	writeField := func(s string) {
		b.WriteString(strconv.Itoa(len(s)))
		b.WriteString(":")
		b.WriteString(s)
	}
	writeField(groupID)
	writeField(region.ConfiguredAppRegion())
	writeField(periodStart.UTC().Format(time.RFC3339))
	writeField(item.SKU.String())

	keys := make([]string, 0, len(item.Labels))
	for key := range item.Labels {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeField(key)
		writeField(item.Labels[key])
	}

	sum := sha256.Sum256([]byte(b.String()))
	return "bb:" + hex.EncodeToString(sum[:])
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
