package metronome_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/billing/metronome"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testflags "github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
)

func TestIngestEvents(t *testing.T) {
	var gotEvents []metronome.MetronomeEvent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		var batch []metronome.MetronomeEvent
		require.NoError(t, json.NewDecoder(r.Body).Decode(&batch))
		gotEvents = append(gotEvents, batch...)
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	periodStart := time.Date(2026, 5, 15, 12, 35, 0, 0, time.UTC)
	periodEnd := periodStart.Add(metronome.WindowSize)
	events := []metronome.UsageEvent{
		{GroupID: "GR1", PeriodStart: periodStart, PeriodEnd: periodEnd, SKU: sku.BuildEventsBESCount, Count: 1},
		{GroupID: "GR1", PeriodStart: periodStart, PeriodEnd: periodEnd, SKU: sku.RemoteCacheCASDownloadedBytes, Count: 2_000_000_000,
			Labels: map[sku.LabelName]sku.LabelValue{sku.Origin: sku.OriginExternal, sku.Client: sku.ClientBazel}},
	}
	c, err := metronome.NewClient(nil, nil)
	require.NoError(t, err)
	require.NoError(t, c.ReportUsage(t.Context(), events))

	require.Len(t, gotEvents, 2)
	txids := map[string]bool{}
	for _, e := range gotEvents {
		assert.Equal(t, "GR1", e.CustomerID)
		assert.Equal(t, periodStart.Format(time.RFC3339), e.Timestamp)
		assert.Equal(t, e.EventType, e.Properties.SKU)
		assert.Equal(t, 67, len(e.TransactionID)) // "bb:" + 64 hex chars
		txids[e.TransactionID] = true
	}
	assert.Len(t, txids, 2, "transaction IDs should be distinct per (sku, labels)")
	assert.Equal(t, int64(2_000_000_000), gotEvents[1].Properties.Count)
	assert.Equal(t, sku.OriginExternal, gotEvents[1].Properties.Origin)
	assert.Equal(t, sku.ClientBazel, gotEvents[1].Properties.Client)
}

func TestIngestEventsBatching(t *testing.T) {
	var requests atomic.Int32
	var totalEvents atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		var batch []metronome.MetronomeEvent
		require.NoError(t, json.NewDecoder(r.Body).Decode(&batch))
		totalEvents.Add(int32(len(batch)))
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	const n = metronome.MaxEventsPerIngestRequest*2 + 5
	events := make([]metronome.UsageEvent, n)
	for i := range events {
		periodStart := time.Unix(int64(i)*int64(metronome.WindowSize/time.Second), 0).UTC()
		events[i] = metronome.UsageEvent{
			GroupID: "GR1", PeriodStart: periodStart, PeriodEnd: periodStart.Add(metronome.WindowSize),
			SKU: sku.BuildEventsBESCount, Count: 1,
		}
	}
	c, err := metronome.NewClient(nil, nil)
	require.NoError(t, err)
	require.NoError(t, c.ReportUsage(t.Context(), events))
	assert.EqualValues(t, 3, requests.Load())
	assert.EqualValues(t, n, totalEvents.Load())
}

func TestIngestRetriesTransientFailures(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) < 3 {
			http.Error(w, "boom", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	c, err := metronome.NewClient(nil, &retry.Options{
		MaxRetries: 5, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, Multiplier: 1,
	})
	require.NoError(t, err)
	periodStart := time.Date(2026, 5, 15, 12, 35, 0, 0, time.UTC)
	require.NoError(t, c.ReportUsage(t.Context(), []metronome.UsageEvent{{
		GroupID: "GR1", PeriodStart: periodStart, PeriodEnd: periodStart.Add(metronome.WindowSize), SKU: sku.BuildEventsBESCount, Count: 1,
	}}))
	assert.EqualValues(t, 3, attempts.Load())
}

func TestIngestDoesNotRetryClientErrors(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts.Add(1)
		http.Error(w, "bad", http.StatusBadRequest)
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	c, err := metronome.NewClient(nil, &retry.Options{
		MaxRetries: 5, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond, Multiplier: 1,
	})
	require.NoError(t, err)
	periodStart := time.Date(2026, 5, 15, 12, 35, 0, 0, time.UTC)
	err = c.ReportUsage(t.Context(), []metronome.UsageEvent{{
		GroupID: "GR1", PeriodStart: periodStart, PeriodEnd: periodStart.Add(metronome.WindowSize), SKU: sku.BuildEventsBESCount, Count: 1,
	}})
	require.Error(t, err)
	assert.True(t, status.IsInvalidArgumentError(err))
	assert.EqualValues(t, 1, attempts.Load())
}

func TestReportUsageRejectsInvalidPeriods(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	periodStart := time.Date(2026, 5, 15, 12, 35, 0, 0, time.UTC)
	for _, tc := range []struct {
		name        string
		periodStart time.Time
		periodEnd   time.Time
	}{
		{
			name:        "misaligned start",
			periodStart: periodStart.Add(time.Second),
			periodEnd:   periodStart.Add(metronome.WindowSize),
		},
		{
			name:        "misaligned end",
			periodStart: periodStart,
			periodEnd:   periodStart.Add(metronome.WindowSize).Add(time.Second),
		},
		{
			name:        "wrong window length",
			periodStart: periodStart,
			periodEnd:   periodStart.Add(2 * metronome.WindowSize),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := metronome.NewClient(nil, nil)
			require.NoError(t, err)
			err = c.ReportUsage(t.Context(), []metronome.UsageEvent{{
				GroupID: "GR1", PeriodStart: tc.periodStart, PeriodEnd: tc.periodEnd, SKU: sku.BuildEventsBESCount, Count: 1,
			}})
			require.Error(t, err)
			assert.True(t, status.IsInvalidArgumentError(err))
		})
	}
	assert.EqualValues(t, 0, requests.Load())
}

func TestTransactionIDDeterministic(t *testing.T) {
	var gotEvents []metronome.MetronomeEvent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []metronome.MetronomeEvent
		require.NoError(t, json.NewDecoder(r.Body).Decode(&batch))
		gotEvents = append(gotEvents, batch...)
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	periodStart := time.Date(2026, 5, 15, 12, 35, 0, 0, time.UTC)
	periodEnd := periodStart.Add(metronome.WindowSize)

	// Ingest two events with the same period, SKU, and labels, even though the labels are in a different order and the  count is different.
	c, err := metronome.NewClient(nil, nil)
	require.NoError(t, err)
	require.NoError(t, c.ReportUsage(t.Context(), []metronome.UsageEvent{{
		GroupID: "GR1", PeriodStart: periodStart, PeriodEnd: periodEnd, SKU: sku.RemoteCacheCASHits, Count: 1,
		Labels: map[sku.LabelName]sku.LabelValue{sku.Origin: sku.OriginExternal, sku.Client: sku.ClientBazel},
	}}))
	require.NoError(t, c.ReportUsage(t.Context(), []metronome.UsageEvent{{
		GroupID: "GR1", PeriodStart: periodStart, PeriodEnd: periodEnd, SKU: sku.RemoteCacheCASHits, Count: 999,
		Labels: map[sku.LabelName]sku.LabelValue{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginExternal},
	}}))
	require.Len(t, gotEvents, 2)

	// Check that the transaction IDs are the same.
	// Metronome de-dupes duplicate transaction IDs, which it important to prevent double-billing retries.
	assert.Equal(t, gotEvents[0].TransactionID, gotEvents[1].TransactionID, "transaction ID must be independent of count and label-map iteration order")
}

func TestCreateCustomerAndContract(t *testing.T) {
	var contract map[string]any
	conflict := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		switch r.Method + " " + r.URL.Path {
		case "POST /v1/customers":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			assert.Equal(t, "Acme", body["name"])
			assert.Equal(t, []any{"GR1"}, body["ingest_aliases"])
			fmt.Fprint(w, `{"data":{"id":"cust-1"}}`)
		case "POST /v1/contracts/create":
			if conflict {
				http.Error(w, "uniqueness key already used", http.StatusConflict)
				return
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&contract))
			fmt.Fprint(w, `{"data":{"id":"contract-1"}}`)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)
	testflags.Set(t, "billing.metronome.rate_card_alias", "self-serve")
	testflags.Set(t, "billing.metronome.free_credit_cents", int64(12345))
	testflags.Set(t, "billing.metronome.free_credit_product_id", "prod-credit")

	c, err := metronome.NewClient(nil, nil)
	require.NoError(t, err)
	id, err := c.CreateCustomer(t.Context(), "Acme", "GR1")
	require.NoError(t, err)
	assert.Equal(t, "cust-1", id)

	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, c.CreateContract(t.Context(), "cust-1", start, "GR1"))
	assert.Equal(t, "cust-1", contract["customer_id"])
	assert.Equal(t, "self-serve", contract["rate_card_alias"])
	assert.Equal(t, "2026-09-01T00:00:00Z", contract["starting_at"])
	assert.Equal(t, "GR1", contract["uniqueness_key"])
	credit := contract["recurring_credits"].([]any)[0].(map[string]any)
	assert.Equal(t, "prod-credit", credit["product_id"])
	assert.Equal(t, "MONTHLY", credit["recurrence_frequency"])
	assert.Equal(t, "2026-09-01T00:00:00Z", credit["starting_at"])
	assert.Equal(t, map[string]any{"unit_price": float64(12345), "quantity": float64(1)}, credit["access_amount"])

	conflict = true
	require.NoError(t, c.CreateContract(t.Context(), "cust-1", start, "GR1"))
}

func TestEventPropertiesCoverAllLabels(t *testing.T) {
	var gotEvents []metronome.MetronomeEvent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch []metronome.MetronomeEvent
		require.NoError(t, json.NewDecoder(r.Body).Decode(&batch))
		gotEvents = append(gotEvents, batch...)
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	labels := map[sku.LabelName]sku.LabelValue{}
	for _, name := range sku.LabelNames {
		labels[name] = "test-" + name
	}
	periodStart := time.Date(2026, 5, 15, 12, 35, 0, 0, time.UTC)
	c, err := metronome.NewClient(nil, nil)
	require.NoError(t, err)
	require.NoError(t, c.ReportUsage(t.Context(), []metronome.UsageEvent{{
		GroupID: "GR1", PeriodStart: periodStart, PeriodEnd: periodStart.Add(metronome.WindowSize), SKU: sku.RemoteCacheCASHits, Count: 1,
		Labels: labels,
	}}))
	require.Len(t, gotEvents, 1)

	propertiesJSON, err := json.Marshal(gotEvents[0].Properties)
	require.NoError(t, err)
	var properties map[string]any
	require.NoError(t, json.Unmarshal(propertiesJSON, &properties))
	for _, name := range sku.LabelNames {
		assert.Equal(t, "test-"+name, properties[name], "label %q", name)
		delete(properties, name)
	}
	assert.Equal(t, map[string]any{
		"group_id":     "GR1",
		"sku":          string(sku.RemoteCacheCASHits),
		"count":        float64(1),
		"period_start": "2026-05-15T12:35:00Z",
		"period_end":   "2026-05-15T12:36:00Z",
	}, properties, "properties other than labels")
}
