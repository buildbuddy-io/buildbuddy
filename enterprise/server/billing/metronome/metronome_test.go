package metronome_test

import (
	"encoding/json"
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

	periodStart := time.Date(2026, 5, 15, 12, 34, 0, 0, time.UTC)
	events := []metronome.UsageEvent{
		{GroupID: "GR1", PeriodStart: periodStart, SKU: sku.BuildEventsBESCount, Count: 1, Unit: "count"},
		{GroupID: "GR1", PeriodStart: periodStart, SKU: sku.RemoteCacheCASDownloadedBytes, Count: 2_000_000_000, Unit: "bytes",
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
		assert.Equal(t, e.EventType, e.Properties["sku"])
		assert.Equal(t, 67, len(e.TransactionID)) // "bb:" + 64 hex chars
		txids[e.TransactionID] = true
	}
	assert.Len(t, txids, 2, "transaction IDs should be distinct per (sku, labels)")
}

func TestIngestEventsBatching(t *testing.T) {
	var requests int32
	var totalEvents int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		var batch []metronome.MetronomeEvent
		require.NoError(t, json.NewDecoder(r.Body).Decode(&batch))
		atomic.AddInt32(&totalEvents, int32(len(batch)))
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	const n = metronome.MaxEventsPerIngestRequest*2 + 5
	events := make([]metronome.UsageEvent, n)
	for i := range events {
		events[i] = metronome.UsageEvent{
			GroupID: "GR1", PeriodStart: time.Unix(int64(i*60), 0).UTC(),
			SKU: sku.BuildEventsBESCount, Count: 1, Unit: "count",
		}
	}
	c, err := metronome.NewClient(nil, nil)
	require.NoError(t, err)
	require.NoError(t, c.ReportUsage(t.Context(), events))
	assert.EqualValues(t, 3, atomic.LoadInt32(&requests))
	assert.EqualValues(t, n, atomic.LoadInt32(&totalEvents))
}

func TestIngestRetriesTransientFailures(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) < 3 {
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
	require.NoError(t, c.ReportUsage(t.Context(), []metronome.UsageEvent{{
		GroupID: "GR1", PeriodStart: time.Now(), SKU: sku.BuildEventsBESCount, Count: 1, Unit: "count",
	}}))
	assert.EqualValues(t, 3, atomic.LoadInt32(&attempts))
}

func TestIngestDoesNotRetryClientErrors(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
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
	err = c.ReportUsage(t.Context(), []metronome.UsageEvent{{
		GroupID: "GR1", PeriodStart: time.Now(), SKU: sku.BuildEventsBESCount, Count: 1, Unit: "count",
	}})
	require.Error(t, err)
	assert.True(t, status.IsInvalidArgumentError(err))
	assert.EqualValues(t, 1, atomic.LoadInt32(&attempts))
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

	periodStart := time.Date(2026, 5, 15, 12, 34, 0, 0, time.UTC)

	// Ingest two events with the same period, SKU, and labels, even though the labels are in a different order and the  count is different.
	c, err := metronome.NewClient(nil, nil)
	require.NoError(t, err)
	require.NoError(t, c.ReportUsage(t.Context(), []metronome.UsageEvent{{
		GroupID: "GR1", PeriodStart: periodStart, SKU: sku.RemoteCacheCASHits, Count: 1, Unit: "count",
		Labels: map[sku.LabelName]sku.LabelValue{sku.Origin: sku.OriginExternal, sku.Client: sku.ClientBazel},
	}}))
	require.NoError(t, c.ReportUsage(t.Context(), []metronome.UsageEvent{{
		GroupID: "GR1", PeriodStart: periodStart, SKU: sku.RemoteCacheCASHits, Count: 999, Unit: "count",
		Labels: map[sku.LabelName]sku.LabelValue{sku.Client: sku.ClientBazel, sku.Origin: sku.OriginExternal},
	}}))
	require.Len(t, gotEvents, 2)

	// Check that the transaction IDs are the same.
	// Metronome de-dupes duplicate transaction IDs, which it important to prevent double-billing retries.
	assert.Equal(t, gotEvents[0].TransactionID, gotEvents[1].TransactionID, "transaction ID must be independent of count and label-map iteration order")
}
