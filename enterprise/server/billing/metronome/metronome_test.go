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
	testflags.Set(t, "billing.metronome.package_alias", "self-serve-free")

	c, err := metronome.NewClient(nil, nil)
	require.NoError(t, err)
	id, err := c.CreateCustomer(t.Context(), "Acme", "GR1")
	require.NoError(t, err)
	assert.Equal(t, "cust-1", id)

	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	require.NoError(t, c.CreateContract(t.Context(), "cust-1", start, "GR1"))
	assert.Equal(t, map[string]any{
		"customer_id":    "cust-1",
		"package_alias":  "self-serve-free",
		"starting_at":    "2026-09-01T00:00:00Z",
		"uniqueness_key": "GR1",
	}, contract)

	conflict = true
	require.NoError(t, c.CreateContract(t.Context(), "cust-1", start, "GR1"))

	testflags.Set(t, "billing.metronome.package_alias", "")
	err = c.CreateContract(t.Context(), "cust-1", start, "GR1")
	require.True(t, status.IsFailedPreconditionError(err), "unexpected error: %v", err)
}

func TestFindCustomerID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "GET /v1/customers", r.Method+" "+r.URL.Path)
		switch r.URL.Query().Get("ingest_alias") {
		case "GR1", "GR2":
			fmt.Fprint(w, `{"data":[{"id":"cust-1","ingest_aliases":["GR1"]}]}`)
		default:
			fmt.Fprint(w, `{"data":[]}`)
		}
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	c, err := metronome.NewClient(nil, nil)
	require.NoError(t, err)
	for alias, want := range map[string]string{"GR1": "cust-1", "GR2": "", "GR3": ""} {
		id, err := c.FindCustomerID(t.Context(), alias)
		require.NoError(t, err)
		assert.Equal(t, want, id, "alias %s", alias)
	}
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

func TestGetCurrentInvoice(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/customers/cust-1/invoices", r.URL.Path)
		assert.Equal(t, "Bearer read-only-key", r.Header.Get("Authorization"))
		assert.Equal(t, "DRAFT", r.URL.Query().Get("status"))
		fmt.Fprint(w, `{"data":[
			{"id":"inv-2","type":"USAGE","status":"DRAFT","start_timestamp":"2026-09-01T00:00:00Z","end_timestamp":"2026-10-01T00:00:00Z","total":2400000,
			 "line_items":[
				{"name":"Action cache hits","type":"usage","quantity":2000,"unit_price":1200,"total":2400000,"starting_at":"2026-09-01T00:00:00Z","ending_before":"2026-09-19T00:00:00Z"},
				{"name":"Monthly credit applied","type":"applied_commit_or_credit","quantity":null,"unit_price":null,"total":-4000,"starting_at":"2026-09-01T00:00:00Z","ending_before":"2026-09-19T00:00:00Z"},
				{"name":"Action cache hits","type":"usage","quantity":0,"unit_price":2400,"total":0,"starting_at":"2026-09-19T00:00:00Z","ending_before":"2026-10-01T00:00:00Z"}]},
			{"id":"inv-1","type":"USAGE","status":"DRAFT","start_timestamp":"2026-08-01T00:00:00Z","end_timestamp":"2026-09-01T00:00:00Z","total":99}
		]}`)
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.read_only_api_key", "read-only-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	c, err := metronome.NewReadOnlyClient(nil, nil)
	require.NoError(t, err)
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	invoice, err := c.GetCurrentInvoice(t.Context(), "cust-1", now)
	require.NoError(t, err)
	require.NotNil(t, invoice)
	assert.Equal(t, float64(2400000), invoice.Total)
	assert.Equal(t, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC), invoice.StartTimestamp)
	priceChange := time.Date(2026, 9, 19, 0, 0, 0, 0, time.UTC)
	assert.Equal(t, []metronome.InvoiceLineItem{
		{Name: "Action cache hits", Type: metronome.LineItemTypeUsage, Quantity: 2000, UnitPrice: 1200, Total: 2400000, StartingAt: invoice.StartTimestamp, EndingBefore: priceChange},
		{Name: "Action cache hits", Type: metronome.LineItemTypeUsage, Quantity: 0, UnitPrice: 2400, Total: 0, StartingAt: priceChange, EndingBefore: invoice.EndTimestamp},
	}, invoice.UsageLineItems())

	invoice, err = c.GetCurrentInvoice(t.Context(), "cust-1", now.AddDate(0, 2, 0))
	require.NoError(t, err)
	assert.Nil(t, invoice)
}

func TestGetCredit(t *testing.T) {
	var body map[string]any
	balances := `{"data":[
		{"type":"CREDIT","balance":1000,"access_schedule":{"schedule_items":[
			{"amount":5000,"starting_at":"2026-09-01T00:00:00Z","ending_before":"2026-10-01T00:00:00Z"}]}},
		{"type":"PREPAID","balance":700,"access_schedule":{"schedule_items":[
			{"amount":900,"starting_at":"2026-09-01T00:00:00Z","ending_before":"2026-10-01T00:00:00Z"}]}}
	]}`
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "POST /v1/contracts/customerBalances/list", r.Method+" "+r.URL.Path)
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		fmt.Fprint(w, balances)
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.read_only_api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	c, err := metronome.NewReadOnlyClient(nil, nil)
	require.NoError(t, err)
	now := time.Date(2026, 9, 16, 12, 0, 0, 0, time.UTC)
	credit, err := c.GetCredit(t.Context(), "cust-1", now)
	require.NoError(t, err)
	assert.Equal(t, &metronome.Credit{Granted: 5000, Remaining: 1000}, credit)
	assert.Equal(t, map[string]any{
		"customer_id":               "cust-1",
		"covering_date":             "2026-09-16T12:00:00Z",
		"include_balance":           true,
		"include_contract_balances": true,
	}, body)

	balances = `{"data":[]}`
	credit, err = c.GetCredit(t.Context(), "cust-1", now)
	require.NoError(t, err)
	assert.Nil(t, credit)
}
