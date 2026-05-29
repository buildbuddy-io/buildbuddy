package metronome

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/usage/sku"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	testflags "github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
)

func TestReportUsageDisabledNoops(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("unexpected Metronome request")
	}))
	defer server.Close()

	testflags.Set(t, "billing.metronome.enabled", false)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	err := NewClient(ClientConfig{}).ReportUsage(context.Background(), "GR1", time.Now(), &tables.UsageLabels{}, &tables.UsageCounts{
		TotalDownloadSizeBytes: 1,
	})
	require.NoError(t, err)
}

func TestReportUsage(t *testing.T) {
	var gotEvents []usageEvent
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, ingestPath, r.URL.Path)
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		require.NoError(t, json.NewDecoder(r.Body).Decode(&gotEvents))
	}))
	defer server.Close()

	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.enabled", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")
	testflags.Set(t, "billing.metronome.api_url", server.URL)

	periodStart := time.Date(2026, 5, 15, 12, 34, 0, 0, time.UTC)
	err := NewClient(ClientConfig{}).ReportUsage(context.Background(), "GR1", periodStart, &tables.UsageLabels{
		Origin: "remote",
		Client: "bazel",
		Server: "cache",
	}, &tables.UsageCounts{
		Invocations:                          1,
		ActionCacheHits:                      2,
		CASCacheHits:                         3,
		CPUNanos:                             int64(90 * time.Second),
		LinuxExecutionDurationUsec:           10,
		MacExecutionDurationUsec:             20,
		SelfHostedLinuxExecutionDurationUsec: 30,
		SelfHostedMacExecutionDurationUsec:   40,
		TotalCachedActionExecUsec:            50,
		TotalDownloadSizeBytes:               2_000_000_000,
		TotalUploadSizeBytes:                 500_000_000,
		MemoryGBUsec:                         60,
	})
	require.NoError(t, err)

	require.Len(t, gotEvents, 12)
	transactionIDs := map[string]bool{}
	for _, event := range gotEvents {
		assert.Equal(t, "GR1", event.CustomerID)
		assert.Equal(t, periodStart.Format(time.RFC3339), event.Timestamp)
		assert.Equal(t, event.EventType, event.Properties["sku"])
		assert.NotEmpty(t, event.TransactionID)
		assert.Equal(t, 67, len(event.TransactionID))
		assert.False(t, transactionIDs[event.TransactionID], "duplicate transaction ID %s", event.TransactionID)
		transactionIDs[event.TransactionID] = true
	}

	download := findEvent(t, gotEvents, sku.RemoteCacheCASDownloadedBytes, nil)
	assert.Equal(t, "2000000000", download.Properties["count"])
	assert.Equal(t, "bytes", download.Properties["unit"])
	assert.Equal(t, "remote", download.Properties["origin"])
	assert.Equal(t, "bazel", download.Properties["client"])
	assert.Equal(t, "cache", download.Properties["server"])

	upload := findEvent(t, gotEvents, sku.RemoteCacheCASUploadedBytes, nil)
	assert.Equal(t, "500000000", upload.Properties["count"])
	assert.Equal(t, "bytes", upload.Properties["unit"])

	cpu := findEvent(t, gotEvents, sku.RemoteExecutionExecuteWorkerCPUNanos, map[string]string{
		"os":          "linux",
		"self_hosted": "false",
	})
	assert.Equal(t, "90000000000", cpu.Properties["count"])
	assert.Equal(t, "nanos", cpu.Properties["unit"])

	acHits := findEvent(t, gotEvents, sku.RemoteCacheACHits, nil)
	assert.Equal(t, "2", acHits.Properties["count"])
	assert.Equal(t, "count", acHits.Properties["unit"])

	casHits := findEvent(t, gotEvents, sku.RemoteCacheCASHits, nil)
	assert.Equal(t, "3", casHits.Properties["count"])
	assert.Equal(t, "count", casHits.Properties["unit"])

	findEvent(t, gotEvents, sku.RemoteExecutionExecuteWorkerDurationNanos, map[string]string{
		"os":          "mac",
		"self_hosted": "true",
	})
}

func TestReportUsageMapsHTTPStatus(t *testing.T) {
	testflags.Set(t, "http.client.allow_localhost", true)
	testflags.Set(t, "billing.metronome.enabled", true)
	testflags.Set(t, "billing.metronome.api_key", "test-key")

	for _, tc := range []struct {
		name      string
		code      int
		assertion func(error) bool
	}{
		{"bad_request", http.StatusBadRequest, status.IsInvalidArgumentError},
		{"unauthorized", http.StatusUnauthorized, status.IsUnauthenticatedError},
		{"forbidden", http.StatusForbidden, status.IsPermissionDeniedError},
		{"too_many_requests", http.StatusTooManyRequests, status.IsResourceExhaustedError},
		{"internal_server_error", http.StatusInternalServerError, status.IsUnavailableError},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "metronome error", tc.code)
			}))
			defer server.Close()
			testflags.Set(t, "billing.metronome.api_url", server.URL)

			err := NewClient(ClientConfig{}).ReportUsage(context.Background(), "GR1", time.Now(), &tables.UsageLabels{}, &tables.UsageCounts{
				TotalDownloadSizeBytes: 1,
			})
			require.Error(t, err)
			assert.True(t, tc.assertion(err), err)
		})
	}
}

func TestReportUsageRequiresAPIKeyWhenEnabled(t *testing.T) {
	testflags.Set(t, "billing.metronome.enabled", true)
	testflags.Set(t, "billing.metronome.api_key", "")
	testflags.Set(t, "billing.metronome.api_url", "http://metronome.invalid")

	err := NewClient(ClientConfig{}).ReportUsage(context.Background(), "GR1", time.Now(), &tables.UsageLabels{}, &tables.UsageCounts{
		TotalDownloadSizeBytes: 1,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "billing.metronome.api_key")
}

func TestReportUsageDefaultsEmptyOriginToExternal(t *testing.T) {
	events, err := eventsForUsage("GR1", time.Now(), &tables.UsageLabels{}, &tables.UsageCounts{
		TotalDownloadSizeBytes: 1,
	})
	require.NoError(t, err)
	require.Len(t, events, 1)
	assert.Equal(t, "external", events[0].Properties["origin"])
}

func TestTransactionIDHandlesLabelDelimiters(t *testing.T) {
	periodStart := time.Date(2026, 5, 15, 12, 34, 0, 0, time.UTC)
	counts := &tables.UsageCounts{TotalDownloadSizeBytes: 1}

	events1, err := eventsForUsage("GR1", periodStart, &tables.UsageLabels{
		Client: "a|origin=b",
		Origin: "c",
	}, counts)
	require.NoError(t, err)
	require.Len(t, events1, 1)

	events2, err := eventsForUsage("GR1", periodStart, &tables.UsageLabels{
		Client: "a",
		Origin: "b|origin=c",
	}, counts)
	require.NoError(t, err)
	require.Len(t, events2, 1)

	assert.NotEqual(t, events1[0].TransactionID, events2[0].TransactionID)
}

func findEvent(t *testing.T, events []usageEvent, usageSKU sku.SKU, labels map[string]string) usageEvent {
	t.Helper()
	for _, event := range events {
		if event.EventType != usageSKU.String() {
			continue
		}
		matches := true
		for key, value := range labels {
			if event.Properties[key] != value {
				matches = false
				break
			}
		}
		if matches {
			return event
		}
	}
	require.FailNowf(t, "event not found", "sku=%s labels=%v events=%v", usageSKU, labels, events)
	return usageEvent{}
}
