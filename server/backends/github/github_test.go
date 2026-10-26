package github_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

func TestIsStatusReportingEnabled(t *testing.T) {
	for _, enabled := range []bool{true, false} {
		te := testenv.GetTestEnv(t)

		groupID := "GR1"
		repoURL := "https://github.com/acme-inc/acme"

		// Use raw SQL to insert ReportCommitStatusesForCIBuilds: GORM
		// skips zero-value booleans during Create, so the column default (true)
		// would be used instead.
		res := te.GetDBHandle().NewQuery(context.Background(), "test_create_installation").Raw(
			`INSERT INTO "GitHubAppInstallations" (group_id, owner, user_id, installation_id, perms, app_id, report_commit_statuses_for_ci_builds) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			groupID, "acme-inc", "US1", 12345, 1, 0, enabled,
		).Exec()
		require.NoError(t, res.Error)

		client := github.NewGithubClient(te, "" /*token*/)

		// Use an unauthenticated context, as would be used when handling a webhook request.
		actual, err := client.IsStatusReportingEnabled(t.Context(), groupID, repoURL)
		require.NoError(t, err)
		require.Equal(t, enabled, actual)
	}
}

func TestCreateStatusSDK(t *testing.T) {
	for _, tc := range []struct {
		name       string
		code       int
		body       string
		wantError  bool
		wantStatus string
	}{
		{name: "status created", code: http.StatusCreated, body: `{}`, wantStatus: "OK"},
		{name: "successful status with malformed body", code: http.StatusCreated, body: `{"state":`, wantStatus: "OK"},
		{name: "successful status with incompatible body", code: http.StatusOK, body: `{"state":42}`, wantStatus: "OK"},
		{name: "authentication failure", code: http.StatusUnauthorized, body: `{}`, wantError: true, wantStatus: "Unknown"},
		{name: "permission failure", code: http.StatusForbidden, body: `{}`, wantError: true, wantStatus: "Unknown"},
		{name: "missing repository", code: http.StatusNotFound, body: `{}`, wantError: true, wantStatus: "Unknown"},
		{name: "rate limit without delay", code: http.StatusTooManyRequests, body: `{}`, wantError: true, wantStatus: "Unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics.GitHubStatusRequestDurationUsec.Reset()
			metrics.GitHubStatusDeliveryDurationUsec.Reset()
			clock := clockwork.NewFakeClock()
			requestDuration := 25 * time.Millisecond
			type receivedRequest struct {
				method, path, authorization string
				body                        []byte
				err                         error
			}
			requests := make(chan receivedRequest, 1)
			var calls atomic.Int32
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				body, err := io.ReadAll(r.Body)
				requests <- receivedRequest{r.Method, r.URL.Path, r.Header.Get("Authorization"), body, err}
				w.Header().Set("Content-Type", "application/json")
				clock.Advance(requestDuration)
				w.WriteHeader(tc.code)
				io.WriteString(w, tc.body)
			}))
			t.Cleanup(server.Close)
			oldTransport := http.DefaultTransport
			http.DefaultTransport = server.Client().Transport
			t.Cleanup(func() { http.DefaultTransport = oldTransport })
			flags.Set(t, "github.enterprise_host", strings.TrimPrefix(server.URL, "https://"))
			flags.Set(t, "github.access_token", "test-token")
			flags.Set(t, "github.status_name_suffix", "(dev)")
			te := testenv.GetTestEnv(t)
			te.SetClock(clock)
			client := github.NewGithubClient(te, "")
			payload := github.NewGithubStatusPayload("Remote tests", "https://example.com/build", "Passed", github.SuccessState)
			err := client.CreateStatus(t.Context(), "GR1", "example/project", "abc123", payload)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, 1, calls.Load())
			testmetrics.AssertHistogramSamples(t, metrics.GitHubStatusRequestDurationUsec, float64(requestDuration.Microseconds()))
			testmetrics.AssertHistogramSamples(t, metrics.GitHubStatusDeliveryDurationUsec, float64(requestDuration.Microseconds()))
			require.Equal(t, map[string]string{metrics.HTTPResponseCodeLabel: strconv.Itoa(tc.code)}, testmetrics.HistogramVecValues(t, metrics.GitHubStatusRequestDurationUsec)[0].Labels)
			require.Equal(t, map[string]string{metrics.StatusHumanReadableLabel: tc.wantStatus}, testmetrics.HistogramVecValues(t, metrics.GitHubStatusDeliveryDurationUsec)[0].Labels)
			request := <-requests
			require.NoError(t, request.err)
			require.Equal(t, "POST", request.method)
			require.Equal(t, "/api/v3/repos/example/project/statuses/abc123", request.path)
			require.Equal(t, "Bearer test-token", request.authorization)
			require.JSONEq(t, `{"context":"Remote tests (dev)","target_url":"https://example.com/build","description":"Passed","state":"success"}`, string(request.body))
		})
	}
}

// roundTripFunc supplies failures without relying on network timing.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

func TestCreateStatusEnterpriseRouting(t *testing.T) {
	for _, tc := range []struct {
		name string
		host string
	}{
		{name: "enterprise hostname", host: "git.example.com"},
		{name: "API prefix hostname", host: "api.git.example.com"},
		{name: "API subdomain hostname", host: "git.api.example.com"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var request *http.Request
			oldTransport := http.DefaultTransport
			http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				request = r
				return &http.Response{
					StatusCode: http.StatusCreated,
					Header:     make(http.Header),
					Body:       io.NopCloser(strings.NewReader(`{}`)),
				}, nil
			})
			t.Cleanup(func() { http.DefaultTransport = oldTransport })
			flags.Set(t, "github.enterprise_host", tc.host)
			flags.Set(t, "github.access_token", "test-token")
			client := github.NewGithubClient(testenv.GetTestEnv(t), "")
			payload := github.NewGithubStatusPayload("Remote tests", "https://example.com/build", "Passed", github.SuccessState)
			require.NoError(t, client.CreateStatus(t.Context(), "GR1", "example/project", "abc123", payload))
			require.NotNil(t, request)
			require.Equal(t, "https://"+tc.host+"/api/v3/repos/example/project/statuses/abc123", request.URL.String())
		})
	}
}

func TestCreateStatusTransportError(t *testing.T) {
	for _, tc := range []struct {
		name          string
		err           error
		matchesStatus func(error) bool
		wantStatus    string
	}{
		{name: "wrapped cancellation", err: fmt.Errorf("request interrupted: %w", context.Canceled), matchesStatus: status.IsCanceledError, wantStatus: "Canceled"},
		{name: "wrapped deadline", err: fmt.Errorf("request timed out: %w", context.DeadlineExceeded), matchesStatus: status.IsDeadlineExceededError, wantStatus: "DeadlineExceeded"},
		{name: "connection failure", err: io.ErrUnexpectedEOF, matchesStatus: status.IsUnavailableError, wantStatus: "Unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics.GitHubStatusRequestDurationUsec.Reset()
			metrics.GitHubStatusDeliveryDurationUsec.Reset()
			clock := clockwork.NewFakeClock()
			oldTransport := http.DefaultTransport
			http.DefaultTransport = roundTripFunc(func(r *http.Request) (*http.Response, error) {
				clock.Advance(25 * time.Millisecond)
				return nil, tc.err
			})
			t.Cleanup(func() { http.DefaultTransport = oldTransport })
			flags.Set(t, "github.access_token", "test-token")
			te := testenv.GetTestEnv(t)
			te.SetClock(clock)
			client := github.NewGithubClient(te, "")
			payload := github.NewGithubStatusPayload("Remote tests", "https://example.com/build", "Passed", github.SuccessState)
			err := client.CreateStatus(t.Context(), "GR1", "example/project", "abc123", payload)
			require.Error(t, err)
			require.True(t, tc.matchesStatus(err), "unexpected status: %s", err)
			testmetrics.AssertHistogramSamples(t, metrics.GitHubStatusRequestDurationUsec, 25000)
			testmetrics.AssertHistogramSamples(t, metrics.GitHubStatusDeliveryDurationUsec, 25000)
			require.Equal(t, map[string]string{metrics.HTTPResponseCodeLabel: "0"}, testmetrics.HistogramVecValues(t, metrics.GitHubStatusRequestDurationUsec)[0].Labels)
			require.Equal(t, map[string]string{metrics.StatusHumanReadableLabel: tc.wantStatus}, testmetrics.HistogramVecValues(t, metrics.GitHubStatusDeliveryDurationUsec)[0].Labels)
		})
	}
}
