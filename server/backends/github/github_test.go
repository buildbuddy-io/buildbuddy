package github_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

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

func TestCreateStatusRequestMetrics(t *testing.T) {
	for _, tc := range []struct {
		name         string
		code         int
		transportErr error
		wantError    bool
	}{
		{name: "status created", code: http.StatusCreated},
		{name: "rate limited", code: http.StatusTooManyRequests, wantError: true},
		{name: "service unavailable", code: http.StatusServiceUnavailable, wantError: true},
		{name: "transport failure", transportErr: errors.New("connection reset"), wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics.GitHubStatusRequestDurationUsec.Reset()
			clock := clockwork.NewFakeClock()
			requestDuration := 25 * time.Millisecond
			var calls int
			oldTransport := http.DefaultTransport
			http.DefaultTransport = roundTripperFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				clock.Advance(requestDuration)
				if tc.transportErr != nil {
					return nil, tc.transportErr
				}
				return &http.Response{
					StatusCode: tc.code,
					Status:     http.StatusText(tc.code),
					Body:       io.NopCloser(strings.NewReader(`{}`)),
				}, nil
			})
			t.Cleanup(func() { http.DefaultTransport = oldTransport })
			flags.Set(t, "github.access_token", "test-token")
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
			require.Equal(t, 1, calls)
			testmetrics.AssertHistogramSamples(t, metrics.GitHubStatusRequestDurationUsec, float64(requestDuration.Microseconds()))
			require.Equal(t, map[string]string{metrics.HTTPResponseCodeLabel: strconv.Itoa(tc.code)}, testmetrics.HistogramVecValues(t, metrics.GitHubStatusRequestDurationUsec)[0].Labels)
		})
	}
}
