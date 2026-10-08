package github_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
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
		name      string
		code      int
		wantError bool
	}{
		{name: "status created", code: http.StatusCreated},
		{name: "authentication failure", code: http.StatusUnauthorized, wantError: true},
		{name: "permission failure", code: http.StatusForbidden, wantError: true},
		{name: "missing repository", code: http.StatusNotFound, wantError: true},
		{name: "rate limit without delay", code: http.StatusTooManyRequests, wantError: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
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
				w.WriteHeader(tc.code)
				io.WriteString(w, `{}`)
			}))
			t.Cleanup(server.Close)
			oldTransport := http.DefaultTransport
			http.DefaultTransport = server.Client().Transport
			t.Cleanup(func() { http.DefaultTransport = oldTransport })
			flags.Set(t, "github.enterprise_host", strings.TrimPrefix(server.URL, "https://"))
			flags.Set(t, "github.access_token", "test-token")
			flags.Set(t, "github.status_name_suffix", "(dev)")
			client := github.NewGithubClient(testenv.GetTestEnv(t), "")
			payload := github.NewGithubStatusPayload("Remote tests", "https://example.com/build", "Passed", github.SuccessState)
			err := client.CreateStatus(t.Context(), "GR1", "example/project", "abc123", payload)
			if tc.wantError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.EqualValues(t, 1, calls.Load())
			request := <-requests
			require.NoError(t, request.err)
			require.Equal(t, "POST", request.method)
			require.Equal(t, "/api/v3/repos/example/project/statuses/abc123", request.path)
			require.Equal(t, "Bearer test-token", request.authorization)
			require.JSONEq(t, `{"context":"Remote tests (dev)","target_url":"https://example.com/build","description":"Passed","state":"success"}`, string(request.body))
		})
	}
}
