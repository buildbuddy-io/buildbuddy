package github_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestCreateStatus(t *testing.T) {
	for _, tc := range []struct {
		name         string
		responses    []int
		wantErr      bool
		wantAttempts int
	}{
		{
			name:         "succeeds after transient server errors",
			responses:    []int{http.StatusInternalServerError, http.StatusBadGateway, http.StatusCreated},
			wantAttempts: 3,
		},
		{
			name:         "succeeds after a rate limit",
			responses:    []int{http.StatusTooManyRequests, http.StatusCreated},
			wantAttempts: 2,
		},
		{
			name:         "does not retry a client error",
			responses:    []int{http.StatusUnprocessableEntity, http.StatusCreated},
			wantErr:      true,
			wantAttempts: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			attempts := 0
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/api/v3/repos/acme-inc/acme/statuses/abc123", r.URL.Path)
				w.WriteHeader(tc.responses[attempts])
				attempts++
			}))
			t.Cleanup(server.Close)

			defaultTransport := http.DefaultTransport
			http.DefaultTransport = server.Client().Transport
			t.Cleanup(func() { http.DefaultTransport = defaultTransport })

			serverURL, err := url.Parse(server.URL)
			require.NoError(t, err)
			flags.Set(t, "github.enterprise_host", serverURL.Host)
			flags.Set(t, "github.access_token", "test-token")

			te := testenv.GetTestEnv(t)
			client := github.NewGithubClient(te, "" /*token*/)
			payload := github.NewGithubStatusPayload("CI", "https://example.com", "Done", github.SuccessState)

			err = client.CreateStatus(t.Context(), "GR1", "acme-inc/acme", "abc123", payload)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.wantAttempts, attempts)
		})
	}
}
