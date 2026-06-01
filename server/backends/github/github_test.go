package github_test

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
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
