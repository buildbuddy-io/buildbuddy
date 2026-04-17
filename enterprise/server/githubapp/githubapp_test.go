package githubapp

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	ghpb "github.com/buildbuddy-io/buildbuddy/proto/github"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-github/v59/github"
	"github.com/stretchr/testify/require"
)

const (
	testUserID         = "US1"
	testGroupID        = "GR1"
	testOwner          = "test-org"
	testRepo           = "test-repo"
	testInstallationID = int64(1234)
	fakeToken          = "fake-token"
)

var (
	testRepoURL = "https://github.com/" + testOwner + "/" + testRepo
)

type fakeAppClient struct {
	t                  testing.TB
	wantInstallationID int64
	createTokenCalls   int
}

func (c *fakeAppClient) CreateInstallationToken(_ context.Context, installationID int64, opts *github.InstallationTokenOptions) (*github.InstallationToken, *github.Response, error) {
	c.createTokenCalls++
	require.Equal(c.t, c.wantInstallationID, installationID)
	return &github.InstallationToken{Token: github.String(fakeToken)}, &github.Response{
		Response: &http.Response{StatusCode: http.StatusCreated},
	}, nil
}

func (c *fakeAppClient) GetInstallation(_ context.Context, _ int64) (*github.Installation, *github.Response, error) {
	return nil, nil, status.UnimplementedError("GetInstallation not implemented")
}

func (c *fakeAppClient) FindRepositoryInstallation(_ context.Context, _, _ string) (*github.Installation, *github.Response, error) {
	return nil, nil, status.UnimplementedError("FindRepositoryInstallation not implemented")
}

func newTestApp(env *testenv.TestEnv, client githubAppClient) *GitHubApp {
	return &GitHubApp{
		env: env,
		newAppClient: func(context.Context) (githubAppClient, error) {
			return client, nil
		},
	}
}

func setupEnv(t *testing.T) (*testenv.TestEnv, context.Context) {
	te := enterprise_testenv.New(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers(testUserID, testGroupID))
	te.SetAuthenticator(auth)
	ctx, err := auth.WithAuthenticatedUser(context.Background(), testUserID)
	require.NoError(t, err)
	return te, ctx
}

func insertInstallation(t *testing.T, te *testenv.TestEnv, ctx context.Context) {
	err := te.GetDBHandle().NewQuery(ctx, "test_insert_installation").Create(&tables.GitHubAppInstallation{
		UserID:         testUserID,
		GroupID:        testGroupID,
		InstallationID: testInstallationID,
		Owner:          testOwner,
	})
	require.NoError(t, err)
}

func insertRepo(t *testing.T, te *testenv.TestEnv, ctx context.Context) {
	insertRepoWithSettings(t, te, ctx, false /*=useDefaultWorkflowConfig*/, false /*=cancelOlderWorkflowRunsOnSamePR*/)
}

func insertRepoWithSettings(t *testing.T, te *testenv.TestEnv, ctx context.Context, useDefaultWorkflowConfig bool, cancelOlderWorkflowRunsOnSamePR bool) {
	err := te.GetDBHandle().NewQuery(ctx, "test_insert_repo").Create(&tables.GitRepository{
		GroupID:                         testGroupID,
		RepoURL:                         testRepoURL,
		AppID:                           1,
		Perms:                           1,
		UseDefaultWorkflowConfig:        useDefaultWorkflowConfig,
		CancelOlderWorkflowRunsOnSamePR: cancelOlderWorkflowRunsOnSamePR,
	})
	require.NoError(t, err)
}

func TestGetRepositoryInstallationToken(t *testing.T) {
	te, ctx := setupEnv(t)
	insertInstallation(t, te, ctx)
	insertRepo(t, te, ctx)
	client := &fakeAppClient{
		t:                  t,
		wantInstallationID: testInstallationID,
	}
	app := newTestApp(te, client)

	tok, err := app.GetRepositoryInstallationToken(ctx, testGroupID, testRepoURL)
	require.NoError(t, err)
	require.Equal(t, fakeToken, tok)
	require.Equal(t, 1, client.createTokenCalls)
}

func TestGetRepositoryInstallationToken_NormalizesRepoURL(t *testing.T) {
	te, ctx := setupEnv(t)
	insertInstallation(t, te, ctx)
	insertRepo(t, te, ctx)
	client := &fakeAppClient{
		t:                  t,
		wantInstallationID: testInstallationID,
	}
	app := newTestApp(te, client)

	tok, err := app.GetRepositoryInstallationToken(ctx, testGroupID, testRepoURL+".git")
	require.NoError(t, err)
	require.Equal(t, fakeToken, tok)
	require.Equal(t, 1, client.createTokenCalls)
}

func TestGetRepositoryInstallationToken_Unauthorized(t *testing.T) {
	te, _ := setupEnv(t)

	// Create a context for a user in a different group.
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("US2", "GR2"))
	unauthorizedCtx, err := auth.WithAuthenticatedUser(context.Background(), "US2")
	require.NoError(t, err)

	client := &fakeAppClient{
		t: t,
	}
	app := newTestApp(te, client)

	token, err := app.GetRepositoryInstallationToken(unauthorizedCtx, testGroupID, testRepoURL)
	require.Error(t, err)
	require.Empty(t, token)
	require.Equal(t, 0, client.createTokenCalls)
}

func TestGetRepositoryInstallationToken_RepoNotImported(t *testing.T) {
	te, ctx := setupEnv(t)
	insertInstallation(t, te, ctx)
	// Don't create a repo. This can happen if the user installs the BB GitHub app but doesn't
	// explicitly import any repos to BB.

	client := &fakeAppClient{
		t: t,
	}
	app := newTestApp(te, client)

	tok, err := app.GetRepositoryInstallationToken(ctx, testGroupID, testRepoURL)
	require.Error(t, err)
	require.Empty(t, tok)
	require.Equal(t, 0, client.createTokenCalls)
}

func TestGetLinkedGitHubRepos_IncludesRepoSettings(t *testing.T) {
	te, ctx := setupEnv(t)
	insertRepoWithSettings(t, te, ctx, true /*=useDefaultWorkflowConfig*/, true /*=cancelOlderWorkflowRunsOnSamePR*/)
	service := &GitHubAppService{env: te}

	rsp, err := service.GetLinkedGitHubRepos(ctx)
	require.NoError(t, err)
	require.Len(t, rsp.GetRepos(), 1)
	require.Equal(t, testRepoURL, rsp.GetRepos()[0].GetRepoUrl())
	require.True(t, rsp.GetRepos()[0].GetUseDefaultWorkflowConfig())
	require.True(t, rsp.GetRepos()[0].GetCancelOlderWorkflowRunsOnSamePr())
}

func TestUpdateRepoSettings_UpdatesAllRepoSettings(t *testing.T) {
	te, ctx := setupEnv(t)
	insertRepo(t, te, ctx)
	app := &GitHubApp{env: te}

	_, err := app.UpdateRepoSettings(ctx, &ghpb.UpdateRepoSettingsRequest{
		RepoUrl:                         testRepoURL,
		UseDefaultWorkflowConfig:        true,
		CancelOlderWorkflowRunsOnSamePr: true,
	})
	require.NoError(t, err)

	var repo tables.GitRepository
	err = te.GetDBHandle().NewQuery(ctx, "test_get_repo").Raw(`
		SELECT *
		FROM "GitRepositories"
		WHERE group_id = ?
		AND repo_url = ?
	`, testGroupID, testRepoURL).Take(&repo)
	require.NoError(t, err)
	require.True(t, repo.UseDefaultWorkflowConfig)
	require.True(t, repo.CancelOlderWorkflowRunsOnSamePR)
}

func TestGetInstallationTokenForStatusReportingOnly(t *testing.T) {
	te, ctx := setupEnv(t)
	insertInstallation(t, te, ctx)
	app := newTestApp(te, &fakeAppClient{
		t:                  t,
		wantInstallationID: testInstallationID,
	})

	tok, err := app.GetInstallationTokenForStatusReportingOnly(ctx, testOwner)
	require.NoError(t, err)
	require.Equal(t, fakeToken, tok.GetToken())

	// We support status reporting if you install the BB GitHub app but don't import any repos to BB.
	// Make sure we don't return an error, even if the repo is not imported to BB.
	gitRepository := &tables.GitRepository{}
	err = te.GetDBHandle().NewQuery(ctx, "test_get_installation_token_for_status_reporting_only").Raw(`
		SELECT *
		FROM "GitRepositories"
		WHERE group_id = ?
	`, testGroupID).Take(gitRepository)
	require.Error(t, err)
}

func TestValidateWebhookPayload(t *testing.T) {
	payload := []byte(`{"action":"created"}`)

	for _, test := range []struct {
		name                   string
		webhookSecret          string
		webhookSecretAlternate string
		requestIsSigned        bool
		requestSigningSecret   string
		wantErr                bool
	}{
		{
			name:                   "primary_only/request_signature_primary/succeeds",
			webhookSecret:          "primary-secret",
			webhookSecretAlternate: "",
			requestIsSigned:        true,
			requestSigningSecret:   "primary-secret",
			wantErr:                false,
		},
		{
			name:                   "primary_only/request_signature_alt/fails",
			webhookSecret:          "primary-secret",
			webhookSecretAlternate: "",
			requestIsSigned:        true,
			requestSigningSecret:   "alt-secret",
			wantErr:                true,
		},
		{
			name:                   "primary_only/request_signature_invalid/fails",
			webhookSecret:          "primary-secret",
			webhookSecretAlternate: "",
			requestIsSigned:        true,
			requestSigningSecret:   "invalid-secret",
			wantErr:                true,
		},
		{
			name:                   "primary_only/request_signature_empty/fails",
			webhookSecret:          "primary-secret",
			webhookSecretAlternate: "",
			requestIsSigned:        true,
			requestSigningSecret:   "",
			wantErr:                true,
		},
		{
			name:                   "primary_only/request_signature_missing/fails",
			webhookSecret:          "primary-secret",
			webhookSecretAlternate: "",
			requestIsSigned:        false,
			requestSigningSecret:   "",
			wantErr:                true,
		},
		{
			name:                   "primary_and_alt/request_signature_primary/succeeds",
			webhookSecret:          "primary-secret",
			webhookSecretAlternate: "alt-secret",
			requestIsSigned:        true,
			requestSigningSecret:   "primary-secret",
			wantErr:                false,
		},
		{
			name:                   "primary_and_alt/request_signature_alt/succeeds",
			webhookSecret:          "primary-secret",
			webhookSecretAlternate: "alt-secret",
			requestIsSigned:        true,
			requestSigningSecret:   "alt-secret",
			wantErr:                false,
		},
		{
			name:                   "primary_and_alt/request_signature_invalid/fails",
			webhookSecret:          "primary-secret",
			webhookSecretAlternate: "alt-secret",
			requestIsSigned:        true,
			requestSigningSecret:   "invalid-secret",
			wantErr:                true,
		},
		{
			name:                   "primary_and_alt/request_signature_empty/fails",
			webhookSecret:          "primary-secret",
			webhookSecretAlternate: "alt-secret",
			requestIsSigned:        true,
			requestSigningSecret:   "",
			wantErr:                true,
		},
		{
			name:                   "primary_and_alt/request_signature_missing/fails",
			webhookSecret:          "primary-secret",
			webhookSecretAlternate: "alt-secret",
			requestIsSigned:        false,
			requestSigningSecret:   "",
			wantErr:                true,
		},
		{
			name:                   "alt_only/request_signature_primary/fails",
			webhookSecret:          "",
			webhookSecretAlternate: "alt-secret",
			requestIsSigned:        true,
			requestSigningSecret:   "primary-secret",
			wantErr:                true,
		},
		{
			name:                   "alt_only/request_signature_alt/succeeds",
			webhookSecret:          "",
			webhookSecretAlternate: "alt-secret",
			requestIsSigned:        true,
			requestSigningSecret:   "alt-secret",
			wantErr:                false,
		},
		{
			name:                   "alt_only/request_signature_invalid/fails",
			webhookSecret:          "",
			webhookSecretAlternate: "alt-secret",
			requestIsSigned:        true,
			requestSigningSecret:   "invalid-secret",
			wantErr:                true,
		},
		{
			name:                   "alt_only/request_signature_empty/fails",
			webhookSecret:          "",
			webhookSecretAlternate: "alt-secret",
			requestIsSigned:        true,
			requestSigningSecret:   "",
			wantErr:                true,
		},
		{
			name:                   "alt_only/request_signature_missing/fails",
			webhookSecret:          "",
			webhookSecretAlternate: "alt-secret",
			requestIsSigned:        false,
			requestSigningSecret:   "",
			wantErr:                true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := &GitHubApp{
				webhookSecret:          test.webhookSecret,
				webhookSecretAlternate: test.webhookSecretAlternate,
			}

			req, err := http.NewRequest("POST", "http://localhost/webhooks/github/app", bytes.NewReader(payload))
			require.NoError(t, err)
			req.Header.Set("Content-Type", "application/json")
			if test.requestIsSigned {
				hash := hmac.New(sha256.New, []byte(test.requestSigningSecret))
				hash.Write(payload)
				signature := "sha256=" + hex.EncodeToString(hash.Sum(nil))
				req.Header.Set(github.SHA256SignatureHeader, signature)
			}

			got, err := app.validateWebhookPayload(req)
			if test.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Equal(t, payload, got)
		})
	}
}
