package githubapp

import (
	"context"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-github/v59/github"
	"github.com/stretchr/testify/assert"
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
	wantRepos          []string
}

func (c *fakeAppClient) CreateInstallationToken(_ context.Context, installationID int64, opts *github.InstallationTokenOptions) (*github.InstallationToken, *github.Response, error) {
	assert.Equal(c.t, c.wantInstallationID, installationID)

	var gotRepos []string
	if opts != nil {
		gotRepos = opts.Repositories
	}
	assert.Equal(c.t, []string{testRepo}, gotRepos)

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
	err := te.GetDBHandle().NewQuery(ctx, "test_insert_repo").Create(&tables.GitRepository{
		GroupID: testGroupID,
		RepoURL: testRepoURL,
		AppID:   1,
		Perms:   1,
	})
	require.NoError(t, err)
}

func TestGetRepositoryInstallationToken(t *testing.T) {
	te, ctx := setupEnv(t)
	insertInstallation(t, te, ctx)
	app := newTestApp(te, &fakeAppClient{
		t:                  t,
		wantInstallationID: testInstallationID,
		wantRepos:          []string{testRepo},
	})

	tok, err := app.GetRepositoryInstallationToken(ctx, &tables.GitRepository{
		GroupID: testGroupID,
		RepoURL: testRepoURL,
	})
	require.NoError(t, err)
	assert.Equal(t, fakeToken, tok)
}

func TestGetRepositoryInstallationToken_Unauthorized(t *testing.T) {
	te, _ := setupEnv(t)

	// Create a context for a user in a different group.
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers("US2", "GR2"))
	unauthorizedCtx, err := auth.WithAuthenticatedUser(context.Background(), "US2")
	require.NoError(t, err)

	app := newTestApp(te, nil)

	_, err = app.GetRepositoryInstallationToken(unauthorizedCtx, &tables.GitRepository{
		GroupID: testGroupID,
		RepoURL: testRepoURL,
	})
	require.Error(t, err)
	assert.True(t, status.IsPermissionDeniedError(err))
}

func TestGetInstallationTokenForStatusReportingOnly(t *testing.T) {
	te, ctx := setupEnv(t)
	insertInstallation(t, te, ctx)
	app := newTestApp(te, &fakeAppClient{
		t:                  t,
		wantInstallationID: testInstallationID,
		wantRepos:          []string{testRepo},
	})

	tok, err := app.GetInstallationTokenForStatusReportingOnly(ctx, testOwner, testRepo)
	require.NoError(t, err)
	assert.Equal(t, fakeToken, tok.GetToken())
}
