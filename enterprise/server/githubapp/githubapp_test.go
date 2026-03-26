package githubapp

import (
	"context"
	"net/http"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
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
	c.t.Helper()
	assert.Equal(c.t, c.wantInstallationID, installationID)

	var gotRepos []string
	if opts != nil {
		gotRepos = opts.Repositories
	}
	assert.Equal(c.t, c.wantRepos, gotRepos)

	return &github.InstallationToken{Token: github.String(fakeToken)}, &github.Response{
		Response: &http.Response{StatusCode: http.StatusCreated},
	}, nil
}

func (c *fakeAppClient) GetInstallation(_ context.Context, _ int64) (*github.Installation, *github.Response, error) {
	require.FailNow(c.t, "unexpected GetInstallation call")
	return nil, nil, nil
}

func (c *fakeAppClient) FindRepositoryInstallation(_ context.Context, _, _ string) (*github.Installation, *github.Response, error) {
	require.FailNow(c.t, "unexpected FindRepositoryInstallation call")
	return nil, nil, nil
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
	t.Helper()
	te := enterprise_testenv.New(t)
	auth := testauth.NewTestAuthenticator(t, testauth.TestUsers(testUserID, testGroupID))
	te.SetAuthenticator(auth)
	ctx, err := auth.WithAuthenticatedUser(context.Background(), testUserID)
	require.NoError(t, err)
	return te, ctx
}

func insertInstallation(t *testing.T, te *testenv.TestEnv, ctx context.Context) {
	t.Helper()
	err := te.GetDBHandle().NewQuery(ctx, "test_insert_installation").Create(&tables.GitHubAppInstallation{
		UserID:         testUserID,
		GroupID:        testGroupID,
		InstallationID: testInstallationID,
		Owner:          testOwner,
	})
	require.NoError(t, err)
}

func insertRepo(t *testing.T, te *testenv.TestEnv, ctx context.Context) {
	t.Helper()
	err := te.GetDBHandle().NewQuery(ctx, "test_insert_repo").Create(&tables.GitRepository{
		GroupID: testGroupID,
		RepoURL: testRepoURL,
		AppID:   1,
		Perms:   1,
	})
	require.NoError(t, err)
}

// fakeWorkflowService is a minimal WorkflowService stub for tests that trigger
// maybeTriggerBuildBuddyWorkflow.
type fakeWorkflowService struct {
	interfaces.WorkflowService
}

func (f *fakeWorkflowService) HandleRepositoryEvent(_ context.Context, _ *tables.GitRepository, _ *interfaces.WebhookData, _ string) error {
	return nil
}

func TestCreateInstallationToken_WithRepoName(t *testing.T) {
	app := newTestApp(nil, &fakeAppClient{
		t:                  t,
		wantInstallationID: testInstallationID,
		wantRepos:          []string{testRepo},
	})

	tok, err := app.createInstallationToken(context.Background(), testInstallationID, testRepo)
	require.NoError(t, err)
	assert.Equal(t, fakeToken, tok.GetToken())
}

func TestCreateInstallationToken_WithoutRepoName(t *testing.T) {
	app := newTestApp(nil, &fakeAppClient{
		t:                  t,
		wantInstallationID: testInstallationID,
	})

	tok, err := app.createInstallationToken(context.Background(), testInstallationID, "")
	require.NoError(t, err)
	assert.Equal(t, fakeToken, tok.GetToken())
}

func TestGetRepositoryInstallationToken_ScopedToRepo(t *testing.T) {
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

func TestGetInstallationTokenForStatusReportingOnly_ScopedToRepo(t *testing.T) {
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

func TestMaybeTriggerBuildBuddyWorkflow_ScopedToRepo(t *testing.T) {
	te, ctx := setupEnv(t)
	insertInstallation(t, te, ctx)
	insertRepo(t, te, ctx)
	te.SetWorkflowService(&fakeWorkflowService{})
	app := newTestApp(te, &fakeAppClient{
		t:                  t,
		wantInstallationID: testInstallationID,
		wantRepos:          []string{testRepo},
	})

	event := &github.PushEvent{
		Ref:   github.String("refs/heads/main"),
		After: github.String("abc123"),
		Repo: &github.PushEventRepository{
			CloneURL:      github.String(testRepoURL + ".git"),
			DefaultBranch: github.String("main"),
		},
		HeadCommit: &github.HeadCommit{
			ID: github.String("abc123"),
		},
	}

	err := app.maybeTriggerBuildBuddyWorkflow(ctx, "push", event)
	require.NoError(t, err)
}
