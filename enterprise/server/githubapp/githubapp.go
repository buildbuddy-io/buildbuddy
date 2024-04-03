package githubapp

import (
	"bytes"
	"context"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/scratchspace"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/golang-jwt/jwt"
	"github.com/google/go-github/v59/github"
	"github.com/shurcooL/githubv4"
	"golang.org/x/oauth2"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/durationpb"

	gh_webhooks "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	ghpb "github.com/buildbuddy-io/buildbuddy/proto/github"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rppb "github.com/buildbuddy-io/buildbuddy/proto/repo"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	gh_oauth "github.com/buildbuddy-io/buildbuddy/server/backends/github"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	gitobject "github.com/go-git/go-git/v5/plumbing/object"
	githttp "github.com/go-git/go-git/v5/plumbing/transport/http"
)

var (
	enabled       = flag.Bool("github.app.enabled", false, "Whether to enable the BuildBuddy GitHub app server.")
	clientID      = flag.String("github.app.client_id", "", "GitHub app OAuth client ID.")
	clientSecret  = flag.String("github.app.client_secret", "", "GitHub app OAuth client secret.", flag.Secret)
	appID         = flag.String("github.app.id", "", "GitHub app ID.")
	publicLink    = flag.String("github.app.public_link", "", "GitHub app installation URL.")
	privateKey    = flag.String("github.app.private_key", "", "GitHub app private key.", flag.Secret)
	webhookSecret = flag.String("github.app.webhook_secret", "", "GitHub app webhook secret used to verify that webhook payload contents were sent by GitHub.", flag.Secret)

	actionsRunnerEnabled = flag.Bool("github.app.actions.runner_enabled", false, "Whether to enable the buildbuddy-hosted runner for GitHub actions.")
	actionsRunnerLabel   = flag.String("github.app.actions.runner_label", "buildbuddy", "Label to apply to the actions runner. This is what 'runs-on' needs to be set to in GitHub workflow YAML in order to run on this BuildBuddy instance.")
	actionsPoolName      = flag.String("github.app.actions.runner_pool_name", "", "Executor pool name to use for GitHub actions runner.")

	enableReviewMutates = flag.Bool("github.app.review_mutates_enabled", false, "Perform mutations of PRs via the GitHub API.")

	validPathRegex = regexp.MustCompile(`^[a-zA-Z0-9/_-]*$`)
)

const (
	oauthAppPath = "/auth/github/app/link/"

	// Max page size that GitHub allows for list requests.
	githubMaxPageSize = 100

	// How long an ephemeral GitHub actions runner task should wait without
	// being assigned a job before it terminates.
	runnerIdleTimeout = 30 * time.Second

	// Max amount of time that a runner is allowed to run for until it is
	// killed. This is just a safeguard for now; we eventually should remove it.
	runnerTimeout = 1 * time.Hour
)

func Register(env *real_environment.RealEnv) error {
	if !*enabled {
		return nil
	}
	app, err := New(env)
	if err != nil {
		return err
	}
	env.SetGitHubApp(app)
	return nil
}

func IsEnabled() bool {
	return *enabled
}

// GitHubApp implements the BuildBuddy GitHub app. Users install the app to
// their personal account or organization, granting access to some or all
// repositories.
//
// Note that in GitHub's terminology, this is a proper "GitHub App" as opposed
// to an OAuth App. This means that it authenticates as its own entity, rather
// than on behalf of a particular user. See
// https://docs.github.com/en/developers/apps/getting-started-with-apps/about-apps
type GitHubApp struct {
	env environment.Env

	oauth *gh_oauth.OAuthHandler

	// privateKey is the GitHub-issued private key for the app. It is used to
	// create JWTs for authenticating with GitHub as the app itself.
	privateKey *rsa.PrivateKey
}

// New returns a new GitHubApp handle.
func New(env environment.Env) (*GitHubApp, error) {
	if *clientID == "" {
		return nil, status.FailedPreconditionError("missing client ID.")
	}
	if *clientSecret == "" {
		return nil, status.FailedPreconditionError("missing client secret.")
	}
	if *appID == "" {
		return nil, status.FailedPreconditionError("missing app ID")
	}
	if *publicLink == "" {
		return nil, status.FailedPreconditionError("missing app public link")
	}
	if *webhookSecret == "" {
		return nil, status.FailedPreconditionError("missing app webhook secret")
	}
	if *privateKey == "" {
		return nil, status.FailedPreconditionError("missing app private key")
	}
	privateKey, err := decodePrivateKey(*privateKey)
	if err != nil {
		return nil, err
	}

	app := &GitHubApp{
		env:        env,
		privateKey: privateKey,
	}
	oauth := gh_oauth.NewOAuthHandler(env, *clientID, *clientSecret, oauthAppPath)
	oauth.HandleInstall = app.handleInstall
	oauth.InstallURL = fmt.Sprintf("%s/installations/new", *publicLink)
	app.oauth = oauth
	return app, nil
}

func (a *GitHubApp) WebhookHandler() http.Handler {
	return http.HandlerFunc(a.handleWebhookRequest)
}

func (a *GitHubApp) handleWebhookRequest(w http.ResponseWriter, req *http.Request) {
	ctx := req.Context()
	b, err := github.ValidatePayload(req, []byte(*webhookSecret))
	if err != nil {
		log.CtxDebugf(ctx, "Failed to validate webhook payload: %s", err)
		http.Error(w, err.Error(), http.StatusUnauthorized)
		return
	}
	t := github.WebHookType(req)
	event, err := github.ParseWebHook(t, b)
	if err != nil {
		log.CtxWarningf(ctx, "Failed to parse GitHub webhook payload for %q event: %s", t, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	// Add delivery ID to context so we can correlate logs with GitHub's webhook
	// deliveries UI.
	ctx = log.EnrichContext(ctx, "github_delivery", req.Header.Get("X-GitHub-Delivery"))
	if err := a.handleWebhookEvent(ctx, t, event); err != nil {
		log.CtxErrorf(ctx, "Failed to handle webhook event %q: %s", t, err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	io.WriteString(w, "OK")
}

func (a *GitHubApp) handleWebhookEvent(ctx context.Context, eventType string, event any) error {
	// Delegate to the appropriate handler func based on event type.
	switch event := event.(type) {
	case *github.InstallationEvent:
		return a.handleInstallationEvent(ctx, eventType, event)
	case *github.WorkflowJobEvent:
		return a.handleWorkflowJobEvent(ctx, eventType, event)
	case *github.PushEvent:
		return a.handlePushEvent(ctx, eventType, event)
	case *github.PullRequestEvent:
		return a.handlePullRequestEvent(ctx, eventType, event)
	case *github.PullRequestReviewEvent:
		return a.handlePullRequestReviewEvent(ctx, eventType, event)
	default:
		// Event type not yet handled
		return nil
	}
}

func (a *GitHubApp) handleInstallationEvent(ctx context.Context, eventType string, event *github.InstallationEvent) error {
	log.CtxInfof(ctx, "Handling installation event type=%q, owner=%q, id=%d", eventType, event.GetInstallation().GetAccount().GetLogin(), event.GetInstallation().GetID())

	// Only handling uninstall events for now. We proactively handle this event
	// by removing references to the installation ID in the DB. Note, however,
	// that we still need to gracefully handle the case where the installation
	// ID is suddenly no longer valid, since webhook deliveries aren't 100%
	// reliable.
	if event.GetAction() != "deleted" {
		return nil
	}
	result := a.env.GetDBHandle().NewQuery(ctx, "githubapp_uninstall_app").Raw(`
		DELETE FROM "GitHubAppInstallations"
		WHERE installation_id = ?
	`, event.GetInstallation().GetID()).Exec()
	if result.Error != nil {
		return status.InternalErrorf("failed to delete installation: %s", result.Error)
	}

	log.CtxInfof(ctx,
		"Handling GitHub app uninstall event: removed %d installation row(s) for installation %d",
		result.RowsAffected, event.GetInstallation().GetID())
	return nil
}

func (a *GitHubApp) handleWorkflowJobEvent(ctx context.Context, eventType string, event *github.WorkflowJobEvent) error {
	if !*actionsRunnerEnabled {
		return nil
	}

	// If this is a queued event, and one of the labels is "buildbuddy", then
	// the user is requesting to run the job on one of BuildBuddy's runners.
	if event.GetAction() == "queued" {
		var labels []string
		if event.WorkflowJob != nil {
			labels = event.WorkflowJob.Labels
		}
		if slices.Contains(labels, *actionsRunnerLabel) {
			return a.startGitHubActionsRunnerTask(ctx, event)
		}
	}
	return nil
}

func (a *GitHubApp) handlePushEvent(ctx context.Context, eventType string, event *github.PushEvent) error {
	return a.maybeTriggerBuildBuddyWorkflow(ctx, eventType, event)
}

func (a *GitHubApp) handlePullRequestEvent(ctx context.Context, eventType string, event *github.PullRequestEvent) error {
	return a.maybeTriggerBuildBuddyWorkflow(ctx, eventType, event)
}

func (a *GitHubApp) handlePullRequestReviewEvent(ctx context.Context, eventType string, event *github.PullRequestReviewEvent) error {
	return a.maybeTriggerBuildBuddyWorkflow(ctx, eventType, event)
}

func (a *GitHubApp) startGitHubActionsRunnerTask(ctx context.Context, event *github.WorkflowJobEvent) error {
	if event.WorkflowJob == nil {
		return status.FailedPreconditionError("workflow job cannot be nil")
	}

	log.CtxInfof(ctx, "Starting ephemeral GitHub Actions runner execution for %s/%s", event.GetRepo().GetOwner().GetLogin(), event.GetRepo().GetName())

	// Get an org API key for the installation. The user must have linked the
	// installation to an org via the BuildBuddy UI, otherwise this will return
	// NotFound.
	installation := &tables.GitHubAppInstallation{}
	err := a.env.GetDBHandle().NewQuery(ctx, "github_app_get_installation_for_workflow_job").Raw(`
		SELECT *
		FROM "GitHubAppInstallations"
		WHERE installation_id = ?
		AND owner = ?
		`,
		event.GetInstallation().GetID(),
		event.GetRepo().GetOwner().GetLogin(),
	).Take(installation)
	if err != nil {
		return status.WrapError(err, "lookup installation")
	}
	apiKey, err := a.env.GetAuthDB().GetAPIKeyForInternalUseOnly(ctx, installation.GroupID)
	if err != nil {
		return status.WrapError(err, "lookup API key")
	}

	// Get an installation client.
	tok, err := a.createInstallationToken(ctx, event.GetInstallation().GetID())
	if err != nil {
		return err
	}
	client, err := a.newAuthenticatedClient(ctx, tok.GetToken())
	if err != nil {
		return err
	}
	// Register a "just-in-time" runner config for the incoming queued job, with
	// the same labels as the queued job. This lets us start a runner instance
	// that is authorized to execute a single job within the repo.
	//
	// TODO: once https://github.com/actions/runner/issues/620 is fixed,
	// restrict the runner to the exact job ID that was queued.
	runnerName := uuid.New()
	req := &github.GenerateJITConfigRequest{
		Name:          runnerName,
		RunnerGroupID: 1, // "default" group ID
		Labels:        []string{*actionsRunnerLabel},
	}
	jitRunnerConfig, res, err := client.Actions.GenerateRepoJITConfig(ctx, event.GetRepo().GetOwner().GetLogin(), event.GetRepo().GetName(), req)
	if err := checkResponse(res, err); err != nil {
		return err
	}
	// Spawn an ephemeral runner action on RBE. Note, this
	// ./buildbuddy_github_actions_runner binary is bundled with the executor
	// and is specially provisioned.
	cmd := &repb.Command{
		Arguments: []string{"./buildbuddy_github_actions_runner"},
		EnvironmentVariables: []*repb.Command_EnvironmentVariable{
			{Name: "HOME", Value: "/home/buildbuddy"},
			{Name: "PATH", Value: "/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"},
			{Name: "RUNNER_IDLE_TIMEOUT", Value: fmt.Sprintf("%d", int(runnerIdleTimeout.Seconds()))},
		},
		Platform: &repb.Platform{
			// TODO: make more of these configurable (via both the workflow YAML
			// and flags)
			Properties: []*repb.Platform_Property{
				{Name: "container-image", Value: "docker://" + platform.Ubuntu20_04GitHubActionsImage},
				{Name: "dockerUser", Value: "buildbuddy"},
				{Name: "EstimatedComputeUnits", Value: "3"},
				{Name: "EstimatedFreeDiskBytes", Value: "20GB"},
				{Name: "github-actions-runner-labels", Value: *actionsRunnerLabel},
				{Name: "init-dockerd", Value: "true"},
				{Name: "Pool", Value: *actionsPoolName},
				{Name: "recycle-runner", Value: "true"},
				{Name: "runner-recycling-max-wait", Value: "3s"},
				{Name: "workload-isolation-type", Value: "firecracker"},
			},
		},
	}
	// Authenticate the execution as the org linked to the installation
	ctx = metadata.AppendToOutgoingContext(ctx, authutil.APIKeyHeader, apiKey.Value)
	// Set jitconfig as env var via remote header to avoid storing it in CAS.
	ctx = platform.WithRemoteHeaderOverride(
		ctx, platform.EnvOverridesPropertyName,
		"RUNNER_ENCODED_JITCONFIG="+jitRunnerConfig.GetEncodedJITConfig())

	action := &repb.Action{
		DoNotCache: true,
		Timeout:    durationpb.New(runnerTimeout),
	}
	// TODO: respect GitRepository.instance_name_suffix, and allow manual cache
	// busting via the UI by setting instance_name_suffix on the GitRepository
	// row.
	instanceName := ""
	arn, err := rexec.Prepare(ctx, a.env, instanceName, repb.DigestFunction_SHA256, action, cmd, "" /*=inputRoot*/)
	if err != nil {
		return status.WrapError(err, "prepare runner action")
	}
	stream, err := rexec.Start(ctx, a.env, arn)
	if err != nil {
		return status.WrapError(err, "start runner execution")
	}
	op, err := stream.Recv()
	if err != nil {
		return status.WrapError(err, "wait for runner execution to be accepted")
	}
	log.CtxInfof(ctx, "Started ephemeral GitHub Actions runner execution %s", op.GetName())
	// Note: we don't wait for execution here; the RBE system is responsible for
	// driving the action to completion at this point.
	return nil
}

func (a *GitHubApp) maybeTriggerBuildBuddyWorkflow(ctx context.Context, eventType string, event any) error {
	wd, err := gh_webhooks.ParseWebhookData(event)
	if err != nil {
		return err
	}
	if wd == nil {
		// Could not parse any webhook data relevant to workflows;
		// nothing to do.
		log.CtxInfof(ctx, "No webhook data parsed for %q event", eventType)
		return nil
	}
	log.CtxInfof(ctx, "Parsed webhook data: %s", webhook_data.DebugString(wd))
	repoURL, err := gitutil.ParseGitHubRepoURL(wd.TargetRepoURL)
	if err != nil {
		return err
	}
	row := &struct {
		InstallationID int64
		*tables.GitRepository
	}{}
	err = a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installation_for_event").Raw(`
		SELECT i.installation_id, r.*
		FROM "GitHubAppInstallations" i, "GitRepositories" r
		WHERE r.repo_url = ?
		AND i.owner = ?
		AND i.group_id = r.group_id
	`, repoURL.String(), repoURL.Owner).Take(row)
	if err != nil {
		return status.NotFoundError("the repository as well as a BuildBuddy GitHub app installation must be linked to a BuildBuddy org in order to use workflows")
	}
	tok, err := a.createInstallationToken(ctx, row.InstallationID)
	if err != nil {
		return err
	}
	return a.env.GetWorkflowService().HandleRepositoryEvent(
		ctx, row.GitRepository, wd, tok.GetToken())
}

func (a *GitHubApp) GetInstallationTokenForStatusReportingOnly(ctx context.Context, owner string) (*github.InstallationToken, error) {
	var installation tables.GitHubAppInstallation
	err := a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installation_token_for_status").Raw(`
		SELECT *
		FROM "GitHubAppInstallations"
		WHERE owner = ?
	`, owner).Take(&installation)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundErrorf("failed to look up GitHub app installation: %s", err)
		}
		return nil, err
	}
	tok, err := a.createInstallationToken(ctx, installation.InstallationID)
	if err != nil {
		return nil, err
	}
	return tok, nil
}

func (a *GitHubApp) GetRepositoryInstallationToken(ctx context.Context, repo *tables.GitRepository) (string, error) {
	if err := authutil.AuthorizeGroupAccess(ctx, a.env, repo.GroupID); err != nil {
		return "", err
	}
	repoURL, err := gitutil.ParseGitHubRepoURL(repo.RepoURL)
	if err != nil {
		return "", err
	}
	var installation tables.GitHubAppInstallation
	err = a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installation_token").Raw(`
		SELECT *
		FROM "GitHubAppInstallations"
		WHERE group_id = ?
		AND owner = ?
	`, repo.GroupID, repoURL.Owner).Take(&installation)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return "", status.NotFoundErrorf("failed to look up GitHub app installation: %s", err)
		}
		return "", err
	}
	tok, err := a.createInstallationToken(ctx, installation.InstallationID)
	if err != nil {
		return "", err
	}
	return tok.GetToken(), nil
}

func (a *GitHubApp) GetGitHubAppInstallations(ctx context.Context, req *ghpb.GetAppInstallationsRequest) (*ghpb.GetAppInstallationsResponse, error) {
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	// List installations linked to the org.
	rq := a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installations").Raw(`
		SELECT *
		FROM "GitHubAppInstallations"
		WHERE group_id = ?
		ORDER BY owner ASC
	`, u.GetGroupID())
	res := &ghpb.GetAppInstallationsResponse{}
	err = db.ScanEach(rq, func(ctx context.Context, row *tables.GitHubAppInstallation) error {
		res.Installations = append(res.Installations, &ghpb.AppInstallation{
			GroupId:        row.GroupID,
			InstallationId: row.InstallationID,
			Owner:          row.Owner,
		})
		return nil
	})
	if err != nil {
		return nil, status.InternalErrorf("failed to get installations: %s", err)
	}
	return res, nil
}

func (a *GitHubApp) LinkGitHubAppInstallation(ctx context.Context, req *ghpb.LinkAppInstallationRequest) (*ghpb.LinkAppInstallationResponse, error) {
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	in, err := a.getInstallation(ctx, req.GetInstallationId())
	if err != nil {
		return nil, err
	}
	if err := a.linkInstallation(ctx, in, u.GetGroupID()); err != nil {
		return nil, err
	}
	return &ghpb.LinkAppInstallationResponse{}, nil
}

func (a *GitHubApp) linkInstallation(ctx context.Context, installation *github.Installation, groupID string) error {
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	if err := authutil.AuthorizeOrgAdmin(u, groupID); err != nil {
		return err
	}
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return err
	}
	if tu.GithubToken == "" {
		return status.UnauthenticatedError("failed to link GitHub app installation: GitHub account link is required")
	}
	if err := a.authorizeUserInstallationAccess(ctx, tu.GithubToken, installation.GetID()); err != nil {
		return err
	}
	err = a.createInstallation(ctx, &tables.GitHubAppInstallation{
		GroupID:        groupID,
		InstallationID: installation.GetID(),
		Owner:          installation.GetAccount().GetLogin(),
	})
	if err != nil {
		return status.InternalErrorf("failed to link GitHub app installation: %s", err)
	}
	return nil
}

func (a *GitHubApp) createInstallation(ctx context.Context, in *tables.GitHubAppInstallation) error {
	if in.Owner == "" {
		return status.FailedPreconditionError("owner field is required")
	}
	log.CtxInfof(ctx,
		"Linking GitHub app installation %d (%s) to group %s",
		in.InstallationID, in.Owner, in.GroupID)
	return a.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		// If an installation already exists with the given owner, unlink it
		// first. That installation must be stale since GitHub only allows
		// one installation per owner.
		err := tx.NewQuery(ctx, "githubapp_delete_existing_installation").Raw(`
			DELETE FROM "GitHubAppInstallations"
			WHERE owner = ?`,
			in.Owner,
		).Exec().Error
		if err != nil {
			return err
		}
		// Note: (GroupID, InstallationID) is the primary key, so this will fail
		// if the installation is already linked to another group.
		return tx.NewQuery(ctx, "githubapp_create_installation").Create(in)
	})
}

func (a *GitHubApp) UnlinkGitHubAppInstallation(ctx context.Context, req *ghpb.UnlinkAppInstallationRequest) (*ghpb.UnlinkAppInstallationResponse, error) {
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	if req.GetInstallationId() == 0 {
		return nil, status.FailedPreconditionError("missing installation_id")
	}
	// TODO(zoey): Could make this one query
	dbh := a.env.GetDBHandle()
	err = dbh.Transaction(ctx, func(tx interfaces.DB) error {
		var ti tables.GitHubAppInstallation
		err := tx.NewQuery(ctx, "githubapp_get_installation_for_unlink").Raw(`
			SELECT *
			FROM "GitHubAppInstallations"
			WHERE installation_id = ?
			`+dbh.SelectForUpdateModifier()+`
		`, req.GetInstallationId()).Take(&ti)
		if err != nil {
			return err
		}
		if err := authutil.AuthorizeOrgAdmin(u, ti.GroupID); err != nil {
			return err
		}
		return tx.NewQuery(ctx, "githubapp_unlink_installation").Raw(`
			DELETE FROM "GitHubAppInstallations"
			WHERE installation_id = ?
		`, req.GetInstallationId()).Exec().Error
	})
	if err != nil {
		return nil, err
	}
	return &ghpb.UnlinkAppInstallationResponse{}, nil
}

func (a *GitHubApp) GetInstallationByOwner(ctx context.Context, owner string) (*tables.GitHubAppInstallation, error) {
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	installation := &tables.GitHubAppInstallation{}
	err = a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_installations_by_owner").Raw(`
		SELECT * FROM "GitHubAppInstallations"
		WHERE group_id = ?
		AND owner = ?
	`, u.GetGroupID(), owner).Take(installation)
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundErrorf("no GitHub app installation for %q was found for the authenticated group", owner)
		}
		return nil, status.InternalErrorf("failed to look up GitHub app installation: %s", err)
	}
	return installation, nil
}

func (a *GitHubApp) GetLinkedGitHubRepos(ctx context.Context) (*ghpb.GetLinkedReposResponse, error) {
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	rq := a.env.GetDBHandle().NewQuery(ctx, "githubapp_get_linked_repos").Raw(`
		SELECT *
		FROM "GitRepositories"
		WHERE group_id = ?
		ORDER BY repo_url ASC
	`, u.GetGroupID())
	res := &ghpb.GetLinkedReposResponse{}
	err = db.ScanEach(rq, func(ctx context.Context, row *tables.GitRepository) error {
		res.RepoUrls = append(res.RepoUrls, row.RepoURL)
		return nil
	})
	if err != nil {
		return nil, status.InternalErrorf("failed to query repo rows: %s", err)
	}
	return res, nil
}
func (a *GitHubApp) LinkGitHubRepo(ctx context.Context, req *ghpb.LinkRepoRequest) (*ghpb.LinkRepoResponse, error) {
	repoURL, err := gitutil.ParseGitHubRepoURL(req.GetRepoUrl())
	if err != nil {
		return nil, err
	}

	// Make sure an installation exists and that the user has access to the
	// repo.
	installation, err := a.GetInstallationByOwner(ctx, repoURL.Owner)
	if err != nil {
		return nil, err
	}
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	// findUserRepo checks user-repo-installation authentication.
	if _, err := a.findUserRepo(ctx, tu.GithubToken, installation.InstallationID, repoURL.Repo); err != nil {
		return nil, err
	}

	if _, err := a.env.GetAuthenticator().AuthenticatedUser(ctx); err != nil {
		return nil, err
	}
	p, err := perms.ForAuthenticatedGroup(ctx, a.env)
	if err != nil {
		return nil, err
	}
	repo := &tables.GitRepository{
		UserID:  p.UserID,
		GroupID: p.GroupID,
		Perms:   p.Perms,
		RepoURL: repoURL.String(),
	}
	if err := a.env.GetDBHandle().NewQuery(ctx, "githubapp_create_repo").Create(repo); err != nil {
		return nil, status.InternalErrorf("failed to link repo: %s", err)
	}

	// Also clean up any associated workflows, since repo linking is meant to
	// replace workflows.
	deleteReq := &wfpb.DeleteWorkflowRequest{
		RequestContext: req.GetRequestContext(),
		RepoUrl:        req.GetRepoUrl(),
	}
	if _, err := a.env.GetWorkflowService().DeleteWorkflow(ctx, deleteReq); err != nil {
		log.CtxInfof(ctx, "Failed to delete legacy workflow for linked repo: %s", err)
	} else {
		log.CtxInfof(ctx, "Deleted legacy workflow for linked repo")
	}

	return &ghpb.LinkRepoResponse{}, nil
}
func (a *GitHubApp) UnlinkGitHubRepo(ctx context.Context, req *ghpb.UnlinkRepoRequest) (*ghpb.UnlinkRepoResponse, error) {
	norm, err := gitutil.NormalizeRepoURL(req.GetRepoUrl())
	if err != nil {
		return nil, status.InvalidArgumentErrorf("failed to parse repo URL: %s", err)
	}
	req.RepoUrl = norm.String()
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	result := a.env.GetDBHandle().NewQuery(ctx, "githubapp_unlink_repo").Raw(`
		DELETE FROM "GitRepositories"
		WHERE group_id = ?
		AND repo_url = ?
	`, u.GetGroupID(), req.GetRepoUrl()).Exec()
	if result.Error != nil {
		return nil, status.InternalErrorf("failed to unlink repo: %s", err)
	}
	if result.RowsAffected == 0 {
		return nil, status.NotFoundError("repo not found")
	}
	return &ghpb.UnlinkRepoResponse{}, nil
}

func (a *GitHubApp) GetAccessibleGitHubRepos(ctx context.Context, req *ghpb.GetAccessibleReposRequest) (*ghpb.GetAccessibleReposResponse, error) {
	req.Query = strings.TrimSpace(req.Query)

	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	userClient, err := a.newAuthenticatedClient(ctx, tu.GithubToken)
	if err != nil {
		return nil, err
	}
	// Note: the search API (filtering "user:{installationOwner}") does not show
	// private repos and also doesn't filter only to the installations
	// accessible to the installation. So instead we fetch the first page of
	// repos accessible to the installation and search through them here.
	opts := &github.ListOptions{PerPage: githubMaxPageSize}
	result, response, err := userClient.Apps.ListUserRepos(ctx, req.GetInstallationId(), opts)
	if err := checkResponse(response, err); err != nil {
		return nil, err
	}
	urls := make([]string, 0, len(result.Repositories))
	foundExactMatch := false
	for _, r := range result.Repositories {
		repo, err := gitutil.ParseGitHubRepoURL(r.GetCloneURL())
		if err != nil {
			return nil, err
		}
		if !strings.Contains(strings.ToLower(repo.Repo), strings.ToLower(req.Query)) {
			continue
		}
		if strings.EqualFold(req.Query, repo.Repo) {
			foundExactMatch = true
		}
		urls = append(urls, repo.String())
	}
	// We only fetch the first page of results (ordered alphabetically - GitHub
	// doesn't let us order any other way). As a result, we're not searching
	// across all repo URLs. So if we didn't find an exact match, make an extra
	// request to retry the search query as an exact match.
	if req.Query != "" && !foundExactMatch {
		ir, err := a.findUserRepo(ctx, tu.GithubToken, req.GetInstallationId(), req.Query)
		if err != nil {
			log.CtxDebugf(ctx, "Could not find exact repo match: %s", err)
		} else {
			norm, err := gitutil.NormalizeRepoURL(ir.repository.GetCloneURL())
			if err != nil {
				return nil, err
			}
			urls = append([]string{norm.String()}, urls...)
		}
	}
	return &ghpb.GetAccessibleReposResponse{RepoUrls: urls}, nil
}

func (a *GitHubApp) CreateRepo(ctx context.Context, req *rppb.CreateRepoRequest) (*rppb.CreateRepoResponse, error) {
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if tu.GithubToken == "" {
		return nil, status.UnauthenticatedError("github account link is required")
	}

	// Pick the right client based on the request (organization or user).
	var githubClient *github.Client
	var token = tu.GithubToken
	if req.InstallationTargetType != "Organization" {
		githubClient, err = a.newAuthenticatedClient(ctx, token)
	} else {
		githubClient, token, err = a.newInstallationClient(ctx, token, req.InstallationId)
	}
	if err != nil {
		return nil, err
	}

	repoURL := fmt.Sprintf("https://github.com/%s/%s", req.Owner, req.Name)

	// Create a new repository on github, if requested
	if !req.SkipRepo {
		organization := ""
		if req.InstallationTargetType == "Organization" {
			organization = req.Owner
		}
		_, _, err := githubClient.Repositories.Create(ctx, organization, &github.Repository{
			Name:        github.String(req.Name),
			Description: github.String(req.Description),
			Private:     github.Bool(req.Private),
			AutoInit:    github.Bool(req.Template == ""),
		})
		if err != nil {
			return nil, err
		}
	}

	// If we have a template, copy the template contents to the new repo
	if req.Template != "" {
		tmpDirName := fmt.Sprintf("template-repo-%d-%s-*", req.InstallationId, req.Name)
		err = cloneTemplate(tu.Email, tmpDirName, token, req.Template, repoURL+".git", req.TemplateDirectory, req.DestinationDirectory, req.TemplateVariables, !req.SkipRepo)
		if err != nil {
			return nil, err
		}
	}

	// Link new repository to enable workflows, if requested
	if !req.SkipLink {
		wfs := a.env.GetWorkflowService()
		if wfs == nil {
			return nil, status.UnimplementedErrorf("no workflow service configured")
		}
		_, err = a.LinkGitHubRepo(ctx, &ghpb.LinkRepoRequest{
			RequestContext: req.RequestContext,
			RepoUrl:        repoURL,
		})
		if err != nil {
			return nil, err
		}
	}

	return &rppb.CreateRepoResponse{
		RepoUrl: repoURL,
	}, nil
}

// TODO(siggisim): consider moving template cloning to a remote action if it causes us troubles doing this on apps.
func cloneTemplate(email, tmpDirName, token, srcURL, destURL, srcDir, destDir string, templateVariables map[string]string, needsInit bool) error {
	// Make a temporary directory for the template
	templateTmpDir, err := scratchspace.MkdirTemp(tmpDirName + "-template")
	if err != nil {
		return err
	}
	defer os.RemoveAll(templateTmpDir)

	// Clone the template into the directory
	auth := &githttp.BasicAuth{
		Username: "github",
		Password: token,
	}
	_, err = git.PlainClone(templateTmpDir, false, &git.CloneOptions{
		URL:  srcURL,
		Auth: auth,
	})
	if err != nil {
		return err
	}
	// Remove existing git information so we can create a single initial commit
	err = os.RemoveAll(filepath.Join(templateTmpDir, ".git"))
	if err != nil {
		return err
	}
	// Remove any github workflows directories, because we won't have permission to create them
	err = os.RemoveAll(filepath.Join(templateTmpDir, ".github/workflows"))
	if err != nil {
		return err
	}
	// Don't allow template paths with dots or other non alphanumeric path components
	if !validPathRegex.MatchString(srcDir) {
		return status.FailedPreconditionErrorf("invalid template path: %q", srcDir)
	}
	path := filepath.Join(templateTmpDir, srcDir)
	// Intentionally using Lstat to avoid following symlinks
	if fileInfo, err := os.Lstat(path); err != nil || !fileInfo.IsDir() {
		return status.FailedPreconditionErrorf("not a valid directory: %q", srcDir)
	}
	// Replace any template variables
	if len(templateVariables) > 0 {
		replace(path, templateVariables)
	}
	// Make a temporary directory for the new repo
	repoTmpDir, err := scratchspace.MkdirTemp(tmpDirName + "-repo")
	if err != nil {
		return err
	}
	defer os.RemoveAll(repoTmpDir)

	newPath := repoTmpDir

	// If we have a destination directory that's not the repo root, update the path
	if destDir != "" {
		// Don't allow destinationPath paths with dots or other non alphanumeric path components
		if !validPathRegex.MatchString(destDir) {
			return status.FailedPreconditionErrorf("invalid destination path: %q", destDir)
		}
		if err := os.RemoveAll(newPath); err != nil {
			return err
		}
		newPath = filepath.Join(repoTmpDir, destDir)
		if err := os.MkdirAll(newPath, os.ModePerm); err != nil {
			return err
		}
	}
	// Initialize or clone the destination repo
	gitRepo, err := initOrClone(needsInit, newPath, destURL, auth)
	if err != nil {
		return err
	}
	fileInfo, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}
	// Move template files into the destination repo
	for _, file := range fileInfo {
		old := filepath.Join(path, file.Name())
		new := filepath.Join(newPath, file.Name())
		if err := os.RemoveAll(new); err != nil {
			return err
		}
		if err := os.Rename(old, new); err != nil {
			return err
		}
	}
	gitWorkTree, err := gitRepo.Worktree()
	if err != nil {
		return err
	}
	_, err = gitWorkTree.Add(".")
	if err != nil {
		return err
	}

	commitMessage := "Initial commit"
	if !needsInit {
		commitMessage = "Update"
	}

	_, err = gitWorkTree.Commit(commitMessage, &git.CommitOptions{All: true, Author: &gitobject.Signature{
		Email: email,
		When:  time.Now(),
	}})
	if err != nil {
		return err
	}
	return gitRepo.Push(&git.PushOptions{
		RemoteName: git.DefaultRemoteName,
		Auth:       auth,
		Force:      true,
	})
}

func initOrClone(init bool, dir, url string, auth transport.AuthMethod) (*git.Repository, error) {
	if !init {
		return git.PlainClone(dir, false, &git.CloneOptions{
			URL:  url,
			Auth: auth,
		})
	}
	gitRepo, err := git.PlainInit(dir, false)
	if err != nil {
		return nil, err
	}
	if err := gitRepo.Storer.SetReference(plumbing.NewSymbolicReference(plumbing.HEAD, plumbing.Main)); err != nil {
		return nil, err
	}
	_, err = gitRepo.CreateRemote(&config.RemoteConfig{
		Name: git.DefaultRemoteName,
		URLs: []string{url},
	})
	return gitRepo, err
}

// Walks the given directory and performs a find and replace in all file contents.
// All instance of the keys in the replacements map are replaced by the values in the map.
func replace(dir string, replacements map[string]string) error {
	return filepath.Walk(dir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		contents, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}
		for k, v := range replacements {
			contents = bytes.Replace(contents, []byte(k), []byte(v), -1)
		}
		return ioutil.WriteFile(path, contents, os.ModePerm)
	})
}

type installationRepository struct {
	installation *github.Installation
	repository   *github.Repository
}

// findUserRepo finds a repo within an installation, checking the user's access
// to the repo. It attempts to work around the fact that "apps.ListUserRepos"
// doesn't have any filtering options.
func (a *GitHubApp) findUserRepo(ctx context.Context, userToken string, installationID int64, repo string) (*installationRepository, error) {
	installationClient, _, err := a.newInstallationClient(ctx, userToken, installationID)
	if err != nil {
		return nil, err
	}
	installation, err := a.getInstallation(ctx, installationID)
	if err != nil {
		return nil, err
	}
	owner := installation.GetAccount().GetLogin()
	// Fetch repository so that we know the canonical repo name (the input
	// `repo` parameter might be equal ignoring case, but not exactly equal).
	repository, response, err := installationClient.Repositories.Get(ctx, owner, repo)
	if err := checkResponse(response, err); err != nil {
		return nil, err
	}
	// Fetch the associated installation to confirm whether the repository
	// is actually installed.
	appClient, err := a.newAppClient(ctx)
	if err != nil {
		return nil, err
	}
	_, _, err = appClient.Apps.FindRepositoryInstallation(ctx, owner, repo)
	if err != nil {
		return nil, err
	}
	return &installationRepository{
		installation: installation,
		repository:   repository,
	}, nil

}

func (a *GitHubApp) getInstallation(ctx context.Context, id int64) (*github.Installation, error) {
	client, err := a.newAppClient(ctx)
	if err != nil {
		return nil, status.WrapError(err, "failed to get installation")
	}
	inst, res, err := client.Apps.GetInstallation(ctx, id)
	if err := checkResponse(res, err); err != nil {
		return nil, status.WrapError(err, "failed to get installation")
	}
	return inst, nil
}

func (a *GitHubApp) createInstallationToken(ctx context.Context, installationID int64) (*github.InstallationToken, error) {
	client, err := a.newAppClient(ctx)
	if err != nil {
		return nil, err
	}
	t, res, err := client.Apps.CreateInstallationToken(ctx, installationID, nil)
	if err := checkResponse(res, err); err != nil {
		return nil, status.UnauthenticatedErrorf("failed to create installation token: %s", status.Message(err))
	}
	return t, nil
}

func (a *GitHubApp) authorizeUserInstallationAccess(ctx context.Context, userToken string, installationID int64) error {
	const installTimeout = 10 * time.Second
	ctx, cancel := context.WithTimeout(ctx, installTimeout)
	defer cancel()
	client, err := a.newAuthenticatedClient(ctx, userToken)
	if err != nil {
		return err
	}
	_, res, err := client.Apps.ListUserRepos(ctx, installationID, &github.ListOptions{PerPage: 1})
	if err := checkResponse(res, err); err != nil {
		return status.WrapError(err, "failed to authorize user installation access")
	}
	return nil
}

func (a *GitHubApp) OAuthHandler() http.Handler {
	return a.oauth
}

func (a *GitHubApp) handleInstall(ctx context.Context, groupID, setupAction string, installationID int64) (string, error) {
	// GitHub might take a second or two to actually create the installation.
	// We need to wait for the installation to be created since we store the
	// owner field in the DB.
	installation, err := a.waitForInstallation(ctx, installationID)
	if err != nil {
		return "", err
	}

	// If group ID is empty, the user initiated via the install via GitHub, not
	// the UI. Redirect them to a page that shows the group picker. The UI can
	// then make an RPC to complete the installation by linking it to the
	// desired org.
	if groupID == "" {
		redirect := fmt.Sprintf(
			"/settings/org/github/complete-installation?installation_id=%d&installation_owner=%s",
			installationID, installation.GetAccount().GetLogin())
		return redirect, nil
	}
	if err := a.linkInstallation(ctx, installation, groupID); err != nil {
		return "", err
	}
	return "", nil
}

// Waits for GitHub to create the installation. This is needed since GitHub
// doesn't atomically create the installation, but it should be created very
// shortly after the install (seconds).
func (a *GitHubApp) waitForInstallation(ctx context.Context, installationID int64) (*github.Installation, error) {
	const timeout = 15 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	r := retry.DefaultWithContext(ctx)
	var lastErr error
	for r.Next() {
		in, err := a.getInstallation(ctx, installationID)
		if err != nil {
			lastErr = err
			continue
		}
		return in, nil
	}
	return nil, status.DeadlineExceededErrorf("timed out waiting for installation %d to exist: %s", installationID, lastErr)
}

// newAppClient returns a GitHub client authenticated as the app.
func (a *GitHubApp) newAppClient(ctx context.Context) (*github.Client, error) {
	// Create and sign JWT
	t := jwt.New(jwt.GetSigningMethod("RS256"))
	t.Claims = &jwt.StandardClaims{
		Issuer:    *appID,
		IssuedAt:  time.Now().Add(-1 * time.Minute).Unix(),
		ExpiresAt: time.Now().Add(5 * time.Minute).Unix(),
	}
	jwtStr, err := t.SignedString(a.privateKey)
	if err != nil {
		log.Errorf("Failed to sign JWT: %s", err)
		return nil, status.InternalErrorf("failed to sign JWT")
	}
	return a.newAuthenticatedClient(ctx, jwtStr)
}

func (a *GitHubApp) newInstallationClient(ctx context.Context, userToken string, installationID int64) (*github.Client, string, error) {
	if err := a.authorizeUserInstallationAccess(ctx, userToken, installationID); err != nil {
		return nil, "", err
	}
	token, err := a.createInstallationToken(ctx, installationID)
	if err != nil {
		return nil, "", err
	}
	client, err := a.newAuthenticatedClient(ctx, token.GetToken())
	return client, token.GetToken(), err
}

// newAuthenticatedClient returns a GitHub client authenticated with the given
// access token.
func (a *GitHubApp) newAuthenticatedClient(ctx context.Context, accessToken string) (*github.Client, error) {
	if accessToken == "" {
		return nil, status.UnauthenticatedError("missing user access token")
	}
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	tc := oauth2.NewClient(ctx, ts)

	if gh_oauth.IsEnterpriseConfigured() {
		host := fmt.Sprintf("https://%s/", gh_oauth.GithubHost())
		return github.NewEnterpriseClient(host, host, tc)
	}

	return github.NewClient(tc), nil
}

func (a *GitHubApp) newAuthenticatedGraphQLClient(ctx context.Context, accessToken string) (*githubv4.Client, error) {
	if accessToken == "" {
		return nil, status.UnauthenticatedError("missing user access token")
	}
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	tc := oauth2.NewClient(ctx, ts)

	if gh_oauth.IsEnterpriseConfigured() {
		host := fmt.Sprintf("https://%s/", gh_oauth.GithubHost())
		return githubv4.NewEnterpriseClient(host, tc), nil
	}

	return githubv4.NewClient(tc), nil
}

// decodePrivateKey decodes a PEM-format RSA private key.
func decodePrivateKey(contents string) (*rsa.PrivateKey, error) {
	contents = strings.TrimSpace(contents)
	block, rest := pem.Decode([]byte(contents))
	if block == nil {
		return nil, status.FailedPreconditionError("failed to decode PEM block from private key")
	}
	if len(rest) > 0 {
		return nil, status.FailedPreconditionErrorf("PEM block is followed by extraneous data (length %d)", len(rest))
	}
	return x509.ParsePKCS1PrivateKey(block.Bytes)
}

func setCookie(w http.ResponseWriter, name, value string) {
	http.SetCookie(w, &http.Cookie{
		Name:     name,
		Value:    value,
		Expires:  time.Now().Add(time.Hour),
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Path:     "/",
	})
}

func getCookie(r *http.Request, name string) string {
	if c, err := r.Cookie(name); err == nil {
		return c.Value
	}
	return ""
}

// checkResponse is a convenience function for checking both the HTTP client
// error returned from the go-github library as well as the HTTP response code
// returned by GitHub.
func checkResponse(res *github.Response, err error) error {
	if err != nil {
		return status.UnknownErrorf("GitHub API request failed: %s", err)
	}
	if res.StatusCode >= 300 {
		return status.UnknownErrorf("GitHub API request failed: unexpected HTTP status %s", res.Status)
	}
	return nil
}

func (a *GitHubApp) getGithubClient(ctx context.Context) (*github.Client, error) {
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if tu.GithubToken == "" {
		return nil, status.UnauthenticatedError("github account link is required")
	}
	return a.newAuthenticatedClient(ctx, tu.GithubToken)
}

func (a *GitHubApp) getGithubGraphQLClient(ctx context.Context) (*githubv4.Client, error) {
	tu, err := a.env.GetUserDB().GetUser(ctx)
	if err != nil {
		return nil, err
	}
	if tu.GithubToken == "" {
		return nil, status.UnauthenticatedError("github account link is required")
	}
	return a.newAuthenticatedGraphQLClient(ctx, tu.GithubToken)
}

func (a *GitHubApp) GetGithubUserInstallations(ctx context.Context, req *ghpb.GetGithubUserInstallationsRequest) (*ghpb.GetGithubUserInstallationsResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	installations, _, err := client.Apps.ListUserInstallations(ctx, nil)
	if err != nil {
		return nil, err
	}

	res := &ghpb.GetGithubUserInstallationsResponse{
		Installations: []*ghpb.UserInstallation{},
	}

	for _, i := range installations {
		installation := &ghpb.UserInstallation{
			Id:         i.GetID(),
			Login:      i.Account.GetLogin(),
			Url:        i.GetHTMLURL(),
			TargetType: i.GetTargetType(),
			Permissions: &ghpb.UserInstallationPermissions{
				Administration:  i.GetPermissions().GetAdministration(),
				RepositoryHooks: i.GetPermissions().GetRepositoryHooks(),
				PullRequests:    i.GetPermissions().GetPullRequests(),
			},
		}
		res.Installations = append(res.Installations, installation)
	}

	return res, nil
}

func (a *GitHubApp) GetGithubUser(ctx context.Context, req *ghpb.GetGithubUserRequest) (*ghpb.GetGithubUserResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	user, _, err := client.Users.Get(ctx, "")
	if err != nil {
		return nil, err
	}

	return &ghpb.GetGithubUserResponse{
		Name:      user.GetName(),
		Login:     user.GetLogin(),
		AvatarUrl: user.GetAvatarURL(),
	}, nil
}

func (a *GitHubApp) GetGithubRepo(ctx context.Context, req *ghpb.GetGithubRepoRequest) (*ghpb.GetGithubRepoResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	repo, _, err := client.Repositories.Get(ctx, req.Owner, req.Repo)
	if err != nil {
		return nil, err
	}

	return &ghpb.GetGithubRepoResponse{
		DefaultBranch: repo.GetDefaultBranch(),
		Permissions: &ghpb.RepoPermissions{
			Push: repo.GetPermissions()["push"],
		},
	}, nil
}

func (a *GitHubApp) GetGithubContent(ctx context.Context, req *ghpb.GetGithubContentRequest) (*ghpb.GetGithubContentResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	contents, _, err := client.Repositories.DownloadContents(ctx, req.Owner, req.Repo, req.Path, &github.RepositoryContentGetOptions{Ref: req.Ref})
	if err != nil {
		return nil, err
	}
	defer contents.Close()

	if req.ExistenceOnly {
		return &ghpb.GetGithubContentResponse{}, nil
	}

	contentBytes, err := io.ReadAll(contents)
	if err != nil {
		return nil, err
	}

	return &ghpb.GetGithubContentResponse{
		Content: contentBytes,
	}, nil
}

func (a *GitHubApp) GetGithubTree(ctx context.Context, req *ghpb.GetGithubTreeRequest) (*ghpb.GetGithubTreeResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	tree, _, err := client.Git.GetTree(ctx, req.Owner, req.Repo, req.Ref, false)
	if err != nil {
		return nil, err
	}

	res := &ghpb.GetGithubTreeResponse{
		Sha: tree.GetSHA(),
	}
	for _, entry := range tree.Entries {
		res.Nodes = append(res.Nodes, githubToProtoTree(entry))
	}
	return res, nil
}

func githubToProtoTree(entry *github.TreeEntry) *ghpb.TreeNode {
	node := &ghpb.TreeNode{
		Path:    entry.GetPath(),
		Sha:     entry.GetSHA(),
		Type:    entry.GetType(),
		Mode:    entry.GetMode(),
		Size:    int64(entry.GetSize()),
		Content: []byte(entry.GetContent()),
	}
	return node
}

func protoToGithubTree(node *ghpb.TreeNode) *github.TreeEntry {
	if node.Content != nil {
		content := string(node.Content)
		return &github.TreeEntry{
			Path:    &node.Path,
			Mode:    &node.Mode,
			Content: &content,
		}
	}

	entry := &github.TreeEntry{
		Path: &node.Path,
		Mode: &node.Mode,
	}

	if node.Sha != "" {
		entry.SHA = &node.Sha
	}

	return entry
}

func (a *GitHubApp) CreateGithubTree(ctx context.Context, req *ghpb.CreateGithubTreeRequest) (*ghpb.CreateGithubTreeResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}
	entries := []*github.TreeEntry{}
	for _, node := range req.Nodes {
		entries = append(entries, protoToGithubTree(node))
	}
	tree, _, err := client.Git.CreateTree(ctx, req.Owner, req.Repo, req.BaseTree, entries)
	if err != nil {
		return nil, err
	}
	return &ghpb.CreateGithubTreeResponse{
		Sha: tree.GetSHA(),
	}, nil
}

func (a *GitHubApp) GetGithubBlob(ctx context.Context, req *ghpb.GetGithubBlobRequest) (*ghpb.GetGithubBlobResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	blob, _, err := client.Git.GetBlobRaw(ctx, req.Owner, req.Repo, req.Sha)
	if err != nil {
		return nil, err
	}

	return &ghpb.GetGithubBlobResponse{
		Content: blob,
	}, nil
}

func (a *GitHubApp) CreateGithubBlob(ctx context.Context, req *ghpb.CreateGithubBlobRequest) (*ghpb.CreateGithubBlobResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	content := string(req.Content)
	blob, _, err := client.Git.CreateBlob(ctx, req.Owner, req.Repo, &github.Blob{
		Content: &content,
	})
	if err != nil {
		return nil, err
	}

	return &ghpb.CreateGithubBlobResponse{
		Sha: blob.GetSHA(),
	}, nil
}

func (a *GitHubApp) CreateGithubPull(ctx context.Context, req *ghpb.CreateGithubPullRequest) (*ghpb.CreateGithubPullResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	pr, _, err := client.PullRequests.Create(ctx, req.Owner, req.Repo, &github.NewPullRequest{
		Head:  &req.Head,
		Base:  &req.Base,
		Title: &req.Title,
		Body:  &req.Body,
		Draft: &req.Draft,
	})
	if err != nil {
		return nil, err
	}

	return &ghpb.CreateGithubPullResponse{
		Url:        pr.GetHTMLURL(),
		PullNumber: int64(pr.GetNumber()),
		Ref:        pr.GetHead().GetRef(),
	}, nil
}

func (a *GitHubApp) MergeGithubPull(ctx context.Context, req *ghpb.MergeGithubPullRequest) (*ghpb.MergeGithubPullResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	_, _, err = client.PullRequests.Merge(ctx, req.Owner, req.Repo, int(req.PullNumber), "", &github.PullRequestOptions{MergeMethod: "squash"})
	if err != nil {
		return nil, err
	}

	return &ghpb.MergeGithubPullResponse{}, nil
}

func (a *GitHubApp) GetGithubCompare(ctx context.Context, req *ghpb.GetGithubCompareRequest) (*ghpb.GetGithubCompareResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	comparison, _, err := client.Repositories.CompareCommits(ctx, req.Owner, req.Repo, req.Base, req.Head, nil)
	if err != nil {
		return nil, err
	}

	res := &ghpb.GetGithubCompareResponse{
		AheadBy: int64(comparison.GetAheadBy()),
		Files:   []*ghpb.File{},
		Commits: []*ghpb.Commit{},
	}

	for _, c := range comparison.Commits {
		res.Commits = append(res.Commits, &ghpb.Commit{
			Sha: c.GetSHA(),
		})
	}

	for _, f := range comparison.Files {
		res.Files = append(res.Files, &ghpb.File{
			Name: f.GetFilename(),
			Sha:  f.GetSHA(),
		})
	}

	return res, nil
}

func (a *GitHubApp) GetGithubForks(ctx context.Context, req *ghpb.GetGithubForksRequest) (*ghpb.GetGithubForksResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	forks, _, err := client.Repositories.ListForks(ctx, req.Owner, req.Repo, nil)
	if err != nil {
		return nil, err
	}

	res := &ghpb.GetGithubForksResponse{
		Forks: []*ghpb.Fork{},
	}

	for _, f := range forks {
		res.Forks = append(res.Forks, &ghpb.Fork{
			Owner: f.GetOwner().GetLogin(),
		})
	}

	return res, nil
}

func (a *GitHubApp) CreateGithubFork(ctx context.Context, req *ghpb.CreateGithubForkRequest) (*ghpb.CreateGithubForkResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	_, _, err = client.Repositories.CreateFork(ctx, req.Owner, req.Repo, nil)
	if _, ok := err.(*github.AcceptedError); !ok && err != nil {
		return nil, err
	}

	return &ghpb.CreateGithubForkResponse{}, nil
}

func (a *GitHubApp) GetGithubCommits(ctx context.Context, req *ghpb.GetGithubCommitsRequest) (*ghpb.GetGithubCommitsResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	commits, _, err := client.Repositories.ListCommits(ctx, req.Owner, req.Repo, &github.CommitsListOptions{
		SHA: req.Sha,
		ListOptions: github.ListOptions{
			PerPage: int(req.PerPage),
		},
	})
	if err != nil {
		return nil, err
	}

	res := &ghpb.GetGithubCommitsResponse{
		Commits: []*ghpb.Commit{},
	}

	for _, c := range commits {
		commit := &ghpb.Commit{
			Sha:     c.GetSHA(),
			TreeSha: c.GetCommit().GetTree().GetSHA(),
		}
		res.Commits = append(res.Commits, commit)
	}

	return res, nil
}

func (a *GitHubApp) CreateGithubCommit(ctx context.Context, req *ghpb.CreateGithubCommitRequest) (*ghpb.CreateGithubCommitResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}
	commit := &github.Commit{
		Message: &req.Message,
		Tree:    &github.Tree{SHA: &req.Tree},
		Parents: []*github.Commit{},
	}
	for _, p := range req.Parents {
		commit.Parents = append(commit.Parents, &github.Commit{SHA: &p})
	}
	c, _, err := client.Git.CreateCommit(ctx, req.Owner, req.Repo, commit, nil)
	if err != nil {
		return nil, err
	}
	return &ghpb.CreateGithubCommitResponse{
		Sha: c.GetSHA(),
	}, nil
}

func (a *GitHubApp) UpdateGithubRef(ctx context.Context, req *ghpb.UpdateGithubRefRequest) (*ghpb.UpdateGithubRefResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	_, _, err = client.Git.UpdateRef(ctx, req.Owner, req.Repo, &github.Reference{Ref: &req.Head, Object: &github.GitObject{SHA: &req.Sha}}, req.Force)
	if err != nil {
		return nil, err
	}

	return &ghpb.UpdateGithubRefResponse{}, nil
}

func (a *GitHubApp) CreateGithubRef(ctx context.Context, req *ghpb.CreateGithubRefRequest) (*ghpb.CreateGithubRefResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	_, _, err = client.Git.CreateRef(ctx, req.Owner, req.Repo, &github.Reference{Ref: &req.Ref, Object: &github.GitObject{SHA: &req.Sha}})
	if err != nil {
		return nil, err
	}

	return &ghpb.CreateGithubRefResponse{}, nil
}

func (a *GitHubApp) GetGithubPullRequest(ctx context.Context, req *ghpb.GetGithubPullRequestRequest) (*ghpb.GetGithubPullRequestResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}
	usernameForFetch := req.GetUser()
	if usernameForFetch == "" {
		usernameForFetch = "@me"
	}
	incoming, outgoing, user, err := a.getIncomingAndOutgoingPRs(ctx, usernameForFetch, client)
	if err != nil {
		return nil, err
	}
	allIssues := append(outgoing.Issues, incoming.Issues...)

	usernameForAttentionSet := req.GetUser()
	if usernameForAttentionSet == "" {
		usernameForAttentionSet = user.GetLogin()
	}
	prs, err := a.populatePRMetadata(ctx, client, allIssues, usernameForAttentionSet)
	if err != nil {
		return nil, err
	}
	resp := &ghpb.GetGithubPullRequestResponse{
		Outgoing: []*ghpb.PullRequest{},
		Incoming: []*ghpb.PullRequest{},
	}
	for _, i := range outgoing.Issues {
		pr := prs[i.GetNodeID()]
		if len(pr.Reviews) == 0 {
			continue
		}
		resp.Outgoing = append(resp.Outgoing, pr)
	}
	for _, i := range incoming.Issues {
		resp.Incoming = append(resp.Incoming, prs[i.GetNodeID()])
	}
	return resp, nil
}

func (a *GitHubApp) CreateGithubPullRequestComment(ctx context.Context, req *ghpb.CreateGithubPullRequestCommentRequest) (*ghpb.CreateGithubPullRequestCommentResponse, error) {
	if !*enableReviewMutates {
		return nil, status.UnimplementedError("Not implemented")
	}
	graphqlClient, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}

	reviewId := req.GetReviewId()
	var commentId string

	var side githubv4.DiffSide
	if req.GetSide() == ghpb.CommentSide_LEFT_SIDE {
		side = githubv4.DiffSideLeft
	} else {
		side = githubv4.DiffSideRight
	}

	if reviewId == "" {
		// The Github API is really wonky around new reviews--let's just make
		// a review with a promotional comment for now and figure it out later.
		var m struct {
			AddPullRequestReview struct {
				PullRequestReview struct {
					Id   string
					Body string
				}
			} `graphql:"addPullRequestReview(input: $input)"`
		}
		input := githubv4.AddPullRequestReviewInput{
			PullRequestID: req.GetPullId(),
			Body:          githubv4.NewString("I wrote this review in the BuildBuddy code review UI alpha!"),
		}
		err := graphqlClient.Mutate(ctx, &m, input, nil)
		if err != nil {
			return nil, err
		}
		reviewId = m.AddPullRequestReview.PullRequestReview.Id
	}

	if req.GetThreadId() != "" {
		// This is a comment in reply to an existing thread, just add it.
		var m struct {
			PullRequestReviewThreadReply struct {
				ClientMutationId string
				Comment          struct {
					Id string
				}
			} `graphql:"addPullRequestReviewThreadReply(input: $input)"`
		}

		input := githubv4.AddPullRequestReviewThreadReplyInput{
			PullRequestReviewID:       githubv4.NewID(reviewId),
			PullRequestReviewThreadID: githubv4.String(req.GetThreadId()),
			Body:                      githubv4.String(req.GetBody()),
		}

		err := graphqlClient.Mutate(ctx, &m, input, nil)
		if err != nil {
			return nil, err
		}
		commentId = m.PullRequestReviewThreadReply.Comment.Id
	} else {
		// This is a comment in a new thread, create it.
		var m struct {
			AddPullRequestReviewThread struct {
				ClientMutationId string
				Thread           struct {
					Id       string
					Comments struct {
						Nodes []reviewComment
					} `graphql:"comments(last: 1)"`
				}
			} `graphql:"addPullRequestReviewThread(input: $input)"`
		}

		input := githubv4.AddPullRequestReviewThreadInput{
			PullRequestID:       githubv4.NewID(req.GetPullId()),
			PullRequestReviewID: githubv4.NewID(reviewId),
			Path:                githubv4.String(req.GetPath()),
			Line:                githubv4.NewInt(githubv4.Int(int(req.GetLine()))),
			Side:                &side,
			Body:                githubv4.String(req.GetBody()),
		}

		err := graphqlClient.Mutate(ctx, &m, input, nil)
		if err != nil {
			return nil, err
		}
		commentId = m.AddPullRequestReviewThread.Thread.Comments.Nodes[0].Id
	}

	return &ghpb.CreateGithubPullRequestCommentResponse{
		ReviewId:  reviewId,
		CommentId: commentId,
	}, nil
}

func (a *GitHubApp) UpdateGithubPullRequestComment(ctx context.Context, req *ghpb.UpdateGithubPullRequestCommentRequest) (*ghpb.UpdateGithubPullRequestCommentResponse, error) {
	if !*enableReviewMutates {
		return nil, status.UnimplementedError("Not implemented")
	}
	graphqlClient, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}
	var m struct {
		UpdatePullRequestReviewComment struct {
			ClientMutationId string
		} `graphql:"updatePullRequestReviewComment(input: $input)"`
	}
	input := githubv4.UpdatePullRequestReviewCommentInput{
		PullRequestReviewCommentID: req.GetCommentId(),
		Body:                       githubv4.String(req.GetNewBody()),
	}
	err = graphqlClient.Mutate(ctx, &m, input, nil)
	if err != nil {
		return nil, err
	}
	return &ghpb.UpdateGithubPullRequestCommentResponse{}, nil
}

func (a *GitHubApp) DeleteGithubPullRequestComment(ctx context.Context, req *ghpb.DeleteGithubPullRequestCommentRequest) (*ghpb.DeleteGithubPullRequestCommentResponse, error) {
	if !*enableReviewMutates {
		return nil, status.UnimplementedError("Not implemented")
	}
	graphqlClient, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}
	var m struct {
		DeletePullRequestReviewComment struct {
			ClientMutationId string
		} `graphql:"deletePullRequestReviewComment(input: $input)"`
	}
	input := githubv4.DeletePullRequestReviewCommentInput{
		ID: githubv4.ID(req.GetCommentId()),
	}
	err = graphqlClient.Mutate(ctx, &m, input, nil)
	if err != nil {
		return nil, err
	}
	return &ghpb.DeleteGithubPullRequestCommentResponse{}, nil
}

type prUser struct {
	Login string
}

type bot struct {
	Login string
}

type actor struct {
	Typename string `graphql:"__typename"`
	User     prUser `graphql:"... on User"`
	Bot      bot    `graphql:"... on Bot"`
}

type timelineItem struct {
	Typename             string `graphql:"__typename"`
	ReviewRequestedEvent struct {
		RequestedReviewer actor
	} `graphql:"... on ReviewRequestedEvent"`
	ReviewRequestRemovedEvent struct {
		RequestedReviewer actor
	} `graphql:"... on ReviewRequestRemovedEvent"`
	ReviewDismissedEvent struct {
		Actor actor
	} `graphql:"... on ReviewDismissedEvent"`
	PullRequestReview struct {
		Author actor
		State  string
	} `graphql:"... on PullRequestReview"`
}

type reviewComment struct {
	comment
	OriginalCommit struct {
		Oid string
	}
	PullRequestReview struct {
		Id string
	}
	ReplyTo struct {
		Id string
	}
}

type comment struct {
	Author    actor
	BodyHTML  string `graphql:"bodyHTML"`
	BodyText  string
	CreatedAt time.Time
	Id        string
}

type commentLink struct {
	Id string
}

type reviewThread struct {
	Id                string
	Path              string
	IsResolved        bool
	DiffSide          githubv4.DiffSide
	OriginalLine      int
	Line              int
	StartDiffSide     githubv4.DiffSide
	OriginalStartLine int
	StartLine         int
	Comments          struct {
		Nodes []reviewComment
	} `graphql:"comments(first: 100)"`
}

type reviewRequest struct {
	RequestedReviewer actor
}

type review struct {
	Id        string
	BodyText  string
	CreatedAt time.Time
	Author    actor
	State     string
}

type file struct {
	Path              string
	Patch             string
	Additions         int
	Deletions         int
	ChangeType        githubv4.PatchStatus
	ViewerViewedState githubv4.FileViewedState
}

type combinedContext struct {
	Typename      string `graphql:"__typename"`
	StatusContext struct {
		Context     string
		CreatedAt   time.Time
		Description string
		TargetUrl   string
		State       githubv4.StatusState
	} `graphql:"... on StatusContext"`
}

type checkSuite struct {
	App struct {
		Id   string
		Name string
	}
	CheckRuns struct {
		TotalCount int
	}
	CreatedAt  time.Time
	Status     githubv4.CheckStatusState
	Conclusion githubv4.CheckConclusionState
	Url        string
}

type prCommit struct {
	Commit struct {
		Oid         string
		CheckSuites struct {
			Nodes []checkSuite
		} `graphql:"checkSuites(last: 100)"`
		Status struct {
			CombinedContexts struct {
				Nodes []combinedContext
			} `graphql:"combinedContexts(last: 100)"`
		}
	}
}

type prDetailsQuery struct {
	Viewer struct {
		Login string
	}
	Repository struct {
		PullRequest struct {
			Title          string
			TitleHTML      string `graphql:"titleHTML"`
			Body           string
			BodyHTML       string `graphql:"bodyHTML"`
			Author         actor
			CreatedAt      time.Time
			Id             string
			UpdatedAt      time.Time
			Mergeable      githubv4.MergeableState
			ReviewDecision githubv4.PullRequestReviewDecision
			Merged         bool
			URL            string `graphql:"url"`
			ChecksURL      string `graphql:"checksUrl"`
			HeadRefName    string
			TimelineItems  struct {
				Nodes []timelineItem
			} `graphql:"timelineItems(first: 100, itemTypes: [REVIEW_REQUESTED_EVENT, REVIEW_REQUEST_REMOVED_EVENT, REVIEW_DISMISSED_EVENT, PULL_REQUEST_REVIEW])"`
			ReviewRequests struct {
				Nodes []reviewRequest
			} `graphql:"reviewRequests(first: 100)"`
			ReviewThreads struct {
				Nodes []reviewThread
			} `graphql:"reviewThreads(first: 100)"`
			Reviews struct {
				Nodes []review
			} `graphql:"reviews(first: 100)"`
			Comments struct {
				Nodes []comment
			} `graphql:"comments(first: 100)"`
			Commits struct {
				Nodes []prCommit
			} `graphql:"commits(first: 100)"`
			// TODO(jdhollen): Fetch files here
		} `graphql:"pullRequest(number: $pullNumber)"`
	} `graphql:"repository(owner: $repoOwner, name: $repoName)"`
}

func getLogin(a *actor) string {
	switch a.Typename {
	case "Bot":
		return a.Bot.Login
	case "User":
		return a.User.Login
	}
	return ""
}

func (a *GitHubApp) SendGithubPullRequestReview(ctx context.Context, req *ghpb.SendGithubPullRequestReviewRequest) (*ghpb.SendGithubPullRequestReviewResponse, error) {
	if !*enableReviewMutates {
		return nil, status.UnimplementedError("Not implemented")
	}
	graphqlClient, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}

	reviewID := req.GetReviewId()
	prID := req.GetPullRequestId()
	if reviewID == "" && prID == "" {
		return nil, status.InvalidArgumentError("You must specify a pull request or Review ID.")
	}

	replyBody := req.GetBody()
	if replyBody == "" {
		if req.GetApprove() {
			replyBody = "LGTM, Approval"
		} else {
			replyBody = "See comments"
		}
	}

	event := githubv4.PullRequestReviewEventComment
	if req.GetApprove() {
		event = githubv4.PullRequestReviewEventApprove
	}

	if reviewID == "" && prID != "" {
		var m struct {
			AddPullRequestReview struct {
				PullRequestReview struct {
					Id   string
					Body string
				}
			} `graphql:"addPullRequestReview(input: $input)"`
		}
		input := githubv4.AddPullRequestReviewInput{
			PullRequestID: req.GetPullRequestId(),
			Body:          githubv4.NewString(githubv4.String(replyBody)),
			Event:         &event,
		}
		err := graphqlClient.Mutate(ctx, &m, input, nil)
		if err != nil {
			return nil, err
		}
		return &ghpb.SendGithubPullRequestReviewResponse{}, nil
	}

	var m struct {
		SubmitPullRequestReview struct {
			ClientMutationId string
		} `graphql:"submitPullRequestReview(input: $input)"`
	}

	input := githubv4.SubmitPullRequestReviewInput{
		PullRequestReviewID: githubv4.NewID(reviewID),
		Body:                githubv4.NewString(githubv4.String(replyBody)),
		Event:               event,
	}
	err = graphqlClient.Mutate(ctx, &m, input, nil)
	if err != nil {
		return nil, err
	}
	return &ghpb.SendGithubPullRequestReviewResponse{}, nil
}

func isBot(a *actor) bool {
	return a.Typename == "Bot"
}

func checkStateToStatus(s githubv4.CheckStatusState, c githubv4.CheckConclusionState) ghpb.ActionStatusState {
	if s != githubv4.CheckStatusStateCompleted {
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_PENDING
	}
	switch c {
	case githubv4.CheckConclusionStateFailure:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.CheckConclusionStateActionRequired:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.CheckConclusionStateCancelled:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.CheckConclusionStateTimedOut:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.CheckConclusionStateStartupFailure:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.CheckConclusionStateStale:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL
	case githubv4.CheckConclusionStateNeutral:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL
	case githubv4.CheckConclusionStateSkipped:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL
	case githubv4.CheckConclusionStateSuccess:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_SUCCESS
	}
	return ghpb.ActionStatusState_ACTION_STATUS_STATE_UNKNOWN
}

func statusStateToStatus(s githubv4.StatusState) ghpb.ActionStatusState {
	switch s {
	case githubv4.StatusStateError:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.StatusStateFailure:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	case githubv4.StatusStateExpected:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL
	case githubv4.StatusStateSuccess:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_SUCCESS
	case githubv4.StatusStatePending:
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_PENDING
	}
	return ghpb.ActionStatusState_ACTION_STATUS_STATE_UNKNOWN
}

func combineStatuses(a ghpb.ActionStatusState, b ghpb.ActionStatusState) ghpb.ActionStatusState {
	if a == ghpb.ActionStatusState_ACTION_STATUS_STATE_PENDING || b == ghpb.ActionStatusState_ACTION_STATUS_STATE_PENDING {
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_PENDING
	} else if a == ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE || b == ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE {
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_FAILURE
	} else if a == ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL || b == ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL {
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_NEUTRAL
	} else if a == ghpb.ActionStatusState_ACTION_STATUS_STATE_SUCCESS || b == ghpb.ActionStatusState_ACTION_STATUS_STATE_SUCCESS {
		return ghpb.ActionStatusState_ACTION_STATUS_STATE_SUCCESS
	}
	return ghpb.ActionStatusState_ACTION_STATUS_STATE_UNKNOWN
}

type combinedChecksForApp struct {
	Name   string
	Count  int
	Status ghpb.ActionStatusState
	URL    string
}

func (a *GitHubApp) GetGithubPullRequestDetails(ctx context.Context, req *ghpb.GetGithubPullRequestDetailsRequest) (*ghpb.GetGithubPullRequestDetailsResponse, error) {
	client, err := a.getGithubClient(ctx)
	if err != nil {
		return nil, err
	}

	gqClient, err := a.getGithubGraphQLClient(ctx)
	if err != nil {
		return nil, err
	}
	eg, gCtx := errgroup.WithContext(ctx)

	graph := &prDetailsQuery{}
	vars := map[string]interface{}{
		"repoOwner":  githubv4.String(req.GetOwner()),
		"repoName":   githubv4.String(req.GetRepo()),
		"pullNumber": githubv4.Int(req.GetPull()),
	}
	eg.Go(func() error {
		if err := gqClient.Query(gCtx, &graph, vars); err != nil {
			return err
		}
		return nil
	})

	var files []*github.CommitFile
	eg.Go(func() error {
		f, err := a.cachedFiles(gCtx, client, req.Owner, req.Repo, int(req.Pull))
		if err != nil {
			return err
		}
		files = *f
		return nil
	})

	if err := eg.Wait(); err != nil {
		return nil, err
	}

	pr := graph.Repository.PullRequest

	outputComments := make([]*ghpb.Comment, 0)
	draftReviewId := ""
	for _, r := range pr.Reviews.Nodes {
		if r.State == "PENDING" {
			draftReviewId = r.Id
		}
	}

	fileCommentCount := make(map[string]int64)
	for _, thread := range pr.ReviewThreads.Nodes {
		for _, c := range thread.Comments.Nodes {
			comment := &ghpb.Comment{}
			comment.Id = c.Id
			comment.Body = c.BodyText
			comment.Path = thread.Path
			comment.CommitSha = c.OriginalCommit.Oid
			comment.ReviewId = c.PullRequestReview.Id
			comment.CreatedAtUsec = c.CreatedAt.UnixMicro()
			comment.ParentCommentId = c.ReplyTo.Id
			comment.ThreadId = thread.Id
			comment.IsResolved = thread.IsResolved

			position := &ghpb.CommentPosition{}
			position.StartLine = int64(thread.OriginalStartLine)
			position.EndLine = int64(thread.OriginalLine)

			if thread.DiffSide == "LEFT" {
				position.Side = ghpb.CommentSide_LEFT_SIDE
			} else {
				position.Side = ghpb.CommentSide_RIGHT_SIDE
			}
			comment.Position = position

			commenter := &ghpb.ReviewUser{}
			commenter.Login = getLogin(&c.Author)
			commenter.Bot = isBot(&c.Author)
			comment.Commenter = commenter

			if thread.Path != "" {
				fileCommentCount[thread.Path]++
			}
			outputComments = append(outputComments, comment)
		}
	}

	notExplicitlyRemoved := map[string]struct{}{}
	approved := map[string]struct{}{}

	for _, e := range pr.TimelineItems.Nodes {
		switch eventType := e.Typename; eventType {
		case "ReviewRequestedEvent":
			notExplicitlyRemoved[getLogin(&e.ReviewRequestedEvent.RequestedReviewer)] = struct{}{}
		case "ReviewRequestRemovedEvent":
			delete(notExplicitlyRemoved, getLogin(&e.ReviewRequestRemovedEvent.RequestedReviewer))
		case "PullRequestReview":
			if e.PullRequestReview.State == "APPROVED" {
				approved[getLogin(&e.PullRequestReview.Author)] = struct{}{}
			} else if e.PullRequestReview.State == "CHANGES_REQUESTED" {
				delete(approved, getLogin(&e.PullRequestReview.Author))
			}
		}
	}

	activeReviewers := map[string]struct{}{}

	reviewers := make([]*ghpb.Reviewer, 0)
	for _, r := range pr.ReviewRequests.Nodes {
		activeReviewers[getLogin(&r.RequestedReviewer)] = struct{}{}
	}

	for r := range notExplicitlyRemoved {
		reviewer := &ghpb.Reviewer{}
		reviewer.Login = r
		if _, ok := activeReviewers[r]; ok {
			reviewer.Attention = true
		}
		if _, ok := approved[r]; ok {
			reviewer.Approved = true
		}
		reviewers = append(reviewers, reviewer)
	}

	fileSummaries := make([]*ghpb.FileSummary, 0)
	for _, f := range files {
		summary := &ghpb.FileSummary{}
		summary.Name = f.GetFilename()
		summary.Additions = int64(f.GetAdditions())
		summary.Deletions = int64(f.GetDeletions())
		summary.Patch = f.GetPatch()
		url, err := url.Parse(f.GetContentsURL())
		if err != nil {
			return nil, err
		}
		ref := url.Query().Get("ref")
		if ref == "" {
			return nil, status.InternalErrorf("Couldn't find SHA for file.")
		}
		summary.CommitSha = ref
		summary.Comments = fileCommentCount[f.GetFilename()]
		fileSummaries = append(fileSummaries, summary)
	}

	statusTrackingMap := make(map[string]*combinedContext, 0)
	checkTrackingMap := make(map[string]*combinedChecksForApp, 0)
	// TODO(jdhollen): show previous checks + status as "outdated" when a new
	// run hasn't fired instead of hiding them.
	if len(pr.Commits.Nodes) > 0 {
		lastCommit := pr.Commits.Nodes[len(pr.Commits.Nodes)-1]
		for _, s := range lastCommit.Commit.Status.CombinedContexts.Nodes {
			if s.Typename == "StatusContext" {
				if prev, ok := statusTrackingMap[s.StatusContext.Context]; ok && s.StatusContext.CreatedAt.UnixMicro() < prev.StatusContext.CreatedAt.UnixMicro() {
					continue
				}
				s2 := s
				statusTrackingMap[s.StatusContext.Context] = &s2
			}
		}
		// TODO(jdhollen): Request access to Workflows in Github App, and show
		// GH workflow names instead of just the app name.
		for _, s := range lastCommit.Commit.CheckSuites.Nodes {
			// GitHub creates CheckSuites for all apps that listen for
			// CheckSuite hooks, and can't know if they'll ever create a
			// CheckRun--let's just show the ones that did.
			if s.CheckRuns.TotalCount == 0 {
				continue
			}
			name := s.App.Id + s.App.Name
			v, ok := checkTrackingMap[name]
			if !ok {
				v = &combinedChecksForApp{
					Count:  1,
					Name:   s.App.Name,
					Status: checkStateToStatus(s.Status, s.Conclusion),
					URL:    pr.ChecksURL,
				}
			} else {
				v.Count = v.Count + 1
				v.Status = combineStatuses(v.Status, checkStateToStatus(s.Status, s.Conclusion))
			}
			checkTrackingMap[name] = v
		}
	}
	actionStatuses := make([]*ghpb.ActionStatus, 0)
	for _, s := range statusTrackingMap {
		status := &ghpb.ActionStatus{}
		status.Name = s.StatusContext.Context
		status.Status = statusStateToStatus(s.StatusContext.State)
		status.Url = s.StatusContext.TargetUrl
		actionStatuses = append(actionStatuses, status)
	}
	for _, s := range checkTrackingMap {
		status := &ghpb.ActionStatus{}
		status.Name = fmt.Sprintf("%s (%d actions)", s.Name, s.Count)
		status.Status = s.Status
		status.Url = s.URL
		actionStatuses = append(actionStatuses, status)
	}
	slices.SortFunc(actionStatuses, func(a, b *ghpb.ActionStatus) int {
		return strings.Compare(a.Name, b.Name)
	})

	resp := &ghpb.GetGithubPullRequestDetailsResponse{
		Owner:          req.Owner,
		Repo:           req.Repo,
		Pull:           req.Pull,
		PullId:         pr.Id,
		Title:          pr.Title,
		Body:           pr.Body,
		Author:         getLogin(&pr.Author),
		CreatedAtUsec:  pr.CreatedAt.UnixMicro(),
		UpdatedAtUsec:  pr.UpdatedAt.UnixMicro(),
		Branch:         pr.HeadRefName,
		Reviewers:      reviewers,
		Files:          fileSummaries,
		ActionStatuses: actionStatuses,
		Comments:       outputComments,
		// TODO(jdhollen): Switch to MergeStateStatus when it's stable. https://docs.github.com/en/graphql/reference/enums#mergestatestatus
		Mergeable:     pr.Mergeable == "MERGEABLE" && pr.ReviewDecision == githubv4.PullRequestReviewDecisionApproved,
		Submitted:     pr.Merged,
		GithubUrl:     pr.URL,
		DraftReviewId: draftReviewId,
		ViewerLogin:   graph.Viewer.Login,
	}

	return resp, nil
}

func (a *GitHubApp) getIncomingAndOutgoingPRs(ctx context.Context, username string, client *github.Client) (*github.IssuesSearchResult, *github.IssuesSearchResult, *github.User, error) {
	var incoming *github.IssuesSearchResult
	var outgoing *github.IssuesSearchResult
	var user *github.User
	eg, gCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		r, err := a.cachedSearch(gCtx, client, fmt.Sprintf("is:open is:pr user-review-requested:%s archived:false draft:false", username))
		if err != nil {
			return err
		}
		incoming = r
		return nil
	})
	eg.Go(func() error {
		r, err := a.cachedSearch(gCtx, client, fmt.Sprintf("is:open is:pr author:%s archived:false draft:false", username))
		if err != nil {
			return err
		}
		outgoing = r
		return nil
	})
	eg.Go(func() error {
		u, err := a.cachedUser(gCtx, client)
		if err != nil {
			return err
		}
		user = u
		return nil
	})
	if err := eg.Wait(); err != nil {
		return nil, nil, nil, err
	}
	return incoming, outgoing, user, nil
}

func (a *GitHubApp) populatePRMetadata(ctx context.Context, client *github.Client, prIssues []*github.Issue, requestedUser string) (map[string]*ghpb.PullRequest, error) {
	prsMu := &sync.Mutex{}
	prs := make(map[string]*ghpb.PullRequest, len(prIssues))
	eg, gCtx := errgroup.WithContext(ctx)
	for _, i := range prIssues {
		i := i
		prsMu.Lock()
		prs[i.GetNodeID()] = issueToPullRequestProto(i)
		prsMu.Unlock()
		urlParts := strings.Split(*i.URL, "/")
		if len(urlParts) < 6 {
			return nil, status.FailedPreconditionErrorf("invalid github url: %q", *i.URL)
		}
		eg.Go(func() error {
			pr, err := a.cachedPullRequests(gCtx, client, urlParts[4], urlParts[5], i.GetNumber(), i.GetUpdatedAt().Time)
			if err != nil {
				return err
			}
			prsMu.Lock()
			p := prs[i.GetNodeID()]
			p.Additions = int64(pr.GetAdditions())
			p.Deletions = int64(pr.GetDeletions())
			p.ChangedFiles = int64(pr.GetChangedFiles())
			p.Owner = urlParts[4]
			p.Repo = urlParts[5]
			for _, r := range pr.RequestedReviewers {
				review, ok := prs[i.GetNodeID()].Reviews[r.GetLogin()]
				if !ok {
					review = &ghpb.Review{}
					prs[i.GetNodeID()].Reviews[r.GetLogin()] = review
				}
				review.Requested = true
				if r.GetLogin() == requestedUser {
					review.IsCurrentUser = true
				}
			}
			prsMu.Unlock()
			return nil
		})
		eg.Go(func() error {
			reviews, err := a.cachedReviews(gCtx, client, urlParts[4], urlParts[5], i.GetNumber(), i.GetUpdatedAt().Time)
			if err != nil {
				return err
			}
			for _, r := range *reviews {
				prsMu.Lock()
				review, ok := prs[i.GetNodeID()].Reviews[r.GetUser().GetLogin()]
				if !ok {
					review = &ghpb.Review{}
					prs[i.GetNodeID()].Reviews[r.GetUser().GetLogin()] = review
				}
				review.Status = strings.ToLower(r.GetState())
				review.SubmittedAtUsec = r.GetSubmittedAt().UnixMicro()
				if r.GetUser().GetLogin() == requestedUser {
					review.IsCurrentUser = true
				}
				prsMu.Unlock()
			}
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}
	return prs, nil
}

func (a *GitHubApp) cachedUser(ctx context.Context, client *github.Client) (*github.User, error) {
	key := "user"
	pr, err := a.cached(ctx, key, &github.User{}, time.Hour*24, func() (any, any, error) {
		return client.Users.Get(ctx, "")
	})
	if err != nil {
		return nil, err
	}
	return pr.(*github.User), nil
}

func (a *GitHubApp) cachedSearch(ctx context.Context, client *github.Client, query string) (*github.IssuesSearchResult, error) {
	key := fmt.Sprintf("search/%s", query)
	pr, err := a.cached(ctx, key, &github.IssuesSearchResult{}, time.Second*5, func() (any, any, error) {
		return client.Search.Issues(ctx, query, &github.SearchOptions{ListOptions: github.ListOptions{PerPage: 100}})
	})
	if err != nil {
		return nil, err
	}
	return pr.(*github.IssuesSearchResult), nil
}

func (a *GitHubApp) cachedIssue(ctx context.Context, client *github.Client, owner string, repo string, issue int) (*github.Issue, error) {
	key := fmt.Sprintf("issue/%s/%s/%d", owner, repo, issue)
	value, err := a.cached(ctx, key, &github.Issue{}, time.Second*30, func() (any, any, error) {
		return client.Issues.Get(ctx, owner, repo, issue)
	})
	if err != nil {
		return nil, err
	}
	return value.(*github.Issue), nil
}

func (a *GitHubApp) cachedStatuses(ctx context.Context, client *github.Client, owner string, repo string, ref string) (*github.CombinedStatus, error) {
	key := fmt.Sprintf("statuses/%s/%s/%s", owner, repo, ref)
	value, err := a.cached(ctx, key, &github.CombinedStatus{}, time.Second*30, func() (any, any, error) {
		return client.Repositories.GetCombinedStatus(ctx, owner, repo, ref, &github.ListOptions{PerPage: 100})
	})
	if err != nil {
		return nil, err
	}
	return value.(*github.CombinedStatus), nil
}

func (a *GitHubApp) cachedTimeline(ctx context.Context, client *github.Client, owner, repo string, number int) (*[]*github.Timeline, error) {
	key := fmt.Sprintf("timeline/%s/%s/%d", owner, repo, number)
	pr, err := a.cached(ctx, key, &[]*github.Timeline{}, time.Second*30, func() (any, any, error) {
		rev, res, err := client.Issues.ListIssueTimeline(ctx, owner, repo, number, &github.ListOptions{PerPage: 100})
		return &rev, res, err
	})
	if err != nil {
		return nil, err
	}
	return pr.(*[]*github.Timeline), nil
}

func (a *GitHubApp) cachedFiles(ctx context.Context, client *github.Client, owner, repo string, number int) (*[]*github.CommitFile, error) {
	key := fmt.Sprintf("files/%s/%s/%d", owner, repo, number)
	pr, err := a.cached(ctx, key, &[]*github.CommitFile{}, time.Second*30, func() (any, any, error) {
		rev, res, err := client.PullRequests.ListFiles(ctx, owner, repo, number, &github.ListOptions{PerPage: 100})
		return &rev, res, err
	})
	if err != nil {
		return nil, err
	}
	return pr.(*[]*github.CommitFile), nil
}

func (a *GitHubApp) cachedPullRequests(ctx context.Context, client *github.Client, owner, repo string, number int, updatedAt time.Time) (*github.PullRequest, error) {
	key := fmt.Sprintf("pr/%s/%s/%d/%d", owner, repo, number, updatedAt.Unix())

	pr, err := a.cached(ctx, key, &github.PullRequest{}, time.Hour*24, func() (any, any, error) {
		return client.PullRequests.Get(ctx, owner, repo, number)
	})
	if err != nil {
		return nil, err
	}
	return pr.(*github.PullRequest), nil
}

func (a *GitHubApp) cachedReviews(ctx context.Context, client *github.Client, owner, repo string, number int, updatedAt time.Time) (*[]*github.PullRequestReview, error) {
	key := fmt.Sprintf("pr-reviews/%s/%s/%d/%d", owner, repo, number, updatedAt.Unix())
	pr, err := a.cached(ctx, key, &[]*github.PullRequestReview{}, time.Hour*24, func() (any, any, error) {
		rev, res, err := client.PullRequests.ListReviews(ctx, owner, repo, number, &github.ListOptions{PerPage: 100})
		return &rev, res, err
	})
	if err != nil {
		return nil, err
	}
	return pr.(*[]*github.PullRequestReview), nil
}

type fetchFunction func() (any, any, error)

func (a *GitHubApp) cached(ctx context.Context, key string, v any, exp time.Duration, fetch fetchFunction) (any, error) {
	if a.env.GetDefaultRedisClient() == nil {
		pr, _, err := fetch()
		return pr, err
	}
	u, err := a.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	key = fmt.Sprintf("githubapp/0/%s/%s", u.GetUserID(), key)
	if cachedVal, err := a.env.GetDefaultRedisClient().Get(ctx, key).Result(); err == nil {
		if err := json.Unmarshal([]byte(cachedVal), &v); err == nil {
			log.Debugf("got cached github result for key: %s", key)
			return v, nil
		} else {
			log.Errorf("error unmarshalling cached github redis result for key %s: %s", key, err)
		}
	}
	log.Debugf("making github request for key: %s", key)
	pr, _, err := fetch()
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(pr)
	if err != nil {
		return nil, err
	}
	a.env.GetDefaultRedisClient().Set(ctx, key, string(b), exp)
	return pr, nil
}

func issueToPullRequestProto(i *github.Issue) *ghpb.PullRequest {
	return &ghpb.PullRequest{
		Number:        uint64(i.GetNumber()),
		Title:         i.GetTitle(),
		Body:          i.GetBody(),
		Author:        i.GetUser().GetLogin(),
		Url:           i.GetHTMLURL(),
		UpdatedAtUsec: i.GetUpdatedAt().UnixMicro(),
		Reviews:       map[string]*ghpb.Review{},
	}
}
