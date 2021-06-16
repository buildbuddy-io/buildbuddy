package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/bitbucket"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/oauth2"
	"google.golang.org/genproto/googleapis/longrunning"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	githubapi "github.com/google/go-github/github"
	guuid "github.com/google/uuid"
)

var (
	workflowURLMatcher   = regexp.MustCompile(`^.*/webhooks/workflow/(?P<instance_name>.*)$`)
	buildbuddyCIUserName = "buildbuddy"
	buildbuddyCIHostName = "buildbuddy-ci-runner"
)

// getWebhookID returns a string that can be used to uniquely identify a webhook.
func generateWebhookID() (string, error) {
	u, err := guuid.NewRandom()
	if err != nil {
		return "", err
	}
	return strings.ToLower(u.String()), nil
}

func instanceName(wd *webhook_data.WebhookData) string {
	// Note, we use a unique remote instance per repo URL, so that forked repos
	// don't share the same cache as the base repo.
	return fmt.Sprintf("%x", sha256.Sum256([]byte(wd.RepoURL)))
}

type workflowService struct {
	env environment.Env
}

func NewWorkflowService(env environment.Env) *workflowService {
	return &workflowService{
		env: env,
	}
}

// getWebhookURL takes a webhookID and returns a fully qualified URL, on this
// server, where the specified webhook can be called.
func (ws *workflowService) getWebhookURL(webhookID string) (string, error) {
	base, err := url.Parse(ws.env.GetConfigurator().GetAppBuildBuddyURL())
	if err != nil {
		return "", err
	}
	u, err := url.Parse(fmt.Sprintf("/webhooks/workflow/%s", webhookID))
	if err != nil {
		return "", err
	}
	wu := base.ResolveReference(u)
	return wu.String(), nil
}

// testRepo will call "git ls-repo repoURL" to verify that the repo is valid and
// the accessToken (if non-empty) works.
func (ws *workflowService) testRepo(ctx context.Context, repoURL, username, accessToken string) error {
	rdl := ws.env.GetRepoDownloader()
	if rdl != nil {
		return rdl.TestRepoAccess(ctx, repoURL, username, accessToken)
	}
	return nil
}

func (ws *workflowService) checkPreconditions(ctx context.Context) error {
	if ws.env.GetDBHandle() == nil {
		return status.FailedPreconditionError("database not configured")
	}
	if ws.env.GetAuthenticator() == nil {
		return status.FailedPreconditionError("anonymous workflow access is not supported")
	}
	if _, err := ws.env.GetAuthenticator().AuthenticatedUser(ctx); err != nil {
		return err
	}

	return nil
}
func (ws *workflowService) CreateWorkflow(ctx context.Context, req *wfpb.CreateWorkflowRequest) (*wfpb.CreateWorkflowResponse, error) {
	// Validate the request.
	if err := ws.checkPreconditions(ctx); err != nil {
		return nil, err
	}
	repoReq := req.GetGitRepo()
	if repoReq.GetRepoUrl() == "" {
		return nil, status.InvalidArgumentError("A repo URL is required to create a new workflow.")
	}

	// Ensure the request is authenticated so some user can own this workflow.
	var permissions *perms.UserGroupPerm
	groupID := req.GetRequestContext().GetGroupId()
	if groupID == "" {
		return nil, status.InvalidArgumentError("Request context is missing group ID.")
	}
	if err := perms.AuthorizeGroupAccess(ctx, ws.env, groupID); err != nil {
		return nil, err
	}
	permissions = perms.GroupAuthPermissions(groupID)

	// Do a quick check to see if this is a valid repo that we can actually access.
	repoURL := repoReq.GetRepoUrl()
	username := repoReq.GetUsername()
	accessToken := repoReq.GetAccessToken()

	// If no access token is provided explicitly, try getting the token from the
	// group.
	if accessToken == "" && isGitHubURL(repoURL) {
		token, err := ws.gitHubTokenForAuthorizedGroup(ctx, req.GetRequestContext())
		if err != nil {
			return nil, err
		}
		accessToken = token
	}

	if err := ws.testRepo(ctx, repoURL, username, accessToken); err != nil {
		return nil, status.UnavailableErrorf("Repo %q is unavailable: %s", repoURL, err.Error())
	}

	webhookID, err := generateWebhookID()
	if err != nil {
		return nil, status.InternalError(err.Error())
	}
	webhookURL, err := ws.getWebhookURL(webhookID)
	if err != nil {
		return nil, status.InternalError(err.Error())
	}

	rsp := &wfpb.CreateWorkflowResponse{}

	gitHubWebhookID := int64(0)
	if isGitHubURL(repoURL) {
		if gitHubWebhookID, err = github.RegisterWebhook(ctx, accessToken, repoURL, webhookURL); err != nil {
			return nil, status.WrapError(err, "Failed to register webhook to GitHub")
		}
		rsp.WebhookRegistered = true
	}

	err = ws.env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
		workflowID, err := tables.PrimaryKeyForTable("Workflows")
		if err != nil {
			return status.InternalError(err.Error())
		}
		rsp.Id = workflowID
		rsp.WebhookUrl = webhookURL
		wf := &tables.Workflow{
			WorkflowID:      workflowID,
			UserID:          permissions.UserID,
			GroupID:         permissions.GroupID,
			Perms:           permissions.Perms,
			Name:            req.GetName(),
			RepoURL:         repoURL,
			Username:        username,
			AccessToken:     accessToken,
			WebhookID:       webhookID,
			GitHubWebhookID: gitHubWebhookID,
		}
		return tx.Create(wf).Error
	})
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (ws *workflowService) DeleteWorkflow(ctx context.Context, req *wfpb.DeleteWorkflowRequest) (*wfpb.DeleteWorkflowResponse, error) {
	if err := ws.checkPreconditions(ctx); err != nil {
		return nil, err
	}
	workflowID := req.GetId()
	if workflowID == "" {
		return nil, status.InvalidArgumentError("An ID is required to delete a workflow.")
	}
	authenticatedUser, err := ws.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	var wf tables.Workflow
	err = ws.env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
		if err := tx.Raw(`SELECT user_id, group_id, perms, access_token, repo_url, github_webhook_id FROM Workflows WHERE workflow_id = ?`, workflowID).Take(&wf).Error; err != nil {
			return err
		}
		acl := perms.ToACLProto(&uidpb.UserId{Id: wf.UserID}, wf.GroupID, wf.Perms)
		if err := perms.AuthorizeWrite(&authenticatedUser, acl); err != nil {
			return err
		}
		return tx.Exec(`DELETE FROM Workflows WHERE workflow_id = ?`, req.GetId()).Error
	})
	if err != nil {
		return nil, err
	}
	if wf.GitHubWebhookID != 0 {
		if err := github.UnregisterWebhook(ctx, wf.AccessToken, wf.RepoURL, wf.GitHubWebhookID); err != nil {
			return nil, status.WrapError(err, "Failed to unregister the webhook from GitHub. The hook may need to be removed manually via the repo settings.")
		}
	}
	return &wfpb.DeleteWorkflowResponse{}, nil
}

func (ws *workflowService) GetWorkflows(ctx context.Context, req *wfpb.GetWorkflowsRequest) (*wfpb.GetWorkflowsResponse, error) {
	if err := ws.checkPreconditions(ctx); err != nil {
		return nil, err
	}

	rsp := &wfpb.GetWorkflowsResponse{}
	q := query_builder.NewQuery(`SELECT workflow_id, name, repo_url, webhook_id FROM Workflows`)
	// Adds user / permissions check.
	if err := perms.AddPermissionsCheckToQuery(ctx, ws.env, q); err != nil {
		return nil, err
	}
	q.SetOrderBy("created_at_usec" /*ascending=*/, true)
	qStr, qArgs := q.Build()
	err := ws.env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
		rows, err := tx.Raw(qStr, qArgs...).Rows()
		if err != nil {
			return err
		}
		defer rows.Close()

		rsp.Workflow = make([]*wfpb.GetWorkflowsResponse_Workflow, 0)
		for rows.Next() {
			var tw tables.Workflow
			if err := tx.ScanRows(rows, &tw); err != nil {
				return err
			}
			u, err := ws.getWebhookURL(tw.WebhookID)
			if err != nil {
				return err
			}
			rsp.Workflow = append(rsp.Workflow, &wfpb.GetWorkflowsResponse_Workflow{
				Id:         tw.WorkflowID,
				Name:       tw.Name,
				RepoUrl:    tw.RepoURL,
				WebhookUrl: u,
			})
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rsp, nil
}

func (ws *workflowService) ExecuteWorkflow(ctx context.Context, req *wfpb.ExecuteWorkflowRequest) (*wfpb.ExecuteWorkflowResponse, error) {
	// Validate req
	if req.GetWorkflowId() == "" {
		return nil, status.InvalidArgumentError("Missing workflow_id")
	}
	if req.GetCommitSha() == "" {
		return nil, status.InvalidArgumentError("Missing commit_sha")
	}
	if req.GetBranch() == "" {
		return nil, status.InvalidArgumentError("Missing branch")
	}
	if req.GetActionName() == "" {
		return nil, status.InvalidArgumentError("Missing action_name")
	}

	// Authenticate
	user, err := perms.AuthenticatedUser(ctx, ws.env)
	if err != nil {
		return nil, err
	}

	// Lookup workflow
	wf := &tables.Workflow{}
	err = ws.env.GetDBHandle().Raw(
		`SELECT group_id, repo_url, access_token, perms FROM Workflows WHERE workflow_id = ?`,
		req.GetWorkflowId(),
	).Take(wf).Error
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundError("Workflow not found")
		}
		return nil, status.InternalError(err.Error())
	}

	// Authorize workflow access
	wfACL := perms.ToACLProto(&uidpb.UserId{Id: wf.GroupID}, wf.GroupID, wf.Perms)
	if err := perms.AuthorizeRead(&user, wfACL); err != nil {
		return nil, err
	}

	// Execute
	isRepoPrivate := false
	// TODO: Support other Git providers. We default to false on those for now,
	// which means that we don't pass secrets even for private repos.
	if isGitHubURL(wf.RepoURL) {
		isRepoPrivate, err = github.IsRepoPrivate(ctx, wf.AccessToken, wf.RepoURL)
		if err != nil {
			return nil, status.WrapErrorf(err, "failed to determine whether repo is private")
		}
	}
	// TODO: Refactor to avoid using this WebhookData struct in the case of manual
	// workflow execution, since there are no webhooks involved when executing a
	// workflow manually.
	wd := &webhook_data.WebhookData{
		PushedBranch:  req.GetBranch(),
		TargetBranch:  req.GetBranch(),
		RepoURL:       wf.RepoURL,
		SHA:           req.GetCommitSha(),
		IsRepoPrivate: isRepoPrivate,
	}
	invocationUUID, err := guuid.NewRandom()
	if err != nil {
		return nil, err
	}
	invocationID := invocationUUID.String()
	extraCIRunnerArgs := []string{
		fmt.Sprintf("--action_name=%s", req.GetActionName()),
		fmt.Sprintf("--invocation_id=%s", invocationID),
	}

	executionID, err := ws.executeWorkflow(ctx, wf, wd, extraCIRunnerArgs)
	if err != nil {
		return nil, err
	}
	if err := ws.waitForWorkflowInvocationCreated(ctx, executionID, invocationID); err != nil {
		return nil, err
	}

	return &wfpb.ExecuteWorkflowResponse{InvocationId: invocationID}, nil
}

func (ws *workflowService) waitForWorkflowInvocationCreated(ctx context.Context, executionID, invocationID string) error {
	executionClient := ws.env.GetRemoteExecutionClient()
	if executionClient == nil {
		return status.UnimplementedError("Missing remote execution client.")
	}
	indb := ws.env.GetInvocationDB()

	errCh := make(chan error)
	opCh := make(chan *longrunning.Operation)

	waitStream, err := executionClient.WaitExecution(ctx, &repb.WaitExecutionRequest{
		Name: executionID,
	})
	if err != nil {
		return err
	}

	// Listen on operation stream in the background
	go func() {
		for {
			op, err := waitStream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			opCh <- op
		}
	}()

	stage := repb.ExecutionStage_UNKNOWN
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-errCh:
			return err
		case op := <-opCh:
			stage = operation.ExtractStage(op)
		case <-time.After(1 * time.Second):
			break
		}
		if stage == repb.ExecutionStage_EXECUTING || stage == repb.ExecutionStage_COMPLETED {
			_, err := indb.LookupInvocation(ctx, invocationID)
			if err == nil {
				return nil
			}
			if !db.IsRecordNotFound(err) {
				return err
			}
		}
		if stage == repb.ExecutionStage_COMPLETED {
			return status.InternalErrorf("Failed to create workflow invocation (execution ID: %s)", executionID)
		}
	}
}

func (ws *workflowService) GetRepos(ctx context.Context, req *wfpb.GetReposRequest) (*wfpb.GetReposResponse, error) {
	if req.GetGitProvider() == wfpb.GitProvider_UNKNOWN_GIT_PROVIDER {
		return nil, status.FailedPreconditionError("Unknown git provider")
	}
	token, err := ws.gitHubTokenForAuthorizedGroup(ctx, req.GetRequestContext())
	urls, err := listGitHubRepoURLs(ctx, token)
	if err != nil {
		return nil, status.UnknownErrorf("Failed to list GitHub repo URLs: %s", err)
	}
	res := &wfpb.GetReposResponse{}
	for _, url := range urls {
		res.Repo = append(res.Repo, &wfpb.Repo{Url: url})
	}
	return res, nil
}

func (ws *workflowService) gitHubTokenForAuthorizedGroup(ctx context.Context, reqCtx *ctxpb.RequestContext) (string, error) {
	_, err := perms.AuthenticatedUser(ctx, ws.env)
	if err != nil {
		return "", err
	}
	d := ws.env.GetUserDB()
	if d == nil {
		return "", status.FailedPreconditionError("Missing UserDB")
	}
	groupID := reqCtx.GetGroupId()
	if err := perms.AuthorizeGroupAccess(ctx, ws.env, groupID); err != nil {
		return "", err
	}
	g, err := d.GetGroupByID(ctx, groupID)
	if err != nil {
		return "", err
	}
	if g.GithubToken == "" {
		return "", status.FailedPreconditionError("The selected group does not have a GitHub account linked")
	}
	return g.GithubToken, nil
}

func listGitHubRepoURLs(ctx context.Context, accessToken string) ([]string, error) {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	tc := oauth2.NewClient(ctx, ts)
	client := githubapi.NewClient(tc)
	repos, _, err := client.Repositories.List(ctx, "", &githubapi.RepositoryListOptions{
		Sort: "updated",
	})
	if err != nil {
		// TODO: transform GH HTTP response to proper gRPC status code
		return nil, err
	}
	urls := []string{}
	for _, repo := range repos {
		if repo.HTMLURL == nil {
			continue
		}
		urls = append(urls, *repo.HTMLURL)
	}
	return urls, nil
}

func (ws *workflowService) readWorkflowForWebhook(ctx context.Context, webhookID string) (*tables.Workflow, error) {
	if ws.env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	if webhookID == "" {
		return nil, status.InvalidArgumentError("A webhook ID is required.")
	}
	tw := &tables.Workflow{}
	if err := ws.env.GetDBHandle().ReadRow(tw, `webhook_id = ?`, webhookID); err != nil {
		return nil, err
	}
	return tw, nil
}

// Creates an action that executes the CI runner for the given workflow and params.
// Returns the digest of the action as well as the invocation ID that the CI runner
// will assign to the workflow invocation.
func (ws *workflowService) createActionForWorkflow(ctx context.Context, wf *tables.Workflow, wd *webhook_data.WebhookData, ak *tables.APIKey, instanceName string, extraArgs []string) (*repb.Digest, error) {
	cache := ws.env.GetCache()
	if cache == nil {
		return nil, status.UnavailableError("No cache configured.")
	}

	runnerBinName := "buildbuddy_ci_runner"
	runnerBinFile, err := runnerBinaryFile()
	if err != nil {
		return nil, err
	}
	runnerBinDigest, err := cachetools.UploadBytesToCAS(ctx, cache, instanceName, runnerBinFile)
	if err != nil {
		return nil, err
	}
	dir := &repb.Directory{
		Files: []*repb.FileNode{
			{
				Name:         runnerBinName,
				Digest:       runnerBinDigest,
				IsExecutable: true,
			},
		},
	}
	inputRootDigest, err := cachetools.UploadProtoToCAS(ctx, cache, instanceName, dir)
	if err != nil {
		return nil, err
	}
	envVars := []*repb.Command_EnvironmentVariable{}
	if wd.IsTrusted() {
		envVars = append(envVars, []*repb.Command_EnvironmentVariable{
			{Name: "BUILDBUDDY_API_KEY", Value: ak.Value},
			{Name: "REPO_USER", Value: wf.Username},
			{Name: "REPO_TOKEN", Value: wf.AccessToken},
		}...)
	}
	conf := ws.env.GetConfigurator()
	cmd := &repb.Command{
		EnvironmentVariables: envVars,
		Arguments: append([]string{
			"./" + runnerBinName,
			"--bes_backend=" + conf.GetAppEventsAPIURL(),
			"--bes_results_url=" + conf.GetAppBuildBuddyURL() + "/invocation/",
			"--repo_url=" + wd.RepoURL,
			"--commit_sha=" + wd.SHA,
			"--branch=" + wd.PushedBranch,
			"--workflow_id=" + wf.WorkflowID,
			"--trigger_event=" + wd.EventName,
			"--trigger_branch=" + wd.TargetBranch,
		}, extraArgs...),
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "Pool", Value: ws.workflowsPoolName()},
				{Name: "container-image", Value: "docker://gcr.io/flame-public/buildbuddy-ci-runner:v1.7.1"},
				// Reuse the docker container for the CI runner across executions if
				// possible, and also keep the git repo around so it doesn't need to be
				// re-cloned each time.
				{Name: "recycle-runner", Value: "true"},
				{Name: "preserve-workspace", Value: "true"},
				// Pass the workflow ID to the executor so that it can try to assign
				// this task to a runner which has previously executed the workflow.
				{Name: "workflow-id", Value: wf.WorkflowID},
			},
		},
	}
	cmdDigest, err := cachetools.UploadProtoToCAS(ctx, cache, instanceName, cmd)
	if err != nil {
		return nil, err
	}
	action := &repb.Action{
		CommandDigest:   cmdDigest,
		InputRootDigest: inputRootDigest,
		DoNotCache:      true,
	}
	actionDigest, err := cachetools.UploadProtoToCAS(ctx, cache, instanceName, action)
	return actionDigest, err
}

func (ws *workflowService) workflowsPoolName() string {
	cfg := ws.env.GetConfigurator().GetRemoteExecutionConfig()
	if cfg != nil && cfg.WorkflowsPoolName != "" {
		return cfg.WorkflowsPoolName
	}
	return platform.DefaultPoolValue
}

func runnerBinaryFile() (*os.File, error) {
	path, err := bazelgo.Runfile("enterprise/server/cmd/ci_runner/ci_runner_/ci_runner")
	if err != nil {
		return nil, status.FailedPreconditionErrorf("could not find runner binary runfile: %s", err)
	}
	return os.Open(path)
}

func (ws *workflowService) apiKeyForWorkflow(ctx context.Context, wf *tables.Workflow) (*tables.APIKey, error) {
	q := query_builder.NewQuery(`SELECT * FROM APIKeys`)
	q.AddWhereClause("group_id = ?", wf.GroupID)
	qStr, qArgs := q.Build()
	k := &tables.APIKey{}
	if err := ws.env.GetDBHandle().Raw(qStr, qArgs...).Take(&k).Error; err != nil {
		return nil, err
	}
	return k, nil
}

func parseRequest(r *http.Request) (*webhook_data.WebhookData, error) {
	if r.Header.Get("X-Github-Event") != "" {
		return github.ParseRequest(r)
	}
	if r.Header.Get("X-Event-Key") != "" {
		return bitbucket.ParseRequest(r)
	}
	return nil, status.UnimplementedErrorf("failed to classify Git provider from webhook request: %+v", r)
}

func (ws *workflowService) checkStartWorkflowPreconditions(ctx context.Context) error {
	if ws.env.GetDBHandle() == nil {
		return status.FailedPreconditionError("database not configured")
	}
	if ws.env.GetAuthenticator() == nil {
		return status.FailedPreconditionError("anonymous workflow access is not supported")
	}
	if ws.env.GetRemoteExecutionService() == nil {
		return status.UnavailableError("Remote execution not configured.")
	}
	return nil
}

func (ws *workflowService) getBazelFlags(ak *tables.APIKey, instanceName string) ([]string, error) {
	flags := []string{
		"--build_metadata=USER=" + buildbuddyCIUserName,
		"--build_metadata=HOST=" + buildbuddyCIHostName,
		"--remote_header=x-buildbuddy-api-key=" + ak.Value,
		"--remote_instance_name=" + instanceName,
	}
	if bbURL := ws.env.GetConfigurator().GetAppBuildBuddyURL(); bbURL != "" {
		u, err := url.Parse(bbURL)
		if err != nil {
			return nil, err
		}
		u.Path = path.Join(u.Path, "invocation")
		flags = append(flags, fmt.Sprintf("--bes_results_url=%s/", u))
	}
	if eventsAPIURL := ws.env.GetConfigurator().GetAppEventsAPIURL(); eventsAPIURL != "" {
		flags = append(flags, "--bes_backend="+eventsAPIURL)
	}
	return flags, nil
}

func (ws *workflowService) startWorkflow(webhookID string, r *http.Request) error {
	ctx := r.Context()
	if err := ws.checkStartWorkflowPreconditions(ctx); err != nil {
		return err
	}
	webhookData, err := parseRequest(r)
	if err != nil {
		return err
	}
	if webhookData == nil {
		return nil
	}
	wf, err := ws.readWorkflowForWebhook(ctx, webhookID)
	if err != nil {
		return err
	}
	_, err = ws.executeWorkflow(ctx, wf, webhookData, nil /*=extraCIRunnerArgs*/)
	return err
}

// starts a CI runner execution and returns the execution ID.
func (ws *workflowService) executeWorkflow(ctx context.Context, wf *tables.Workflow, wd *webhook_data.WebhookData, extraCIRunnerArgs []string) (string, error) {
	key, err := ws.apiKeyForWorkflow(ctx, wf)
	if err != nil {
		return "", err
	}
	ctx = ws.env.GetAuthenticator().AuthContextFromAPIKey(ctx, key.Value)
	if ctx, err = prefix.AttachUserPrefixToContext(ctx, ws.env); err != nil {
		return "", err
	}
	in := instanceName(wd)
	ad, err := ws.createActionForWorkflow(ctx, wf, wd, key, in, extraCIRunnerArgs)
	if err != nil {
		return "", err
	}

	executionID, err := ws.env.GetRemoteExecutionService().Dispatch(ctx, &repb.ExecuteRequest{
		InstanceName:    in,
		SkipCacheLookup: true,
		ActionDigest:    ad,
	})
	if err != nil {
		return "", err
	}
	log.Infof("Started workflow execution (ID: %q)", executionID)
	metrics.WebhookHandlerWorkflowsStarted.With(prometheus.Labels{
		metrics.WebhookEventName: wd.EventName,
	}).Inc()
	return executionID, nil
}

func isGitHubURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return u.Host == "github.com"
}

func (ws *workflowService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	workflowMatch := workflowURLMatcher.FindStringSubmatch(r.URL.Path)
	if len(workflowMatch) != 2 {
		http.Error(w, "workflow URL not recognized", http.StatusNotFound)
		return
	}
	webhookID := workflowMatch[1]
	if err := ws.startWorkflow(webhookID, r); err != nil {
		log.Errorf("Failed to start workflow: %s", err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Write([]byte("OK"))
}
