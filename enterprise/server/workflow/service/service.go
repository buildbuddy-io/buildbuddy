package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/oauth2"
	"google.golang.org/genproto/googleapis/longrunning"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	githubapi "github.com/google/go-github/v43/github"
	guuid "github.com/google/uuid"
)

const (
	workflowsImage = "docker://gcr.io/flame-public/buildbuddy-ci-runner:v2.2.8"
)

var (
	enableFirecracker = flag.Bool("remote_execution.workflows_enable_firecracker", false, "Whether to enable firecracker for Linux workflow actions.")

	workflowURLMatcher = regexp.MustCompile(`^.*/webhooks/workflow/(?P<instance_name>.*)$`)

	// ApprovalRequired is an error indicating that a workflow action could not be
	// run at a commit because it is untrusted. An approving review at the
	// commit will allow the action to run.
	ApprovalRequired = status.PermissionDeniedErrorf("approval required")
)

// getWebhookID returns a string that can be used to uniquely identify a webhook.
func generateWebhookID() (string, error) {
	u, err := guuid.NewRandom()
	if err != nil {
		return "", err
	}
	return strings.ToLower(u.String()), nil
}

func instanceName(wf *tables.Workflow, wd *interfaces.WebhookData, workflowActionName string) string {
	// Use a unique remote instance name per repo URL and workflow action name, to help
	// route workflow tasks to runners which previously executed the same workflow
	// action.
	//
	// Instance name suffix is additionally used to effectively invalidate all
	// existing runners for the workflow and cause subsequent workflows to be run
	// from a clean runner.
	return fmt.Sprintf("%x", sha256.Sum256([]byte(wd.PushedRepoURL+workflowActionName+wf.InstanceNameSuffix)))
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

	// Ensure the request is authenticated so some group can own this workflow.
	groupID, err := perms.AuthenticateSelectedGroupID(ctx, ws.env, req.GetRequestContext())
	if err != nil {
		return nil, err
	}
	permissions := perms.GroupAuthPermissions(groupID)

	// Do a quick check to see if this is a valid repo that we can actually access.
	repoURL := repoReq.GetRepoUrl()
	u, err := gitutil.ParseRepoURL(repoURL)
	if err != nil {
		return nil, err
	}

	provider, err := ws.providerForRepo(u)
	if err != nil {
		return nil, err
	}
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

	providerWebhookID, err := provider.RegisterWebhook(ctx, accessToken, repoURL, webhookURL)
	if err != nil && !status.IsUnimplementedError(err) {
		return nil, err
	}
	rsp.WebhookRegistered = (providerWebhookID != "")

	err = ws.env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
		workflowID, err := tables.PrimaryKeyForTable("Workflows")
		if err != nil {
			return status.InternalError(err.Error())
		}
		rsp.Id = workflowID
		rsp.WebhookUrl = webhookURL
		wf := &tables.Workflow{
			WorkflowID:           workflowID,
			UserID:               permissions.UserID,
			GroupID:              permissions.GroupID,
			Perms:                permissions.Perms,
			Name:                 req.GetName(),
			RepoURL:              repoURL,
			Username:             username,
			AccessToken:          accessToken,
			WebhookID:            webhookID,
			GitProviderWebhookID: providerWebhookID,
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
		if err := tx.Raw(`SELECT user_id, group_id, perms, access_token, repo_url, git_provider_webhook_id FROM Workflows WHERE workflow_id = ?`, workflowID).Take(&wf).Error; err != nil {
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
	if wf.GitProviderWebhookID != "" {
		const errMsg = "Failed to remove the webhook from the repo; it may need to be removed manually via the repo's settings page"

		u, err := gitutil.ParseRepoURL(wf.RepoURL)
		if err != nil {
			return nil, status.WrapError(err, errMsg)
		}
		provider, err := ws.providerForRepo(u)
		if err != nil {
			return nil, status.WrapError(err, errMsg)
		}
		if err := provider.UnregisterWebhook(ctx, wf.AccessToken, wf.RepoURL, wf.GitProviderWebhookID); err != nil {
			return nil, status.WrapError(err, errMsg)
		}
	}
	return &wfpb.DeleteWorkflowResponse{}, nil
}

func (ws *workflowService) GetLinkedWorkflows(ctx context.Context, accessToken string) ([]string, error) {
	q, args := query_builder.
		NewQuery("SELECT workflow_id FROM Workflows").
		AddWhereClause("access_token = ?", accessToken).
		Build()
	rows, err := ws.env.GetDBHandle().DB(ctx).Raw(q, args...).Rows()
	if err != nil {
		return nil, err
	}
	var ids []string
	for rows.Next() {
		id := ""
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

func (ws *workflowService) providerForRepo(u *url.URL) (interfaces.GitProvider, error) {
	for _, provider := range ws.env.GetGitProviders() {
		if provider.MatchRepoURL(u) {
			return provider, nil
		}
	}
	return nil, status.InvalidArgumentErrorf("could not find git provider for %s", u.Hostname())
}

func (ws *workflowService) GetWorkflows(ctx context.Context, req *wfpb.GetWorkflowsRequest) (*wfpb.GetWorkflowsResponse, error) {
	if err := ws.checkPreconditions(ctx); err != nil {
		return nil, err
	}
	groupID, err := perms.AuthenticateSelectedGroupID(ctx, ws.env, req.GetRequestContext())
	if err != nil {
		return nil, err
	}

	rsp := &wfpb.GetWorkflowsResponse{}
	q := query_builder.NewQuery(`SELECT workflow_id, name, repo_url, webhook_id FROM Workflows`)
	// Respect selected group ID.
	q.AddWhereClause(`group_id = ?`, groupID)
	// Adds user / permissions check.
	if err := perms.AddPermissionsCheckToQuery(ctx, ws.env, q); err != nil {
		return nil, err
	}
	q.SetOrderBy("created_at_usec" /*ascending=*/, true)
	qStr, qArgs := q.Build()
	err = ws.env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
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
	if req.GetPushedRepoUrl() == "" {
		return nil, status.InvalidArgumentError("Missing pushed_repo_url")
	}
	if req.GetPushedBranch() == "" {
		return nil, status.InvalidArgumentError("Missing pushed_branch")
	}
	if req.GetTargetRepoUrl() == "" {
		return nil, status.InvalidArgumentError("Missing target_repo_url")
	}
	if req.GetTargetBranch() == "" {
		return nil, status.InvalidArgumentError("Missing target_branch")
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
	err = ws.env.GetDBHandle().DB(ctx).Raw(
		`SELECT * FROM Workflows WHERE workflow_id = ?`,
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

	// If running clean, update the instance name suffix.
	if req.GetClean() {
		if err := perms.AuthorizeWrite(&user, wfACL); err != nil {
			return nil, err
		}
		suffix, err := random.RandomString(10)
		if err != nil {
			return nil, err
		}
		wf.InstanceNameSuffix = suffix
		err = ws.env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
			return tx.Exec(
				`UPDATE Workflows SET instance_name_suffix = ? WHERE workflow_id = ?`,
				wf.InstanceNameSuffix, wf.WorkflowID,
			).Error
		})
		if err != nil {
			return nil, err
		}
	}

	// Execute
	// TODO: Refactor to avoid using this WebhookData struct in the case of manual
	// workflow execution, since there are no webhooks involved when executing a
	// workflow manually.
	wd := &interfaces.WebhookData{
		PushedRepoURL: req.GetPushedRepoUrl(),
		PushedBranch:  req.GetPushedBranch(),
		TargetRepoURL: req.GetTargetRepoUrl(),
		TargetBranch:  req.GetTargetBranch(),
		SHA:           req.GetCommitSha(),
		IsTrusted:     req.GetTargetRepoUrl() == req.GetPushedRepoUrl(),
		// Don't set IsTargetRepoPublic here; instead set visibility directly
		// from build metadata.
	}
	invocationUUID, err := guuid.NewRandom()
	if err != nil {
		return nil, err
	}
	invocationID := invocationUUID.String()
	extraCIRunnerArgs := []string{
		fmt.Sprintf("--visibility=%s", req.GetVisibility()),
	}
	apiKey, err := ws.apiKeyForWorkflow(ctx, wf)
	if err != nil {
		return nil, err
	}

	// Fetch the workflow config for the action we're about to execute
	repoURL, err := gitutil.ParseRepoURL(wd.PushedRepoURL)
	if err != nil {
		return nil, err
	}
	gitProvider, err := ws.providerForRepo(repoURL)
	if err != nil {
		return nil, err
	}
	cfg, err := ws.fetchWorkflowConfig(ctx, gitProvider, wf, wd)
	if err != nil {
		return nil, err
	}
	var action *config.Action
	for _, a := range cfg.Actions {
		if a.Name == req.GetActionName() {
			action = a
			break
		}
	}
	if action == nil {
		return nil, status.NotFoundErrorf("Workflow action %q not found", req.GetActionName())
	}
	executionID, err := ws.executeWorkflow(ctx, apiKey, wf, wd, action, invocationID, extraCIRunnerArgs)
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
	if err != nil {
		return nil, err
	}
	urls, err := listGitHubRepoURLs(ctx, token)
	if err != nil {
		return nil, status.WrapErrorf(err, "failed to list GitHub repo URLs")
	}
	res := &wfpb.GetReposResponse{}
	for _, url := range urls {
		res.Repo = append(res.Repo, &wfpb.Repo{Url: url})
	}
	return res, nil
}

func (ws *workflowService) gitHubTokenForAuthorizedGroup(ctx context.Context, reqCtx *ctxpb.RequestContext) (string, error) {
	d := ws.env.GetUserDB()
	if d == nil {
		return "", status.FailedPreconditionError("Missing UserDB")
	}
	groupID, err := perms.AuthenticateSelectedGroupID(ctx, ws.env, reqCtx)
	if err != nil {
		return "", err
	}
	g, err := d.GetGroupByID(ctx, groupID)
	if err != nil {
		return "", err
	}
	if g.GithubToken == nil || *g.GithubToken == "" {
		return "", status.FailedPreconditionError("The selected group does not have a GitHub account linked")
	}
	return *g.GithubToken, nil
}

func listGitHubRepoURLs(ctx context.Context, accessToken string) ([]string, error) {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	tc := oauth2.NewClient(ctx, ts)
	client := githubapi.NewClient(tc)
	repos, res, err := client.Repositories.List(ctx, "", &githubapi.RepositoryListOptions{
		Sort: "updated",
	})
	if err != nil {
		if res != nil && res.StatusCode == 401 {
			return nil, status.PermissionDeniedError(err.Error())
		}
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
	if err := ws.env.GetDBHandle().ReadRow(ctx, tw, `webhook_id = ?`, webhookID); err != nil {
		return nil, err
	}
	return tw, nil
}

// Creates an action that executes the CI runner for the given workflow and params.
// Returns the digest of the action as well as the invocation ID that the CI runner
// will assign to the workflow invocation.
func (ws *workflowService) createActionForWorkflow(ctx context.Context, wf *tables.Workflow, wd *interfaces.WebhookData, ak *tables.APIKey, instanceName string, workflowAction *config.Action, invocationID string, extraArgs []string) (*repb.Digest, error) {
	cache := ws.env.GetCache()
	if cache == nil {
		return nil, status.UnavailableError("No cache configured.")
	}
	inputRootDigest, err := digest.ComputeForMessage(&repb.Directory{})
	if err != nil {
		return nil, err
	}
	envVars := []*repb.Command_EnvironmentVariable{}
	if wd.IsTrusted {
		envVars = append(envVars, []*repb.Command_EnvironmentVariable{
			{Name: "BUILDBUDDY_API_KEY", Value: ak.Value},
			{Name: "REPO_USER", Value: wf.Username},
			{Name: "REPO_TOKEN", Value: wf.AccessToken},
		}...)
	}
	conf := ws.env.GetConfigurator()
	containerImage := ""
	isolationType := ""
	os := strings.ToLower(workflowAction.OS)
	// Use the CI runner image if the OS supports containerized actions.
	if os == "" || os == platform.LinuxOperatingSystemName {
		containerImage = ws.workflowsImage()
		if *enableFirecracker {
			isolationType = string(platform.FirecrackerContainerType)
			// When using Firecracker, write all outputs to the scratch disk, which
			// has more space than the workspace disk and doesn't need to be extracted
			// to the executor between action runs.
			envVars = append(envVars, &repb.Command_EnvironmentVariable{Name: "WORKDIR_OVERRIDE", Value: "/root/workspace"})
		}
	}
	if os == platform.DarwinOperatingSystemName && !wd.IsTrusted {
		return nil, ApprovalRequired
	}
	// Make the "outer" workflow invocation public if the target repo is public,
	// so that workflow commit status details can be seen by contributors.
	visibility := ""
	if wd.IsTargetRepoPublic {
		visibility = "PUBLIC"
	}
	cmd := &repb.Command{
		EnvironmentVariables: envVars,
		Arguments: append([]string{
			// NOTE: The executor is responsible for making sure this
			// buildbuddy_ci_runner binary exists at the workspace root. It does so
			// whenever it sees the `workflow-id` platform property.
			"./buildbuddy_ci_runner",
			"--invocation_id=" + invocationID,
			"--action_name=" + workflowAction.Name,
			"--bes_backend=" + conf.GetAppEventsAPIURL(),
			"--bes_results_url=" + conf.GetAppBuildBuddyURL() + "/invocation/",
			"--cache_backend=" + conf.GetAppCacheAPIURL(),
			"--rbe_backend=" + conf.GetAppRemoteExecutionAPIURL(),
			"--commit_sha=" + wd.SHA,
			"--pushed_repo_url=" + wd.PushedRepoURL,
			"--pushed_branch=" + wd.PushedBranch,
			"--target_repo_url=" + wd.TargetRepoURL,
			"--target_branch=" + wd.TargetBranch,
			"--visibility=" + visibility,
			"--workflow_id=" + wf.WorkflowID,
			"--trigger_event=" + wd.EventName,
			"--bazel_command=" + ws.ciRunnerBazelCommand(),
			"--debug=" + fmt.Sprintf("%v", ws.ciRunnerDebugMode()),
		}, extraArgs...),
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "Pool", Value: ws.workflowsPoolName()},
				{Name: "OSFamily", Value: os},
				{Name: "Arch", Value: workflowAction.Arch},
				{Name: "workload-isolation-type", Value: isolationType},
				{Name: "container-image", Value: containerImage},
				// Reuse the container/VM for the CI runner across executions if
				// possible, and also keep the git repo around so it doesn't need to be
				// re-cloned each time.
				{Name: "recycle-runner", Value: "true"},
				{Name: "preserve-workspace", Value: "true"},
				// Pass the workflow ID to the executor so that it can try to assign
				// this task to a runner which has previously executed the workflow.
				{Name: "workflow-id", Value: wf.WorkflowID},
				{Name: platform.EstimatedComputeUnitsPropertyName, Value: "2"},
				{Name: platform.EstimatedFreeDiskPropertyName, Value: "20000000000"}, // 20GB
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

func (ws *workflowService) workflowsImage() string {
	cfg := ws.env.GetConfigurator().GetRemoteExecutionConfig()
	if cfg != nil && cfg.WorkflowsDefaultImage != "" {
		return cfg.WorkflowsDefaultImage
	}
	return workflowsImage
}

func (ws *workflowService) ciRunnerDebugMode() bool {
	cfg := ws.env.GetConfigurator().GetRemoteExecutionConfig()
	if cfg == nil {
		return false
	}
	return cfg.WorkflowsCIRunnerDebug
}

func (ws *workflowService) ciRunnerBazelCommand() string {
	cfg := ws.env.GetConfigurator().GetRemoteExecutionConfig()
	if cfg == nil {
		return ""
	}
	return cfg.WorkflowsCIRunnerBazelCommand
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
	if err := ws.env.GetDBHandle().DB(ctx).Raw(qStr, qArgs...).Take(&k).Error; err != nil {
		return nil, err
	}
	return k, nil
}

func (ws *workflowService) gitProviderForRequest(r *http.Request) (interfaces.GitProvider, error) {
	for _, provider := range ws.env.GetGitProviders() {
		if provider.MatchWebhookRequest(r) {
			return provider, nil
		}
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

// fetchWorkflowConfig returns the BuildBuddyConfig from the repo, or the
// default BuildBuddyConfig if one is not set up.
func (ws *workflowService) fetchWorkflowConfig(ctx context.Context, gitProvider interfaces.GitProvider, workflow *tables.Workflow, webhookData *interfaces.WebhookData) (*config.BuildBuddyConfig, error) {
	b, err := gitProvider.GetFileContents(ctx, workflow.AccessToken, webhookData.PushedRepoURL, config.FilePath, webhookData.SHA)
	if err != nil {
		if status.IsNotFoundError(err) {
			return config.GetDefault(), nil
		}
		return nil, err
	}
	return config.NewConfig(bytes.NewReader(b))
}

func (ws *workflowService) startWorkflow(webhookID string, r *http.Request) error {
	ctx := r.Context()
	if err := ws.checkStartWorkflowPreconditions(ctx); err != nil {
		return err
	}
	gitProvider, err := ws.gitProviderForRequest(r)
	if err != nil {
		return err
	}
	webhookData, err := gitProvider.ParseWebhookData(r)
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
	apiKey, err := ws.apiKeyForWorkflow(ctx, wf)
	if err != nil {
		return err
	}
	// Fetch the workflow config (buildbuddy.yaml) and start a CI runner execution
	// for each action matching the webhook event.
	cfg, err := ws.fetchWorkflowConfig(ctx, gitProvider, wf, webhookData)
	if err != nil {
		return err
	}
	for _, action := range cfg.Actions {
		if !config.MatchesAnyTrigger(action, webhookData.EventName, webhookData.TargetBranch) {
			continue
		}
		invocationUUID, err := guuid.NewRandom()
		if err != nil {
			return err
		}
		invocationID := invocationUUID.String()
		_, err = ws.executeWorkflow(ctx, apiKey, wf, webhookData, action, invocationID, nil /*=extraCIRunnerArgs*/)
		if err != nil {
			if err == ApprovalRequired {
				if err := ws.createApprovalRequiredStatus(ctx, wf, webhookData, action.Name); err != nil {
					log.Warningf("Failed to create workflow action status: %s", err)
				}
				continue
			}
			return err
		}
	}
	return nil
}

// starts a CI runner execution and returns the execution ID.
func (ws *workflowService) executeWorkflow(ctx context.Context, key *tables.APIKey, wf *tables.Workflow, wd *interfaces.WebhookData, workflowAction *config.Action, invocationID string, extraCIRunnerArgs []string) (string, error) {
	ctx = ws.env.GetAuthenticator().AuthContextFromAPIKey(ctx, key.Value)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, ws.env)
	if err != nil {
		return "", err
	}
	in := instanceName(wf, wd, workflowAction.Name)
	ad, err := ws.createActionForWorkflow(ctx, wf, wd, key, in, workflowAction, invocationID, extraCIRunnerArgs)
	if err != nil {
		return "", err
	}

	execCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{ToolInvocationId: invocationID})
	if err != nil {
		return "", err
	}
	execCtx, cancelRPC := context.WithCancel(execCtx)
	// Note that we use this to cancel the operation update stream from the Execute RPC, not the execution itself.
	defer cancelRPC()

	opStream, err := ws.env.GetRemoteExecutionClient().Execute(execCtx, &repb.ExecuteRequest{
		InstanceName:    in,
		SkipCacheLookup: true,
		ActionDigest:    ad,
	})
	if err != nil {
		return "", err
	}
	op, err := opStream.Recv()
	if err != nil {
		return "", err
	}
	log.Infof("Started workflow execution (ID: %q)", op.GetName())
	metrics.WebhookHandlerWorkflowsStarted.With(prometheus.Labels{
		metrics.WebhookEventName: wd.EventName,
	}).Inc()
	return op.GetName(), nil
}

func (ws *workflowService) createApprovalRequiredStatus(ctx context.Context, wf *tables.Workflow, wd *interfaces.WebhookData, actionName string) error {
	// TODO: Create a help section in the docs that explains this error status, and link to it
	u, err := url.Parse(ws.env.GetConfigurator().GetAppBuildBuddyURL())
	if err != nil {
		return err
	}
	status := github.NewGithubStatusPayload(actionName, u.String(), "Check requires approving review", github.ErrorState)
	ownerRepo, err := gitutil.OwnerRepoFromRepoURL(wd.TargetRepoURL)
	if err != nil {
		return err
	}
	ghc := github.NewGithubClient(ws.env, wf.AccessToken)
	return ghc.CreateStatus(ctx, ownerRepo, wd.SHA, status)
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
