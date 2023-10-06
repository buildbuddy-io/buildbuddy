package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/workflow/config"
	"github.com/buildbuddy-io/buildbuddy/server/backends/github"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/build_buddy_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/cache_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/events_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/endpoint_urls/remote_exec_api_url"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/role"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/oauth2"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/types/known/durationpb"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
	remote_execution_config "github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/config"
	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	githubapi "github.com/google/go-github/v43/github"
	guuid "github.com/google/uuid"
	gstatus "google.golang.org/grpc/status"
)

var (
	enableFirecracker             = flag.Bool("remote_execution.workflows_enable_firecracker", false, "Whether to enable firecracker for Linux workflow actions.")
	workflowsPoolName             = flag.String("remote_execution.workflows_pool_name", "", "The executor pool to use for workflow actions. Defaults to the default executor pool if not specified.")
	workflowsDefaultImage         = flag.String("remote_execution.workflows_default_image", platform.DockerPrefix+platform.Ubuntu18_04WorkflowsImage, "The default container-image property to use for workflows. Must include docker:// prefix if applicable.")
	workflowsCIRunnerDebug        = flag.Bool("remote_execution.workflows_ci_runner_debug", false, "Whether to run the CI runner in debug mode.")
	workflowsCIRunnerBazelCommand = flag.String("remote_execution.workflows_ci_runner_bazel_command", "", "Bazel command to be used by the CI runner.")
	workflowsLinuxComputeUnits    = flag.Int("remote_execution.workflows_linux_compute_units", 3, "Number of BuildBuddy compute units (BCU) to reserve for Linux workflow actions.")
	workflowsMacComputeUnits      = flag.Int("remote_execution.workflows_mac_compute_units", 3, "Number of BuildBuddy compute units (BCU) to reserve for Mac workflow actions.")

	workflowURLMatcher = regexp.MustCompile(`^.*/webhooks/workflow/(?P<instance_name>.*)$`)

	// ApprovalRequired is an error indicating that a workflow action could not be
	// run at a commit because it is untrusted. An approving review at the
	// commit will allow the action to run.
	ApprovalRequired = status.PermissionDeniedErrorf("approval required")
)

const (
	// A workflow ID prefix that identifies a Workflow as being a
	// "synthetic" workflow adapted from a GitRepository.
	repoWorkflowIDPrefix = "WF#GitRepository"

	// Number of workers to work on processing webhook events in the background.
	webhookWorkerCount = 64

	// Max number of webhook payloads to buffer in memory. We expect some events
	// to occasionally be buffered during transient spikes in CAS or Execution
	// service latency.
	webhookWorkerTaskQueueSize = 100

	// How long to wait before giving up on processing a webhook payload.
	webhookWorkerTimeout = 30 * time.Second

	// How many times to retry workflow execution if it fails due to a transient
	// error.
	executeWorkflowMaxRetries = 4
)

// getWebhookID returns a string that can be used to uniquely identify a webhook.
func generateWebhookID() (string, error) {
	u, err := guuid.NewRandom()
	if err != nil {
		return "", err
	}
	return strings.ToLower(u.String()), nil
}

func instanceName(wf *tables.Workflow, wd *interfaces.WebhookData, workflowActionName string, gitCleanExclude []string) string {
	// Use a unique remote instance name per repo URL and workflow action name, to help
	// route workflow tasks to runners which previously executed the same workflow
	// action.
	//
	// git_clean_exclude is also included in this key so that if a PR is
	// modifying git_clean_exclude, it sees a consistent view of the excluded
	// directories throughout the PR.
	//
	// Instance name suffix is additionally used to effectively invalidate all
	// existing runners for the workflow and cause subsequent workflows to be run
	// from a clean runner.
	keys := append([]string{
		wd.PushedRepoURL,
		workflowActionName,
		wf.InstanceNameSuffix,
	}, gitCleanExclude...)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(strings.Join(keys, "|"))))
}

// startWorkflowTask represents a workflow to be started in the background in
// response to a webhook event. This is done in the background to avoid timeouts
// in the HTTP response to the webhook sender, in particular when there are
// spikes in CAS or Execution service latency.
type startWorkflowTask struct {
	ctx         context.Context
	gitProvider interfaces.GitProvider
	webhookData *interfaces.WebhookData
	workflow    *tables.Workflow
}

type workflowService struct {
	env environment.Env

	wg    sync.WaitGroup
	tasks chan *startWorkflowTask
	bbUrl *url.URL
}

func NewWorkflowService(env environment.Env) *workflowService {
	ws := &workflowService{
		env:   env,
		tasks: make(chan *startWorkflowTask, webhookWorkerTaskQueueSize),
		bbUrl: build_buddy_url.WithPath(""),
	}
	ws.startBackgroundWorkers()
	return ws
}

func (ws *workflowService) startBackgroundWorkers() {
	for i := 0; i < webhookWorkerCount; i++ {
		ws.wg.Add(1)
		go func() {
			defer ws.wg.Done()

			for task := range ws.tasks {
				ws.runStartWorkflowTask(task)
			}
		}()
	}
	ws.env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		// Wait until the HTTP server shuts down to ensure that all in-flight
		// webhook requests are done being handled and that no further HTTP
		// webhook requests will come in.
		ws.env.GetHTTPServerWaitGroup().Wait()
		// Now that all in-flight HTTP requests have completed, it is safe to
		// close the tasks channel. Once we do this, the background workers
		// should exit as soon as they drain and execute the remaining tasks in
		// the channel.
		close(ws.tasks)
		// Wait for all workers to exit.
		ws.wg.Wait()
		return nil
	})
}

func (ws *workflowService) enqueueStartWorkflowTask(ctx context.Context, gitProvider interfaces.GitProvider, wd *interfaces.WebhookData, wf *tables.Workflow) error {
	t := &startWorkflowTask{
		ctx:         ctx,
		gitProvider: gitProvider,
		webhookData: wd,
		workflow:    wf,
	}
	select {
	case ws.tasks <- t:
		return nil
	default:
		alert.UnexpectedEvent(
			"workflow_task_queue_full",
			"Workflows are not being triggered due to the task queue being full. This may be due to elevated CAS or Execution service latency.")
		return status.ResourceExhaustedError("workflow task queue is full")
	}
}

func (ws *workflowService) runStartWorkflowTask(task *startWorkflowTask) {
	// Keep the existing context values from the client but set a new timeout
	// since the HTTP request has already completed at this point.
	ctx, cancel := background.ExtendContextForFinalization(task.ctx, webhookWorkerTimeout)
	defer cancel()

	if err := ws.startWorkflow(ctx, task.gitProvider, task.webhookData, task.workflow); err != nil {
		log.Errorf("Failed to start workflow in the background: %s", err)
	}
}

// getWebhookURL takes a webhookID and returns a fully qualified URL, on this
// server, where the specified webhook can be called.
func (ws *workflowService) getWebhookURL(webhookID string) (string, error) {
	u, err := url.Parse(fmt.Sprintf("/webhooks/workflow/%s", webhookID))
	if err != nil {
		return "", err
	}
	wu := ws.bbUrl.ResolveReference(&url.URL{Path: u.String()})
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

	u, err := gitutil.NormalizeRepoURL(repoReq.GetRepoUrl())
	if err != nil {
		return nil, err
	}
	repoURL := u.String()

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
			return nil, status.InvalidArgumentError("An access token is required since the current organization does not have a GitHub account linked.")
		}
		accessToken = token
	}

	// Do a quick check to see if this is a valid repo that we can actually access.
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
	if err != nil {
		log.CtxWarningf(ctx, "Failed to register webhook with git provider: %s", err)
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
	if req.GetId() == "" && req.GetRepoUrl() == "" {
		return nil, status.InvalidArgumentError("An ID or repo_url is required to delete a workflow.")
	}
	authenticatedUser, err := ws.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	var wf tables.Workflow
	err = ws.env.GetDBHandle().Transaction(ctx, func(tx *db.DB) error {
		var q *db.DB
		if req.GetId() != "" {
			q = tx.Raw(`
				SELECT * FROM "Workflows" WHERE workflow_id = ?
				`+ws.env.GetDBHandle().SelectForUpdateModifier()+`
			`, req.GetId())
		} else {
			q = tx.Raw(`
				SELECT * FROM "Workflows"
				WHERE group_id = ? AND repo_url = ?
				`+ws.env.GetDBHandle().SelectForUpdateModifier()+`
			`, authenticatedUser.GetGroupID(), req.GetRepoUrl())
		}
		if err := q.Take(&wf).Error; err != nil {
			return err
		}
		acl := perms.ToACLProto(&uidpb.UserId{Id: wf.UserID}, wf.GroupID, wf.Perms)
		if err := perms.AuthorizeWrite(&authenticatedUser, acl); err != nil {
			return err
		}
		return tx.Exec(`DELETE FROM "Workflows" WHERE workflow_id = ?`, wf.WorkflowID).Error
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
		NewQuery(`SELECT workflow_id FROM "Workflows"`).
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

func (ws *workflowService) GetWorkflows(ctx context.Context) (*wfpb.GetWorkflowsResponse, error) {
	if err := ws.checkPreconditions(ctx); err != nil {
		return nil, err
	}

	u, err := perms.AuthenticatedUser(ctx, ws.env)
	if err != nil {
		return nil, err
	}
	groupID := u.GetGroupID()

	rsp := &wfpb.GetWorkflowsResponse{}
	q := query_builder.NewQuery(`SELECT workflow_id, name, repo_url, webhook_id FROM "Workflows"`)
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

	// Authenticate
	user, err := perms.AuthenticatedUser(ctx, ws.env)
	if err != nil {
		return nil, err
	}

	wf, err := ws.getWorkflowByID(ctx, req.GetWorkflowId())
	if err != nil {
		return nil, err
	}

	// Authorize workflow access
	wfACL := perms.ToACLProto(&uidpb.UserId{Id: wf.GroupID}, wf.GroupID, wf.Perms)
	if err := perms.AuthorizeRead(user, wfACL); err != nil {
		return nil, err
	}

	if req.GetClean() {
		err = ws.useCleanWorkflow(ctx, wf)
		if err != nil {
			return nil, err
		}
	}

	// TODO: Refactor to avoid using this WebhookData struct in the case of manual
	// workflow execution, since there are no webhooks involved when executing a
	// workflow manually.
	wd := &interfaces.WebhookData{
		PushedRepoURL:     req.GetPushedRepoUrl(),
		PushedBranch:      req.GetPushedBranch(),
		TargetRepoURL:     req.GetTargetRepoUrl(),
		TargetBranch:      req.GetTargetBranch(),
		SHA:               req.GetCommitSha(),
		PullRequestNumber: req.GetPullRequestNumber(),
		// Don't set IsTargetRepoPublic here; instead set visibility directly
		// from build metadata.
	}
	extraCIRunnerArgs := []string{
		fmt.Sprintf("--visibility=%s", req.GetVisibility()),
	}
	apiKey, err := ws.apiKeyForWorkflow(ctx, wf)
	if err != nil {
		return nil, err
	}

	actions, err := ws.getActions(ctx, wf, wd, req.GetActionNames())
	if err != nil {
		return nil, err
	}

	wg := sync.WaitGroup{}
	actionStatuses := make([]*wfpb.ExecuteWorkflowResponse_ActionStatus, 0, len(actions))
	for actionName, action := range actions {
		action := action
		actionName := actionName
		actionStatus := &wfpb.ExecuteWorkflowResponse_ActionStatus{
			ActionName: actionName,
		}
		actionStatuses = append(actionStatuses, actionStatus)

		wg.Add(1)
		go func() {
			defer wg.Done()

			var invocationID string
			var statusErr error
			defer func() {
				actionStatus.InvocationId = invocationID
				actionStatus.Status = gstatus.Convert(statusErr).Proto()
			}()

			if action == nil {
				statusErr = status.NotFoundErrorf("action %s not found", actionName)
				return
			}

			invocationUUID, err := guuid.NewRandom()
			if err != nil {
				statusErr = status.InternalErrorf("failed to generate invocation ID: %s", err)
				log.CtxError(ctx, statusErr.Error())
				return
			}
			invocationID = invocationUUID.String()

			// The workflow execution is trusted since we're authenticated as a member of
			// the BuildBuddy org that owns the workflow.
			isTrusted := true
			executionID, err := ws.executeWorkflowAction(ctx, apiKey, wf, wd, isTrusted, action, invocationID, extraCIRunnerArgs)
			if err != nil {
				statusErr = status.WrapErrorf(err, "failed to execute workflow action %s", req.GetActionName())
				log.CtxWarning(ctx, statusErr.Error())
				return
			}
			if req.GetAsync() {
				return
			}
			if err := ws.waitForWorkflowInvocationCreated(ctx, executionID, invocationID); err != nil {
				statusErr = err
				log.CtxWarning(ctx, statusErr.Error())
				return
			}
		}()
	}
	wg.Wait()

	return &wfpb.ExecuteWorkflowResponse{
		ActionStatuses: actionStatuses,
	}, nil
}

func (ws *workflowService) getActions(ctx context.Context, wf *tables.Workflow, wd *interfaces.WebhookData, actionFilter []string) (map[string]*config.Action, error) {
	// Fetch the workflow config
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

	actionMap := make(map[string]*config.Action, len(cfg.Actions))
	for _, a := range cfg.Actions {
		actionMap[a.Name] = a
	}

	var filteredActions map[string]*config.Action
	if len(actionFilter) > 0 {
		filteredActions = make(map[string]*config.Action, len(actionFilter))
		for _, actionName := range actionFilter {
			a, ok := actionMap[actionName]
			if ok {
				filteredActions[actionName] = a
			} else {
				log.Debugf("workflow action %s not found", actionName)
				filteredActions[actionName] = nil
			}
		}
	} else {
		filteredActions = actionMap
	}

	if len(filteredActions) == 0 {
		if len(actionFilter) == 0 {
			return nil, status.NotFoundError("no workflow actions found")
		} else {
			return nil, status.NotFoundErrorf("requested workflow actions %v not found", actionFilter)
		}
	}

	return filteredActions, nil
}

func (ws *workflowService) getWorkflowByID(ctx context.Context, workflowID string) (*tables.Workflow, error) {
	isLegacyWorkflow := !isRepositoryWorkflowID(workflowID)
	if isLegacyWorkflow {
		wf := &tables.Workflow{}
		err := ws.env.GetDBHandle().DB(ctx).Raw(
			`SELECT * FROM Workflows WHERE workflow_id = ?`,
			workflowID,
		).Take(wf).Error
		if err != nil {
			if db.IsRecordNotFound(err) {
				return nil, status.NotFoundError("Workflow not found")
			}
			return nil, status.InternalError(err.Error())
		}
		return wf, nil
	}

	// If the workflow ID identifies a GitRepository, look up the GitRepository and construct a synthetic Workflow
	// from it.
	groupID, repoURL, err := parseRepositoryWorkflowID(workflowID)
	if err != nil {
		return nil, err
	}
	rwf, err := ws.getRepositoryWorkflow(ctx, groupID, repoURL)
	if err != nil {
		return nil, err
	}

	return rwf.Workflow, nil
}

// GetLegacyWorkflowIDForGitRepository generates an artificial workflow ID so that legacy Workflow structs
// can be created from GitRepositories to play nicely with the pre-existing architecture
// that expects the legacy format
func (ws *workflowService) GetLegacyWorkflowIDForGitRepository(groupID string, repoURL string) string {
	return fmt.Sprintf("%s:%s:%s", repoWorkflowIDPrefix, groupID, repoURL)
}

func (ws *workflowService) checkCleanWorkflowPermissions(ctx context.Context, wf *tables.Workflow) error {
	u, err := perms.AuthenticatedUser(ctx, ws.env)
	if err != nil {
		return err
	}
	g, err := ws.env.GetUserDB().GetGroupByID(ctx, u.GetGroupID())
	if err != nil {
		return err
	}
	if !g.RestrictCleanWorkflowRunsToAdmins {
		return nil
	}
	return authutil.AuthorizeGroupRole(u, wf.GroupID, role.Admin)
}

// To run workflow in a clean container, update the instance name suffix
func (ws *workflowService) useCleanWorkflow(ctx context.Context, wf *tables.Workflow) error {
	if err := ws.checkCleanWorkflowPermissions(ctx, wf); err != nil {
		return err
	}

	suffix, err := random.RandomString(10)
	if err != nil {
		return err
	}
	wf.InstanceNameSuffix = suffix

	if isRepositoryWorkflowID(wf.WorkflowID) {
		log.CtxInfof(ctx, "Workflow clean run requested for repo %q (group %s)", wf.RepoURL, wf.GroupID)
		err = ws.env.GetDBHandle().DB(ctx).Exec(`
				UPDATE GitRepositories
				SET instance_name_suffix = ?
				WHERE group_id = ? AND repo_url = ?`,
			suffix, wf.GroupID, wf.RepoURL,
		).Error
	} else {
		log.CtxInfof(ctx, "Workflow clean run requested for workflow %q (group %s)", wf.WorkflowID, wf.GroupID)
		err = ws.env.GetDBHandle().DB(ctx).Exec(`
				UPDATE Workflows
				SET instance_name_suffix = ?
				WHERE workflow_id = ?`,
			wf.InstanceNameSuffix, wf.WorkflowID,
		).Error
	}
	return err
}

func (ws *workflowService) getRepositoryWorkflow(ctx context.Context, groupID string, repoURL *gitutil.RepoURL) (*repositoryWorkflow, error) {
	app := ws.env.GetGitHubApp()
	if app == nil {
		return nil, status.UnimplementedError("GitHub App is not configured")
	}
	if err := perms.AuthorizeGroupAccess(ctx, ws.env, groupID); err != nil {
		return nil, err
	}
	gitRepository := &tables.GitRepository{}
	err := ws.env.GetDBHandle().DB(ctx).Raw(`
		SELECT *
		FROM "GitRepositories"
		WHERE group_id = ?
		AND repo_url = ?
	`, groupID, repoURL.String()).Take(gitRepository).Error
	if err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundErrorf("repo %q not found", repoURL)
		}
		return nil, status.InternalErrorf("failed to look up repo %q: %s", repoURL, err)
	}
	token, err := app.GetRepositoryInstallationToken(ctx, gitRepository)
	if err != nil {
		return nil, err
	}
	return ws.gitRepositoryWorkflow(gitRepository, token), nil
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
		log.Infof("Polling invocation status...")
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

func (ws *workflowService) buildActionHistoryQuery(ctx context.Context, repoUrl string, pattern string, timeLimitMicros int64) (*query_builder.Query, error) {
	q := query_builder.NewQuery(`SELECT repo_url, pattern, invocation_id, commit_sha, branch_name, created_at_usec, updated_at_usec, duration_usec, invocation_status, success FROM Invocations`)
	if err := perms.AddPermissionsCheckToQuery(ctx, ws.env, q); err != nil {
		return nil, err
	}
	authenticatedUser, err := ws.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	q.AddWhereClause("repo_url = ?", repoUrl)
	q.AddWhereClause("pattern = ?", pattern)
	// TODO(jdhollen): Efficiently fetch the default branch name for the
	// repo instead of guessing.
	q.AddWhereClause("branch_name IN ?", []string{"master", "main"})
	q.AddWhereClause("role = ?", "CI_RUNNER")
	q.AddWhereClause("group_id = ?", authenticatedUser.GetGroupID())
	q.AddWhereClause("updated_at_usec > ?", timeLimitMicros)
	q.SetOrderBy("created_at_usec", false)
	q.SetLimit(30)

	return q, nil
}

func (ws *workflowService) GetWorkflowHistory(ctx context.Context) (*wfpb.GetWorkflowHistoryResponse, error) {
	if ws.env.GetDBHandle() == nil || ws.env.GetOLAPDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}

	linkedRepos, err := ws.env.GetGitHubApp().GetLinkedGitHubRepos(ctx)
	if err != nil {
		return nil, err
	}
	repos := linkedRepos.GetRepoUrls()
	if len(repos) == 0 {
		// Fall back to legacy workflow registrations.
		repos = []string{}
		workflows, err := ws.GetWorkflows(ctx)
		if err != nil {
			return nil, err
		}
		for _, wf := range workflows.GetWorkflow() {
			if wf.GetRepoUrl() != "" {
				repos = append(repos, wf.GetRepoUrl())
			}
		}
	}

	if len(repos) == 0 {
		return &wfpb.GetWorkflowHistoryResponse{}, nil
	}

	authenticatedUser, err := ws.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	// Only fetch workflow history for stuff that has run in the last 7 days.
	timeLimitMicros := time.Now().Add(-7 * 24 * time.Hour).UnixMicro()

	// Find the names of all of the actions for each repo.
	q := query_builder.NewQuery(`SELECT repo_url,pattern,count(1) AS total_runs, countIf(success) AS successful_runs, toInt64(avg(duration_usec)) AS average_duration FROM Invocations`)
	q.AddWhereClause("repo_url IN ?", repos)
	q.AddWhereClause("role = ?", "CI_RUNNER")
	q.AddWhereClause("notEmpty(pattern)")

	// Clickhouse doesn't have an explicit perms column, so we only do auth
	// on group ID on this query path.  We don't really have a single-user
	// use case for workflows and we don't have anonymous access to the
	// workflow status page, so this should be fine.
	q.AddWhereClause("group_id = ?", authenticatedUser.GetGroupID())
	q.AddWhereClause("invocation_status = ?", int(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS))
	q.AddWhereClause("updated_at_usec > ?", timeLimitMicros)
	q.SetGroupBy("repo_url,pattern")

	type actionQueryOut struct {
		RepoUrl         string
		Pattern         string
		TotalRuns       int64
		SuccessfulRuns  int64
		AverageDuration int64
	}

	qStr, qArgs := q.Build()
	rows, err := ws.env.GetOLAPDBHandle().RawWithOptions(ctx, clickhouse.Opts().WithQueryName("query_find_workflow_actions"), qStr, qArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	actionHistoryQArgs := make([]interface{}, 0)
	actionHistoryQStrs := make([]string, 0)
	workflows := make(map[string]map[string]*wfpb.ActionHistory)
	for rows.Next() {
		row := &actionQueryOut{}
		if err := ws.env.GetOLAPDBHandle().DB(ctx).ScanRows(rows, &row); err != nil {
			return nil, err
		}
		q, err := ws.buildActionHistoryQuery(ctx, row.RepoUrl, row.Pattern, timeLimitMicros)
		if err != nil {
			return nil, err
		}
		actionQStr, actionQArgs := q.Build()
		actionHistoryQArgs = append(actionHistoryQArgs, actionQArgs...)
		actionHistoryQStrs = append(actionHistoryQStrs, "("+actionQStr+")")
		summary := &wfpb.ActionHistory_Summary{
			TotalRuns:       row.TotalRuns,
			SuccessfulRuns:  row.SuccessfulRuns,
			AverageDuration: durationpb.New(time.Duration(row.AverageDuration) * time.Microsecond),
		}
		if _, ok := workflows[row.RepoUrl]; !ok {
			workflows[row.RepoUrl] = make(map[string]*wfpb.ActionHistory)
		}
		workflows[row.RepoUrl][row.Pattern] = &wfpb.ActionHistory{ActionName: row.Pattern, Summary: summary}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// No workflows configured for any repos yet.
	if len(actionHistoryQStrs) == 0 {
		return &wfpb.GetWorkflowHistoryResponse{}, nil
	}
	finalActionHistoryQStr := strings.Join(actionHistoryQStrs, " UNION ALL ")

	historyRows, err := ws.env.GetDBHandle().RawWithOptions(ctx, db.Opts().WithQueryName("query_workflow_action_history"), finalActionHistoryQStr, actionHistoryQArgs...).Rows()
	if err != nil {
		return nil, err
	}
	defer historyRows.Close()

	type historyQueryOut struct {
		RepoUrl          string
		Pattern          string
		InvocationId     string
		CommitSha        string
		BranchName       string
		CreatedAtUsec    int64
		UpdatedAtUsec    int64
		DurationUsec     int64
		InvocationStatus int64
		Success          bool
	}

	for historyRows.Next() {
		row := &historyQueryOut{}
		if err := ws.env.GetDBHandle().DB(ctx).ScanRows(historyRows, &row); err != nil {
			return nil, err
		}
		entry := &wfpb.ActionHistory_Entry{
			Status:        inspb.InvocationStatus(row.InvocationStatus),
			Success:       row.Success,
			CommitSha:     row.CommitSha,
			BranchName:    row.BranchName,
			InvocationId:  row.InvocationId,
			Duration:      durationpb.New(time.Duration(row.DurationUsec) * time.Microsecond),
			CreatedAtUsec: row.CreatedAtUsec,
			UpdatedAtUsec: row.UpdatedAtUsec,
		}

		workflows[row.RepoUrl][row.Pattern].Entries = append(workflows[row.RepoUrl][row.Pattern].Entries, entry)
	}
	if err := historyRows.Err(); err != nil {
		return nil, err
	}

	// OKAY! We have everything, now we'll just sort and build the response.
	res := &wfpb.GetWorkflowHistoryResponse{}

	for repoUrl := range workflows {
		wfHistory := &wfpb.GetWorkflowHistoryResponse_WorkflowHistory{
			RepoUrl: repoUrl,
		}

		// Sort actions by name.
		actions := make([]string, 0, len(workflows[repoUrl]))
		for action := range workflows[repoUrl] {
			actions = append(actions, action)
		}
		sort.Strings(actions)
		for _, action := range actions {
			wfHistory.ActionHistory = append(wfHistory.ActionHistory, workflows[repoUrl][action])
		}
		res.WorkflowHistory = append(res.WorkflowHistory, wfHistory)
	}

	return res, nil
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
func (ws *workflowService) createActionForWorkflow(ctx context.Context, wf *tables.Workflow, wd *interfaces.WebhookData, isTrusted bool, ak *tables.APIKey, instanceName string, workflowAction *config.Action, invocationID string, extraArgs []string) (*repb.Digest, error) {
	cache := ws.env.GetCache()
	if cache == nil {
		return nil, status.UnavailableError("No cache configured.")
	}
	inputRootDigest, err := digest.ComputeForMessage(&repb.Directory{}, repb.DigestFunction_SHA256)
	if err != nil {
		return nil, err
	}
	envVars := []*repb.Command_EnvironmentVariable{}
	containerImage := ""
	isolationType := ""
	os := strings.ToLower(workflowAction.OS)
	computeUnits := *workflowsMacComputeUnits
	// Use the CI runner image if the OS supports containerized actions.
	isSharedFirecrackerWorkflow := *enableFirecracker && !workflowAction.SelfHosted
	if os == "" || os == platform.LinuxOperatingSystemName {
		computeUnits = *workflowsLinuxComputeUnits
		containerImage = ws.containerImage(workflowAction)
		if isSharedFirecrackerWorkflow {
			isolationType = string(platform.FirecrackerContainerType)
			// When using Firecracker, write all outputs to the scratch disk, which
			// has more space than the workspace disk and doesn't need to be extracted
			// to the executor between action runs.
			wd := filepath.Join(workflowHomeDir(workflowAction.User), "workspace")
			envVars = append(envVars, &repb.Command_EnvironmentVariable{Name: "WORKDIR_OVERRIDE", Value: wd})
		}
	}
	if os == platform.DarwinOperatingSystemName && !isTrusted {
		return nil, ApprovalRequired
	}
	// Make the "outer" workflow invocation public if the target repo is public,
	// so that workflow commit status details can be seen by contributors.
	visibility := ""
	if wd.IsTargetRepoPublic {
		visibility = "PUBLIC"
	}
	includeSecretsPropertyValue := "false"
	if isTrusted && ws.env.GetSecretService() != nil {
		includeSecretsPropertyValue = "true"
	}
	estimatedDisk := workflowAction.ResourceRequests.GetEstimatedDisk()
	if estimatedDisk == "" {
		estimatedDisk = fmt.Sprintf("%d", 20_000_000_000) // 20 GB
	}
	useSelfHostedExecutors := "false"
	if workflowAction.SelfHosted {
		useSelfHostedExecutors = "true"
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
			"--bes_backend=" + events_api_url.String(),
			"--bes_results_url=" + ws.bbUrl.ResolveReference(&url.URL{Path: "/invocation/"}).String(),
			"--cache_backend=" + cache_api_url.String(),
			"--rbe_backend=" + remote_exec_api_url.String(),
			"--remote_instance_name=" + instanceName,
			"--commit_sha=" + wd.SHA,
			"--pushed_repo_url=" + wd.PushedRepoURL,
			"--pushed_branch=" + wd.PushedBranch,
			"--pull_request_number=" + fmt.Sprintf("%d", wd.PullRequestNumber),
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
				{Name: "Pool", Value: ws.poolForAction(workflowAction)},
				{Name: "OSFamily", Value: os},
				{Name: "Arch", Value: workflowAction.Arch},
				{Name: platform.DockerUserPropertyName, Value: workflowAction.User},
				{Name: "workload-isolation-type", Value: isolationType},
				{Name: "container-image", Value: containerImage},
				// Reuse the container/VM for the CI runner across executions if
				// possible, and also keep the git repo around so it doesn't need to be
				// re-cloned each time.
				{Name: "recycle-runner", Value: "true"},
				{Name: "preserve-workspace", Value: "true"},
				{Name: "use-self-hosted-executors", Value: useSelfHostedExecutors},
				// Pass the workflow ID to the executor so that it can try to assign
				// this task to a runner which has previously executed the workflow.
				{Name: "workflow-id", Value: wf.WorkflowID},
				{Name: platform.IncludeSecretsPropertyName, Value: includeSecretsPropertyValue},
				{Name: platform.EstimatedComputeUnitsPropertyName, Value: fmt.Sprintf("%d", computeUnits)},
				{Name: platform.EstimatedFreeDiskPropertyName, Value: estimatedDisk},
				{Name: platform.EstimatedMemoryPropertyName, Value: workflowAction.ResourceRequests.GetEstimatedMemory()},
				{Name: platform.EstimatedCPUPropertyName, Value: workflowAction.ResourceRequests.GetEstimatedCPU()},
			},
		},
	}
	if !isSharedFirecrackerWorkflow {
		// For docker/podman workflows, run with `--init` so that the bazel
		// process can be reaped.
		cmd.Platform.Properties = append(cmd.Platform.Properties, &repb.Platform_Property{
			Name:  platform.DockerInitPropertyName,
			Value: "true",
		})
	}
	for _, path := range workflowAction.GitCleanExclude {
		cmd.Arguments = append(cmd.Arguments, "--git_clean_exclude="+path)
	}
	cmdDigest, err := cachetools.UploadProtoToCAS(ctx, cache, instanceName, repb.DigestFunction_SHA256, cmd)
	if err != nil {
		return nil, err
	}
	action := &repb.Action{
		CommandDigest:   cmdDigest,
		InputRootDigest: inputRootDigest,
		DoNotCache:      true,
	}
	actionDigest, err := cachetools.UploadProtoToCAS(ctx, cache, instanceName, repb.DigestFunction_SHA256, action)
	return actionDigest, err
}

func (ws *workflowService) poolForAction(action *config.Action) string {
	if action.SelfHosted && action.Pool != "" {
		return action.Pool
	}
	return ws.WorkflowsPoolName()
}

func (ws *workflowService) WorkflowsPoolName() string {
	if remote_execution_config.RemoteExecutionEnabled() && *workflowsPoolName != "" {
		return *workflowsPoolName
	}
	return platform.DefaultPoolValue
}

func (ws *workflowService) containerImage(action *config.Action) string {
	if action.ContainerImage != "" {
		return ws.resolveImageAliases(action.ContainerImage)
	}
	return *workflowsDefaultImage
}

func (ws *workflowService) resolveImageAliases(value string) string {
	// Let people write "container_image: ubuntu-<VERSION>" as a shorthand for
	// "latest ubuntu <VERSION> image".
	if value == "ubuntu-18.04" {
		return platform.DockerPrefix + platform.Ubuntu18_04WorkflowsImage
	}
	if value == "ubuntu-20.04" {
		return platform.DockerPrefix + platform.Ubuntu20_04WorkflowsImage
	}

	// Otherwise, interpret container_image the same way we treat it for RBE
	// actions.
	return value
}

func (ws *workflowService) ciRunnerDebugMode() bool {
	return remote_execution_config.RemoteExecutionEnabled() && *workflowsCIRunnerDebug
}

func (ws *workflowService) ciRunnerBazelCommand() string {
	if !remote_execution_config.RemoteExecutionEnabled() {
		return ""
	}
	return *workflowsCIRunnerBazelCommand
}

func runnerBinaryFile() (*os.File, error) {
	path, err := bazelgo.Runfile("enterprise/server/cmd/ci_runner/ci_runner_/ci_runner")
	if err != nil {
		return nil, status.FailedPreconditionErrorf("could not find runner binary runfile: %s", err)
	}
	return os.Open(path)
}

func (ws *workflowService) apiKeyForWorkflow(ctx context.Context, wf *tables.Workflow) (*tables.APIKey, error) {
	k, err := ws.env.GetAuthDB().GetAPIKeyForInternalUseOnly(ctx, wf.GroupID)
	if err != nil {
		return nil, status.WrapErrorf(err, "failed to get API key for workflow")
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
	if ws.env.GetRemoteExecutionClient() == nil {
		return status.UnavailableError("Remote execution not configured.")
	}
	return nil
}

// fetchWorkflowConfig returns the BuildBuddyConfig from the repo, or the
// default BuildBuddyConfig if one is not set up.
func (ws *workflowService) fetchWorkflowConfig(ctx context.Context, gitProvider interfaces.GitProvider, workflow *tables.Workflow, webhookData *interfaces.WebhookData) (*config.BuildBuddyConfig, error) {
	workflowRef := webhookData.SHA
	if workflowRef == "" {
		workflowRef = webhookData.PushedBranch
	}

	b, err := gitProvider.GetFileContents(ctx, workflow.AccessToken, webhookData.PushedRepoURL, config.FilePath, workflowRef)
	if err != nil {
		if status.IsNotFoundError(err) {
			return config.GetDefault(webhookData.TargetRepoDefaultBranch), nil
		}
		return nil, err
	}
	return config.NewConfig(bytes.NewReader(b))
}

func (ws *workflowService) isTrustedCommit(ctx context.Context, gitProvider interfaces.GitProvider, wf *tables.Workflow, wd *interfaces.WebhookData) (bool, error) {
	// If the commit was pushed directly to the target repo then the commit must
	// already be trusted.
	isFork := wd.PushedRepoURL != wd.TargetRepoURL
	if !isFork {
		return true, nil
	}
	if wd.PullRequestAuthor == "" {
		return false, status.FailedPreconditionErrorf("missing pull request author for %s event (workflow %s)", wd.EventName, wf.WorkflowID)
	}
	// Otherwise make an API call to check whether the user is a trusted
	// collaborator.
	return gitProvider.IsTrusted(ctx, wf.AccessToken, wd.TargetRepoURL, wd.PullRequestAuthor)
}

func (ws *workflowService) HandleRepositoryEvent(ctx context.Context, repo *tables.GitRepository, wd *interfaces.WebhookData, accessToken string) error {
	u, err := url.Parse(repo.RepoURL)
	if err != nil {
		log.CtxErrorf(ctx, "Failed to parse repo URL %s", u.String())
		return status.InvalidArgumentError("failed to parse repo URL")
	}
	provider, err := ws.providerForRepo(u)
	if err != nil {
		return err
	}
	wf := ws.gitRepositoryWorkflow(repo, accessToken).Workflow
	return ws.enqueueStartWorkflowTask(ctx, provider, wd, wf)
}

func (ws *workflowService) startLegacyWorkflow(ctx context.Context, webhookID string, r *http.Request) error {
	if err := ws.checkStartWorkflowPreconditions(ctx); err != nil {
		return err
	}
	gitProvider, err := ws.gitProviderForRequest(r)
	if err != nil {
		return err
	}
	wd, err := gitProvider.ParseWebhookData(r)
	if err != nil {
		return err
	}
	if wd == nil {
		return nil
	}
	log.CtxDebugf(ctx, "Parsed webhook data: %s", webhook_data.DebugString(wd))
	wf, err := ws.readWorkflowForWebhook(ctx, webhookID)
	if err != nil {
		return status.WrapErrorf(err, "failed to lookup workflow for webhook ID %q", webhookID)
	}
	return ws.enqueueStartWorkflowTask(ctx, gitProvider, wd, wf)
}

func (ws *workflowService) startWorkflow(ctx context.Context, gitProvider interfaces.GitProvider, wd *interfaces.WebhookData, wf *tables.Workflow) error {
	isTrusted, err := ws.isTrustedCommit(ctx, gitProvider, wf, wd)
	if err != nil {
		return err
	}
	// If this is a PR approval event and the PR is untrusted, then re-run the
	// workflow as a trusted workflow, but only if the approver is trusted, and
	// only if the PR is not already trusted (to avoid unnecessary re-runs).
	if wd.PullRequestApprover != "" {
		if isTrusted {
			log.CtxInfof(ctx, "Ignoring approving pull request review for %s (pull request is already trusted)", wf.WorkflowID)
			return nil
		}
		isApproverTrusted, err := gitProvider.IsTrusted(ctx, wf.AccessToken, wd.TargetRepoURL, wd.PullRequestApprover)
		if err != nil {
			return err
		}
		if !isApproverTrusted {
			log.CtxInfof(ctx, "Ignoring approving pull request review for %s (approver is untrusted)", wf.WorkflowID)
			return nil
		}
		isTrusted = true
	}
	apiKey, err := ws.apiKeyForWorkflow(ctx, wf)
	if err != nil {
		return err
	}
	// Fetch the workflow config (buildbuddy.yaml) and start a CI runner execution
	// for each action matching the webhook event.
	cfg, err := ws.fetchWorkflowConfig(ctx, gitProvider, wf, wd)
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for _, action := range cfg.Actions {
		action := action
		if !config.MatchesAnyTrigger(action, wd.EventName, wd.TargetBranch) {
			jt, _ := json.Marshal(action.Triggers)
			log.CtxDebugf(ctx, "Action %s not matched, triggers=%s", action.Name, string(jt))
			continue
		}
		invocationUUID, err := guuid.NewRandom()
		if err != nil {
			return err
		}
		invocationID := invocationUUID.String()

		// Start executions in parallel to help reduce workflow start latency
		// for repos with lots of workflow actions.
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := ws.executeWorkflowAction(ctx, apiKey, wf, wd, isTrusted, action, invocationID, nil /*=extraCIRunnerArgs*/); err != nil {
				log.CtxErrorf(ctx, "Failed to execute workflow %s (%s) action %q: %s", wf.WorkflowID, wf.RepoURL, action.Name, err)
			}
		}()
	}
	wg.Wait()
	return nil
}

// Starts a CI runner execution to execute a single workflow action, and returns the execution ID.
func (ws *workflowService) executeWorkflowAction(ctx context.Context, key *tables.APIKey, wf *tables.Workflow, wd *interfaces.WebhookData, isTrusted bool, action *config.Action, invocationID string, extraCIRunnerArgs []string) (string, error) {
	opts := retry.DefaultOptions()
	opts.MaxRetries = executeWorkflowMaxRetries
	r := retry.New(ctx, opts)
	var lastErr error
	for r.Next() {
		executionID, err := ws.attemptExecuteWorkflowAction(ctx, key, wf, wd, isTrusted, action, invocationID, nil /*=extraCIRunnerArgs*/)
		if err == ApprovalRequired {
			log.CtxInfof(ctx, "Skipping workflow action %s (%s) %q (requires approval)", wf.WorkflowID, wf.RepoURL, action.Name)
			if err := ws.createApprovalRequiredStatus(ctx, wf, wd, action.Name); err != nil {
				log.CtxErrorf(ctx, "Failed to create 'approval required' status: %s", err)
			}
			return "", nil
		}
		if err != nil {
			// TODO: Create a UI for these errors instead of just logging on the
			// server.
			log.CtxWarningf(ctx, "Failed to execute workflow action %q: %s", action.Name, err)
			lastErr = err
			continue // retry
		}

		return executionID, nil
	}
	return "", lastErr
}

func (ws *workflowService) attemptExecuteWorkflowAction(ctx context.Context, key *tables.APIKey, wf *tables.Workflow, wd *interfaces.WebhookData, isTrusted bool, workflowAction *config.Action, invocationID string, extraCIRunnerArgs []string) (string, error) {
	ctx = ws.env.GetAuthenticator().AuthContextFromAPIKey(ctx, key.Value)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, ws.env)
	if err != nil {
		return "", err
	}
	in := instanceName(wf, wd, workflowAction.Name, workflowAction.GitCleanExclude)
	ad, err := ws.createActionForWorkflow(ctx, wf, wd, isTrusted, key, in, workflowAction, invocationID, extraCIRunnerArgs)
	if err != nil {
		return "", err
	}

	execCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{ToolInvocationId: invocationID})
	if err != nil {
		return "", err
	}
	if isTrusted {
		headerEnv := []*repb.Command_EnvironmentVariable{
			{Name: "BUILDBUDDY_API_KEY", Value: key.Value},
			{Name: "REPO_USER", Value: wf.Username},
			{Name: "REPO_TOKEN", Value: wf.AccessToken},
		}
		execCtx = withEnvOverrides(execCtx, headerEnv)
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
	log.CtxInfof(ctx, "Enqueued workflow execution (WFID: %q, Repo: %q, PushedBranch: %s, Action: %q, TaskID: %q)", wf.WorkflowID, wf.RepoURL, wd.PushedBranch, workflowAction.Name, op.GetName())
	metrics.WebhookHandlerWorkflowsStarted.With(prometheus.Labels{
		metrics.WebhookEventName: wd.EventName,
	}).Inc()

	if err := ws.createQueuedStatus(ctx, wf, wd, workflowAction.Name, invocationID); err != nil {
		log.CtxWarningf(ctx, "Failed to publish workflow action queued status to GitHub: %s", err)
	}

	return op.GetName(), nil
}

func (ws *workflowService) createApprovalRequiredStatus(ctx context.Context, wf *tables.Workflow, wd *interfaces.WebhookData, actionName string) error {
	// TODO: Create a help section in the docs that explains this error status, and link to it
	status := github.NewGithubStatusPayload(actionName, ws.bbUrl.String(), "Check requires approving review", github.ErrorState)
	ownerRepo, err := gitutil.OwnerRepoFromRepoURL(wd.TargetRepoURL)
	if err != nil {
		return err
	}
	ghc := github.NewGithubClient(ws.env, wf.AccessToken)
	return ghc.CreateStatus(ctx, ownerRepo, wd.SHA, status)
}

func (ws *workflowService) createQueuedStatus(ctx context.Context, wf *tables.Workflow, wd *interfaces.WebhookData, actionName, invocationID string) error {
	invocationURL := ws.bbUrl.ResolveReference(&url.URL{Path: "/invocation/" + invocationID})
	invocationURL.RawQuery = "queued=true"
	status := github.NewGithubStatusPayload(actionName, invocationURL.String(), "Queued...", github.PendingState)
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

func workflowHomeDir(user string) string {
	if user == "buildbuddy" {
		return "/home/buildbuddy"
	}
	return "/root"
}

func (ws *workflowService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	workflowMatch := workflowURLMatcher.FindStringSubmatch(r.URL.Path)
	if len(workflowMatch) != 2 {
		http.Error(w, "workflow URL not recognized", http.StatusNotFound)
		return
	}
	webhookID := workflowMatch[1]
	ctx := r.Context()
	ctx = log.EnrichContext(ctx, "github_delivery", r.Header.Get("X-GitHub-Delivery"))
	if err := ws.startLegacyWorkflow(ctx, webhookID, r); err != nil {
		log.Errorf("Failed to start workflow (webhook ID: %q): %s", webhookID, err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Write([]byte("OK"))
}

type repositoryWorkflow struct {
	GitRepository *tables.GitRepository
	Workflow      *tables.Workflow
}

// Adapts a GitRepository to a legacy Workflow struct.
func (ws *workflowService) gitRepositoryWorkflow(repo *tables.GitRepository, accessToken string) *repositoryWorkflow {
	// Construct an artificial workflow ID which identifies this workflow with
	// the original GitRepository row.
	repositoryID := ws.GetLegacyWorkflowIDForGitRepository(repo.GroupID, repo.RepoURL)
	wf := &tables.Workflow{
		WorkflowID:         repositoryID,
		UserID:             repo.UserID,
		GroupID:            repo.GroupID,
		Perms:              repo.Perms,
		RepoURL:            repo.RepoURL,
		InstanceNameSuffix: repo.InstanceNameSuffix,
		AccessToken:        accessToken,
	}
	return &repositoryWorkflow{GitRepository: repo, Workflow: wf}
}

func isRepositoryWorkflowID(id string) bool {
	_, _, err := parseRepositoryWorkflowID(id)
	return err == nil
}

func parseRepositoryWorkflowID(id string) (groupID string, repoURL *gitutil.RepoURL, err error) {
	parts := strings.SplitN(id, ":", 3)
	if len(parts) != 3 || parts[0] != repoWorkflowIDPrefix || parts[1] == "" || parts[2] == "" {
		return "", nil, status.InvalidArgumentErrorf("invalid repository ID: expected '%s:<group_id>:<repo_url>'", repoWorkflowIDPrefix)
	}
	groupID = parts[1]
	repoURL, err = gitutil.ParseGitHubRepoURL(parts[2])
	if err != nil {
		return "", nil, status.InvalidArgumentErrorf("invalid repository ID: failed to parse repo URL: %s", err)
	}
	return groupID, repoURL, nil
}

func withEnvOverrides(ctx context.Context, env []*repb.Command_EnvironmentVariable) context.Context {
	assignments := make([]string, 0, len(env))
	for _, e := range env {
		assignments = append(assignments, e.Name+"="+e.Value)
	}
	return platform.WithRemoteHeaderOverride(
		ctx, platform.EnvOverridesPropertyName, strings.Join(assignments, ","))
}
