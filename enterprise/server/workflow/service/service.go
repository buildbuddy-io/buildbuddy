package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/ci_runner_util"
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
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/authutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/background"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"
	"github.com/buildbuddy-io/buildbuddy/server/util/retry"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/subdomain"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/oauth2"
	"google.golang.org/genproto/googleapis/longrunning"
	"google.golang.org/protobuf/types/known/durationpb"
	"gopkg.in/yaml.v2"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
	remote_execution_config "github.com/buildbuddy-io/buildbuddy/server/remote_execution/config"
	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	githubapi "github.com/google/go-github/v59/github"
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
	enableKytheIndexing           = flag.Bool("remote_execution.enable_kythe_indexing", false, "If set, and codesearch is enabled, automatically run a kythe indexing action.")
	workflowURLMatcher            = regexp.MustCompile(`^.*/webhooks/workflow/(?P<instance_name>.*)$`)

	// ApprovalRequired is an error indicating that a workflow action could not be
	// run at a commit because it is untrusted. An approving review at the
	// commit will allow the action to run.
	ApprovalRequired = status.PermissionDeniedErrorf("approval required")
)

const (
	// A workflow ID prefix that identifies a Workflow as being a
	// "synthetic" workflow adapted from a GitRepository.
	repoWorkflowIDPrefix = "WF#GitRepository"

	// Non-root user that has been pre-provisioned in workflow images.
	nonRootUser = "buildbuddy"

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

	// Additional timeout allowed in addition to user timeout specified in
	// buildbuddy.yaml (or the default timeout). This is long enough to allow
	// some time for the action setup (e.g. pulling the VM snapshot) as well as
	// some extra time for the CI runner to finish publishing the "outer"
	// workflow invocation results after the user-specified timeout is reached.
	timeoutGracePeriod = 10 * time.Minute
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
	b := sha256.Sum256([]byte(strings.Join(keys, "|")))
	return hex.EncodeToString(b[:])
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

	if err := ws.startWorkflow(ctx, task.gitProvider, task.webhookData, task.workflow, nil /*env*/); err != nil {
		log.CtxErrorf(ctx, "Failed to start workflow in the background: %s", err)
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
	if _, err := ws.env.GetAuthenticator().AuthenticatedUser(ctx); err != nil {
		return err
	}

	return nil
}

func (ws *workflowService) DeleteLegacyWorkflow(ctx context.Context, req *wfpb.DeleteWorkflowRequest) (*wfpb.DeleteWorkflowResponse, error) {
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
	err = ws.env.GetDBHandle().Transaction(ctx, func(tx interfaces.DB) error {
		var q interfaces.DBRawQuery
		if req.GetId() != "" {
			q = tx.NewQuery(ctx, "workflow_get_for_delete_by_id").Raw(`
				SELECT * FROM "Workflows" WHERE workflow_id = ?
				`+ws.env.GetDBHandle().SelectForUpdateModifier()+`
			`, req.GetId())
		} else {
			q = tx.NewQuery(ctx, "workflow_get_for_delete_by_repo").Raw(`
				SELECT * FROM "Workflows"
				WHERE group_id = ? AND repo_url = ?
				`+ws.env.GetDBHandle().SelectForUpdateModifier()+`
			`, authenticatedUser.GetGroupID(), req.GetRepoUrl())
		}
		if err := q.Take(&wf); err != nil {
			return err
		}
		acl := perms.ToACLProto(&uidpb.UserId{Id: wf.UserID}, wf.GroupID, wf.Perms)
		if err := perms.AuthorizeWrite(&authenticatedUser, acl); err != nil {
			return err
		}
		return tx.NewQuery(ctx, "workflow_delete").Raw(
			`DELETE FROM "Workflows" WHERE workflow_id = ?`, wf.WorkflowID).Exec().Error
	})
	if err != nil {
		return nil, err
	}
	if wf.GitProviderWebhookID != "" {
		const errMsg = "Failed to remove the webhook from the repo; it may need to be removed manually via the repo's settings page"

		provider, err := ws.providerForRepo(wf.RepoURL)
		if err != nil {
			return nil, status.WrapError(err, errMsg)
		}
		if err := provider.UnregisterWebhook(ctx, wf.AccessToken, wf.RepoURL, wf.GitProviderWebhookID); err != nil {
			return nil, status.WrapError(err, errMsg)
		}
	}
	return &wfpb.DeleteWorkflowResponse{}, nil
}

func (ws *workflowService) GetLinkedLegacyWorkflows(ctx context.Context, accessToken string) ([]string, error) {
	q, args := query_builder.
		NewQuery(`SELECT workflow_id FROM "Workflows"`).
		AddWhereClause("access_token = ?", accessToken).
		Build()
	rq := ws.env.GetDBHandle().NewQuery(ctx, "workflow_service_get_linked_workflows").Raw(q, args...)
	var ids []string
	err := rq.IterateRaw(func(ctx context.Context, row *sql.Rows) error {
		id := ""
		if err := row.Scan(&id); err != nil {
			return err
		}
		ids = append(ids, id)
		return nil
	})
	return ids, err
}

func (ws *workflowService) providerForRepo(repoURL string) (interfaces.GitProvider, error) {
	u, err := gitutil.NormalizeRepoURL(repoURL)
	if err != nil {
		return nil, status.WrapError(err, "parse repo URL")
	}
	for _, provider := range ws.env.GetGitProviders() {
		if provider.MatchRepoURL(u) {
			return provider, nil
		}
	}
	return nil, status.InvalidArgumentErrorf("could not find git provider for %s", u.Hostname())
}

func (ws *workflowService) GetLegacyWorkflows(ctx context.Context) (*wfpb.GetWorkflowsResponse, error) {
	if err := ws.checkPreconditions(ctx); err != nil {
		return nil, err
	}

	u, err := ws.env.GetAuthenticator().AuthenticatedUser(ctx)
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
	rq := ws.env.GetDBHandle().NewQuery(ctx, "workflow_get_workflows").Raw(qStr, qArgs...)
	rsp.Workflow = make([]*wfpb.GetWorkflowsResponse_Workflow, 0)
	err = db.ScanEach(rq, func(ctx context.Context, tw *tables.Workflow) error {
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
	if req.GetPushedBranch() == "" && req.GetCommitSha() == "" {
		return nil, status.InvalidArgumentError("At least one of pushed_branch or commit_sha must be set.")
	}

	// Authenticate
	user, err := ws.env.GetAuthenticator().AuthenticatedUser(ctx)
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
		EventName:         webhook_data.EventName.ManualDispatch,
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
	for _, action := range actions {
		action := action
		actionStatus := &wfpb.ExecuteWorkflowResponse_ActionStatus{
			ActionName: action.Name,
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
				statusErr = status.NotFoundErrorf("action %s not found", action.Name)
				return
			}

			invocationUUID, err := guuid.NewRandom()
			if err != nil {
				statusErr = status.InternalErrorf("failed to generate invocation ID: %s", err)
				log.CtxError(ctx, statusErr.Error())
				return
			}
			invocationID = invocationUUID.String()
			executionCtx := log.EnrichContext(ctx, log.InvocationIDKey, invocationID)

			// The workflow execution is trusted since we're authenticated as a member of
			// the BuildBuddy org that owns the workflow.
			isTrusted := true
			shouldRetry := !req.GetDisableRetry()
			executionID, err := ws.executeWorkflowAction(executionCtx, apiKey, wf, wd, isTrusted, action, invocationID, extraCIRunnerArgs, req.GetEnv(), shouldRetry)
			if err != nil {
				statusErr = status.WrapErrorf(err, "failed to execute workflow action %q", action.Name)
				log.CtxWarning(executionCtx, statusErr.Error())
				return
			}
			executionCtx = log.EnrichContext(executionCtx, log.ExecutionIDKey, executionID)
			if req.GetAsync() {
				return
			}
			if err := ws.waitForWorkflowInvocationCreated(executionCtx, executionID, invocationID); err != nil {
				statusErr = err
				log.CtxWarning(executionCtx, statusErr.Error())
				return
			}
		}()
	}
	wg.Wait()

	return &wfpb.ExecuteWorkflowResponse{
		ActionStatuses: actionStatuses,
	}, nil
}

// getActions fetches the workflow config (buildbuddy.yaml) and returns the list of
// actions matching the webhook event
func (ws *workflowService) getActions(ctx context.Context, wf *tables.Workflow, wd *interfaces.WebhookData, actionFilter []string) ([]*config.Action, error) {
	// Fetch the workflow config
	gitProvider, err := ws.providerForRepo(wd.PushedRepoURL)
	if err != nil {
		return nil, err
	}
	cfg, err := ws.fetchWorkflowConfig(ctx, gitProvider, wf, wd)
	if err != nil {
		return nil, status.WrapError(err, "fetch workflow config")
	} else if cfg == nil {
		// If there is no error and no config, the user does not have one configured.
		// Do nothing in this case.
		return nil, nil
	}

	var actions []*config.Action
	for _, a := range cfg.Actions {
		matchesActionName := len(actionFilter) == 0 || config.MatchesAnyActionName(a, actionFilter)
		matchesTrigger := config.MatchesAnyTrigger(a, wd.EventName, wd.TargetBranch)
		if matchesActionName && matchesTrigger {
			actions = append(actions, a)
		}
	}
	if len(actions) == 0 {
		if len(actionFilter) == 0 {
			return nil, status.NotFoundError("no workflow actions found")
		} else {
			return nil, status.NotFoundErrorf("requested workflow actions %v not found", actionFilter)
		}
	}

	return actions, nil
}

func (ws *workflowService) getWorkflowByID(ctx context.Context, workflowID string) (*tables.Workflow, error) {
	isLegacyWorkflow := !isRepositoryWorkflowID(workflowID)
	if isLegacyWorkflow {
		wf := &tables.Workflow{}
		err := ws.env.GetDBHandle().NewQuery(ctx, "workflow_service_get_by_id").Raw(
			`SELECT * FROM Workflows WHERE workflow_id = ?`,
			workflowID,
		).Take(wf)
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

// InvalidateAllSnapshotsForRepo updates the instance name suffix for the repo.
// As the instance name suffix is used in the snapshot key, this invalidates
// all previous workflows.
func (ws *workflowService) InvalidateAllSnapshotsForRepo(ctx context.Context, repoURL string) error {
	u, err := ws.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return err
	}

	suffix, err := random.RandomString(10)
	if err != nil {
		return err
	}

	log.CtxInfof(ctx, "Workflow clean run requested for repo %q (group %s)", repoURL, u.GetGroupID())
	err = ws.env.GetDBHandle().NewQuery(ctx, "workflow_service_update_repo_instance_name").Raw(`
				UPDATE GitRepositories
				SET instance_name_suffix = ?
				WHERE group_id = ? AND repo_url = ?`,
		suffix, u.GetGroupID(), repoURL,
	).Exec().Error

	return err
}

func (ws *workflowService) addCodesearchActionsIfEnabled(ctx context.Context, c *config.BuildBuddyConfig, workflow *tables.Workflow, wd *interfaces.WebhookData) error {

	enableCS, err := ws.isCodesearchIndexingEnabled(ctx, workflow.GroupID)
	if err != nil {
		return err
	}
	if enableCS {
		// TODO(jdelfino): Using the cache API URL here is hacky, long term we might want a codesearch_api_url
		c.Actions = append(c.Actions, config.CodesearchIncrementalUpdateAction(cache_api_url.WithPath(""), workflow.RepoURL, wd.TargetRepoDefaultBranch))
		c.Actions = append(c.Actions, config.KytheIndexingAction(wd.TargetRepoDefaultBranch))
	}
	return nil
}

func (ws *workflowService) isCodesearchIndexingEnabled(ctx context.Context, groupID string) (bool, error) {
	if !*enableKytheIndexing {
		return false, nil
	}
	g, err := ws.env.GetUserDB().GetGroupByID(ctx, groupID)
	if err != nil {
		return false, err
	}
	return g.CodeSearchEnabled, nil
}

func (ws *workflowService) getRepositoryWorkflow(ctx context.Context, groupID string, repoURL *gitutil.RepoURL) (*repositoryWorkflow, error) {
	gh := ws.env.GetGitHubAppService()
	if gh == nil {
		return nil, status.UnimplementedError("No GitHub app configured")
	}
	app, err := gh.GetGitHubAppForOwner(ctx, repoURL.Owner)
	if err != nil {
		return nil, err
	}
	if err := authutil.AuthorizeGroupAccess(ctx, ws.env, groupID); err != nil {
		return nil, err
	}
	gitRepository := &tables.GitRepository{}
	err = ws.env.GetDBHandle().NewQuery(ctx, "workflow_service_get_for_repo").Raw(`
		SELECT *
		FROM "GitRepositories"
		WHERE group_id = ?
		AND repo_url = ?
	`, groupID, repoURL.String()).Take(gitRepository)
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

	log.CtxInfof(ctx, "Waiting for workflow invocation to be created")
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
			// continue with for loop
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

func (ws *workflowService) buildActionHistoryQuery(ctx context.Context, repoUrl string, pattern string, timeLimitMicros int64, onlyMainBranches bool) (*query_builder.Query, error) {
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
	if onlyMainBranches {
		q.AddWhereClause("branch_name IN ?", []string{"master", "main"})
	}
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
	} else if ws.env.GetGitHubAppService() == nil {
		return nil, status.FailedPreconditionError("GitHub app service not enabled")
	}
	gh := ws.env.GetGitHubAppService()
	if gh == nil {
		return nil, status.UnimplementedError("No GitHub app configured")
	}

	linkedRepos, err := gh.GetLinkedGitHubRepos(ctx)
	if err != nil {
		return nil, err
	}
	repos := linkedRepos.GetRepoUrls()
	if len(repos) == 0 {
		// Fall back to legacy workflow registrations.
		repos = []string{}
		workflows, err := ws.GetLegacyWorkflows(ctx)
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
	q := query_builder.NewQuery(`SELECT repo_url,pattern,count(1) AS total_runs, countIf(success) AS successful_runs, toInt64(avg(duration_usec)) AS average_duration, countIf(branch_name IN ('master', 'main')) AS main_branch_runs FROM Invocations`)
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
		MainBranchRuns  int64
	}

	qStr, qArgs := q.Build()
	rq := ws.env.GetOLAPDBHandle().NewQuery(ctx, "workflow_service_get_actions").Raw(qStr, qArgs...)

	actionHistoryQArgs := make([]interface{}, 0)
	actionHistoryQStrs := make([]string, 0)
	workflows := make(map[string]map[string]*wfpb.ActionHistory)

	err = db.ScanEach(rq, func(ctx context.Context, row *actionQueryOut) error {
		onlyFetchMainBranchRuns := row.MainBranchRuns > 0
		q, err := ws.buildActionHistoryQuery(ctx, row.RepoUrl, row.Pattern, timeLimitMicros, onlyFetchMainBranchRuns)
		if err != nil {
			return err
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
		return nil
	})
	if err != nil {
		return nil, status.WrapError(err, "failed to fetch workflow action summary")
	}

	// No workflows configured for any repos yet.
	if len(actionHistoryQStrs) == 0 {
		return &wfpb.GetWorkflowHistoryResponse{}, nil
	}
	finalActionHistoryQStr := strings.Join(actionHistoryQStrs, " UNION ALL ")

	rq = ws.env.GetDBHandle().NewQuery(ctx, "workflow_service_query_history").Raw(
		finalActionHistoryQStr, actionHistoryQArgs...)

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

	err = db.ScanEach(rq, func(ctx context.Context, row *historyQueryOut) error {
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
		return nil
	})
	if err != nil {
		return nil, status.WrapError(err, "failed to fetch workflow action history")
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

func (ws *workflowService) GetReposForLegacyGitHubApp(ctx context.Context, req *wfpb.GetReposRequest) (*wfpb.GetReposResponse, error) {
	if req.GetGitProvider() == wfpb.GitProvider_UNKNOWN_GIT_PROVIDER {
		return nil, status.FailedPreconditionError("Unknown git provider")
	}
	token, err := ws.legacyGithubTokenForAuthorizedGroup(ctx, req.GetRequestContext())
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

func (ws *workflowService) legacyGithubTokenForAuthorizedGroup(ctx context.Context, reqCtx *ctxpb.RequestContext) (string, error) {
	d := ws.env.GetUserDB()
	if d == nil {
		return "", status.FailedPreconditionError("Missing UserDB")
	}
	u, err := ws.env.GetAuthenticator().AuthenticatedUser(ctx)
	if err != nil {
		return "", err
	}
	g, err := d.GetGroupByID(ctx, u.GetGroupID())
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
	rq := ws.env.GetDBHandle().NewQuery(ctx, "workflow_service_get_workflow_for_webhook").Raw(`
		SELECT * FROM "Workflows" WHERE webhook_id = ?`, webhookID)
	if err := rq.Take(tw); err != nil {
		if db.IsRecordNotFound(err) {
			return nil, status.NotFoundErrorf("workflow not found for id %q", webhookID)
		}
		return nil, err
	}
	return tw, nil
}

func (ws *workflowService) createBBURL(ctx context.Context, path string) (string, error) {
	return subdomain.ReplaceURLSubdomain(ctx, ws.env, build_buddy_url.WithPath(path).String())
}

// Creates an action that executes the CI runner for the given workflow and params.
// Returns the digest of the action as well as the invocation ID that the CI runner
// will assign to the workflow invocation.
func (ws *workflowService) createActionForWorkflow(ctx context.Context, wf *tables.Workflow, wd *interfaces.WebhookData, isTrusted bool, ak *tables.APIKey, instanceName string, workflowAction *config.Action, invocationID string, extraArgs []string, env map[string]string, retry bool) (*repb.Digest, error) {
	cache := ws.env.GetCache()
	if cache == nil {
		return nil, status.UnavailableError("No cache configured.")
	}

	envVars := []*repb.Command_EnvironmentVariable{
		{Name: "CI", Value: "true"},
		{Name: "GIT_COMMIT", Value: wd.SHA},
		{Name: "GIT_BRANCH", Value: wd.PushedBranch},
		{Name: "GIT_BASE_BRANCH", Value: wd.TargetBranch},
		{Name: "GIT_REPO_DEFAULT_BRANCH", Value: wd.TargetRepoDefaultBranch},
		{Name: "GIT_PR_NUMBER", Value: fmt.Sprintf("%d", wd.PullRequestNumber)},
		{Name: "BUILDBUDDY_INVOCATION_ID", Value: invocationID},
	}
	for k, v := range workflowAction.Env {
		envVars = append(envVars, &repb.Command_EnvironmentVariable{
			Name:  k,
			Value: v,
		})
	}
	for k, v := range env {
		envVars = append(envVars, &repb.Command_EnvironmentVariable{
			Name:  k,
			Value: v,
		})
	}
	workflowUser := workflowAction.User
	if workflowUser == "" && wf.GitRepository != nil && wf.GitRepository.DefaultNonRootRunner {
		workflowUser = nonRootUser
	}

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
			wd := filepath.Join(workflowHomeDir(workflowUser), "workspace")
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
	besResultsURL, err := ws.createBBURL(ctx, "/invocation/")
	if err != nil {
		return nil, err
	}

	timeout := *ci_runner_util.CIRunnerDefaultTimeout
	if workflowAction.Timeout != nil {
		timeout = *workflowAction.Timeout
	}

	inputRootDigest, err := ci_runner_util.UploadInputRoot(ctx, ws.env.GetByteStreamClient(), ws.env.GetCache(), instanceName, os, workflowAction.Arch)
	if err != nil {
		return nil, err
	}

	yamlBytes, err := yaml.Marshal(workflowAction)
	if err != nil {
		return nil, err
	}
	serializedAction := base64.StdEncoding.EncodeToString(yamlBytes)

	args := []string{
		"./" + ci_runner_util.ExecutableName,
		"--invocation_id=" + invocationID,
		"--action_name=" + workflowAction.Name,
		"--bes_backend=" + events_api_url.String(),
		"--bes_results_url=" + besResultsURL,
		"--cache_backend=" + cache_api_url.String(),
		"--rbe_backend=" + remote_exec_api_url.String(),
		"--remote_instance_name=" + instanceName,
		"--digest_function=" + repb.DigestFunction_BLAKE3.String(),
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
		"--timeout=" + timeout.String(),
		"--serialized_action=" + serializedAction,
	}

	// Recycle workflow runners by default, but not Kythe ones, to avoid
	// filling the cache with crap.
	enableRunnerRecycling := workflowAction.Name != config.KytheActionName

	for _, filter := range workflowAction.GetGitFetchFilters() {
		args = append(args, "--git_fetch_filters="+filter)
	}
	if workflowAction.GitFetchDepth != nil {
		args = append(args, fmt.Sprintf("--git_fetch_depth=%d", *workflowAction.GitFetchDepth))
	}
	for _, path := range workflowAction.GitCleanExclude {
		args = append(args, "--git_clean_exclude="+path)
	}
	args = append(args, extraArgs...)

	cmd := &repb.Command{
		EnvironmentVariables: envVars,
		Arguments:            args,
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "Pool", Value: ws.poolForAction(workflowAction)},
				{Name: "OSFamily", Value: os},
				{Name: "Arch", Value: workflowAction.Arch},
				{Name: platform.DockerUserPropertyName, Value: workflowUser},
				{Name: "workload-isolation-type", Value: isolationType},
				{Name: "container-image", Value: containerImage},
				{Name: "use-self-hosted-executors", Value: useSelfHostedExecutors},

				// Pass the workflow ID to the executor so that it can try to assign
				// this task to a runner which has previously executed the workflow.
				{Name: "workflow-id", Value: wf.WorkflowID},
				{Name: platform.IncludeSecretsPropertyName, Value: includeSecretsPropertyValue},
				{Name: platform.EstimatedComputeUnitsPropertyName, Value: fmt.Sprintf("%d", computeUnits)},
				{Name: platform.EstimatedFreeDiskPropertyName, Value: estimatedDisk},
				{Name: platform.EstimatedMemoryPropertyName, Value: workflowAction.ResourceRequests.GetEstimatedMemory()},
				{Name: platform.EstimatedCPUPropertyName, Value: workflowAction.ResourceRequests.GetEstimatedCPU()},
				{Name: platform.RetryPropertyName, Value: fmt.Sprintf("%v", retry)},
			},
		},
	}

	if enableRunnerRecycling {
		// Reuse the container/VM for the CI runner across executions if
		// possible, and also keep the git repo around so it doesn't need to be
		// re-cloned each time.
		cmd.Platform.Properties = append(cmd.Platform.Properties, []*repb.Platform_Property{
			{Name: "recycle-runner", Value: "true"},
			{Name: "runner-recycling-max-wait", Value: (*ci_runner_util.RecycledCIRunnerMaxWait).String()},
			{Name: "preserve-workspace", Value: "true"},
		}...)
	}

	if isSharedFirecrackerWorkflow {
		// For firecracker workflows, init dockerd in case local actions or
		// setup scripts want to use it.
		cmd.Platform.Properties = append(cmd.Platform.Properties, &repb.Platform_Property{
			Name:  "init-dockerd",
			Value: "true",
		})
	} else {
		// For docker/podman workflows, run with `--init` so that the bazel
		// process can be reaped.
		cmd.Platform.Properties = append(cmd.Platform.Properties, &repb.Platform_Property{
			Name:  platform.DockerInitPropertyName,
			Value: "true",
		})
	}

	customPlatformProps := make([]*repb.Platform_Property, 0, len(workflowAction.PlatformProperties))
	for k, v := range workflowAction.PlatformProperties {
		customPlatformProps = append(customPlatformProps, &repb.Platform_Property{
			Name:  k,
			Value: v,
		})
	}
	cmd.Platform.Properties = append(cmd.Platform.Properties, customPlatformProps...)
	rexec.NormalizeCommand(cmd)

	cmdDigest, err := cachetools.UploadProtoToCAS(ctx, cache, instanceName, repb.DigestFunction_BLAKE3, cmd)
	if err != nil {
		return nil, err
	}
	action := &repb.Action{
		CommandDigest:   cmdDigest,
		InputRootDigest: inputRootDigest,
		DoNotCache:      true,
		// Set the action timeout slightly longer than the CI runner timeout, so
		// that we allow the CI runner to finalize the outer workflow invocation
		// once the timeout has elapsed, but if the CI runner takes too long to
		// finalize, we can still kill the action.
		Timeout:  durationpb.New(timeout + timeoutGracePeriod),
		Platform: cmd.GetPlatform(),
	}

	actionDigest, err := cachetools.UploadProtoToCAS(ctx, cache, instanceName, repb.DigestFunction_BLAKE3, action)
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
	if value == "ubuntu-22.04" {
		return platform.DockerPrefix + platform.Ubuntu22_04WorkflowsImage
	}
	if value == "ubuntu-24.04" {
		return platform.DockerPrefix + platform.Ubuntu24_04WorkflowsImage
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
	if ws.env.GetRemoteExecutionClient() == nil {
		return status.UnavailableError("Remote execution not configured.")
	}
	return nil
}

// fetchWorkflowConfig returns the BuildBuddyConfig from the repo, or the
// default BuildBuddyConfig if that setting is enabled.
func (ws *workflowService) fetchWorkflowConfig(ctx context.Context, gitProvider interfaces.GitProvider, workflow *tables.Workflow, webhookData *interfaces.WebhookData) (*config.BuildBuddyConfig, error) {
	workflowRef := webhookData.SHA
	if workflowRef == "" {
		workflowRef = webhookData.PushedBranch
	}

	var c *config.BuildBuddyConfig
	b, err := gitProvider.GetFileContents(ctx, workflow.AccessToken, webhookData.PushedRepoURL, config.FilePath, workflowRef)
	if err == nil {
		c, err = config.NewConfig(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}
	} else {
		if status.IsNotFoundError(err) {
			if workflow.GitRepository != nil && !workflow.GitRepository.UseDefaultWorkflowConfig {
				return nil, nil
			}

			c = config.GetDefault(webhookData.TargetRepoDefaultBranch)
		} else {
			return nil, err
		}
	}

	if err := ws.addCodesearchActionsIfEnabled(ctx, c, workflow, webhookData); err != nil {
		return nil, err
	}
	return c, nil
}

func (ws *workflowService) isTrustedCommit(ctx context.Context, gitProvider interfaces.GitProvider, wf *tables.Workflow, wd *interfaces.WebhookData) (bool, error) {
	// If the commit was pushed directly to the target repo then the commit must
	// already be trusted.
	if !isFork(wd) {
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
	provider, err := ws.providerForRepo(repo.RepoURL)
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

func (ws *workflowService) startWorkflow(ctx context.Context, gitProvider interfaces.GitProvider, wd *interfaces.WebhookData, wf *tables.Workflow, env map[string]string) error {
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

	actions, err := ws.getActions(ctx, wf, wd, nil /*actionFilter*/)
	if err != nil {
		if strings.Contains(err.Error(), "fetch workflow config") {
			if err := ws.createWorkflowConfigErrorStatus(ctx, wf, wd); err != nil {
				log.CtxWarningf(ctx, "Failed to create workflow config error status: %s", err)
			}
		}
		return err
	}

	var wg sync.WaitGroup
	for _, action := range actions {
		action := action
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
			// Webhook triggered workflows should always be retried, because they
			// don't have a client to retry for them
			shouldRetry := true
			if _, err := ws.executeWorkflowAction(ctx, apiKey, wf, wd, isTrusted, action, invocationID, nil /*=extraCIRunnerArgs*/, env, shouldRetry); err != nil {
				log.CtxErrorf(ctx, "Failed to execute workflow %s (%s) action %q: %s", wf.WorkflowID, wf.RepoURL, action.Name, err)
			}
		}()
	}
	wg.Wait()
	return nil
}

// Starts a CI runner execution to execute a single workflow action, and returns the execution ID.
func (ws *workflowService) executeWorkflowAction(ctx context.Context, key *tables.APIKey, wf *tables.Workflow, wd *interfaces.WebhookData, isTrusted bool, action *config.Action, invocationID string, extraCIRunnerArgs []string, env map[string]string, shouldRetry bool) (string, error) {
	opts := retry.DefaultOptions()
	opts.MaxRetries = executeWorkflowMaxRetries
	r := retry.New(ctx, opts)
	var lastErr error
	for r.Next() {
		executionID, err := ws.attemptExecuteWorkflowAction(ctx, key, wf, wd, isTrusted, action, invocationID, nil /*=extraCIRunnerArgs*/, env, shouldRetry)
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

func (ws *workflowService) attemptExecuteWorkflowAction(ctx context.Context, key *tables.APIKey, wf *tables.Workflow, wd *interfaces.WebhookData, isTrusted bool, workflowAction *config.Action, invocationID string, extraCIRunnerArgs []string, env map[string]string, retry bool) (string, error) {
	ctx = ws.env.GetAuthenticator().AuthContextFromAPIKey(ctx, key.Value)
	ctx, err := prefix.AttachUserPrefixToContext(ctx, ws.env.GetAuthenticator())
	if err != nil {
		return "", err
	}
	in := instanceName(wf, wd, workflowAction.Name, workflowAction.GitCleanExclude)
	ad, err := ws.createActionForWorkflow(ctx, wf, wd, isTrusted, key, in, workflowAction, invocationID, extraCIRunnerArgs, env, retry)
	if err != nil {
		return "", err
	}

	execCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
		ToolInvocationId: invocationID,
		ActionMnemonic:   "BuildBuddyWorkflowRun",
	})
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
		DigestFunction:  repb.DigestFunction_BLAKE3,
		ExecutionPolicy: &repb.ExecutionPolicy{
			Priority: int32(workflowAction.Priority),
		},
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
	invocationURL, err := ws.createBBURL(ctx, "/invocation/"+invocationID)
	if err != nil {
		return err
	}
	invocationURL += "?queued=true"
	status := github.NewGithubStatusPayload(actionName, invocationURL, "Queued...", github.PendingState)
	statusReportingURL := getStatusReportingURL(wd)
	provider, err := ws.providerForRepo(statusReportingURL)
	if err != nil {
		return err
	}
	return provider.CreateStatus(ctx, wf.AccessToken, statusReportingURL, wd.SHA, status)
}

// getStatusReportingURL returns the URL the workflow should report statuses to
func getStatusReportingURL(wd *interfaces.WebhookData) string {
	// If the workflow was triggered by a pull request from a fork, statuses should
	// be reported to the target repo (the repo the fork will be merged into)
	if isFork(wd) {
		return wd.TargetRepoURL
	}
	// If there was not a fork, TargetRepoURL will not be set, or TargetRepoURL
	// and PushedRepoURL will be equal
	return wd.PushedRepoURL
}

func isFork(wd *interfaces.WebhookData) bool {
	return wd.TargetRepoURL != "" && wd.PushedRepoURL != wd.TargetRepoURL
}

func (ws *workflowService) createWorkflowConfigErrorStatus(ctx context.Context, wf *tables.Workflow, wd *interfaces.WebhookData) error {
	// For now just point to docs. Eventually it'd be nice to link to BB code
	// and highlight the YAML syntax error.
	status := github.NewGithubStatusPayload(
		"BuildBuddy Workflows",
		"https://buildbuddy.io/docs/workflows-config",
		"Invalid buildbuddy.yaml",
		github.ErrorState)
	statusReportingURL := getStatusReportingURL(wd)
	provider, err := ws.providerForRepo(statusReportingURL)
	if err != nil {
		return err
	}
	return provider.CreateStatus(ctx, wf.AccessToken, statusReportingURL, wd.SHA, status)
}

func isGitHubURL(s string) bool {
	u, err := url.Parse(s)
	if err != nil {
		return false
	}
	return u.Host == "github.com"
}

func workflowHomeDir(user string) string {
	if user == nonRootUser {
		return "/home/buildbuddy"
	}
	return "/root"
}

// ServeHTTP is a deprecated way to handle webhook events for legacy workflows.
// Modern workflows should use the GitHub app, which should route through
// `HandleRepositoryEvent` instead.
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
		GitRepository:      repo,
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
