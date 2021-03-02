package workflow

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jinzhu/gorm"

	bazelgo "github.com/bazelbuild/rules_go/go/tools/bazel"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	wfpb "github.com/buildbuddy-io/buildbuddy/proto/workflow"
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
func (ws *workflowService) testRepo(ctx context.Context, repoURL, accessToken string) error {
	rdl := ws.env.GetRepoDownloader()
	if rdl != nil {
		return rdl.TestRepoAccess(ctx, repoURL, accessToken)
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
		return nil, status.InvalidArgumentError("A repo_url is required to create a new workflow.")
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
	accessToken := repoReq.GetAccessToken()
	if err := ws.testRepo(ctx, repoURL, accessToken); err != nil {
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
	err = ws.env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
		workflowID, err := tables.PrimaryKeyForTable("Workflows")
		if err != nil {
			return status.InternalError(err.Error())
		}
		rsp.Id = workflowID
		rsp.WebhookUrl = webhookURL
		wf := &tables.Workflow{
			WorkflowID:  workflowID,
			UserID:      permissions.UserID,
			GroupID:     permissions.GroupID,
			Perms:       permissions.Perms,
			Name:        req.GetName(),
			RepoURL:     repoURL,
			AccessToken: accessToken,
			WebhookID:   webhookID,
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
	err = ws.env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
		var in tables.Workflow
		if err := tx.Raw(`SELECT user_id, group_id, perms FROM Workflows WHERE workflow_id = ?`, workflowID).Scan(&in).Error; err != nil {
			return err
		}
		acl := perms.ToACLProto(&uidpb.UserId{Id: in.UserID}, in.GroupID, in.Perms)
		if err := perms.AuthorizeWrite(&authenticatedUser, acl); err != nil {
			return err
		}
		return tx.Exec(`DELETE FROM Workflows WHERE workflow_id = ?`, req.GetId()).Error
	})
	return &wfpb.DeleteWorkflowResponse{}, err
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
	err := ws.env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
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

func (ws *workflowService) createActionForWorkflow(ctx context.Context, wf *tables.Workflow, wd *webhook_data.WebhookData, ak *tables.APIKey, instanceName string) (*repb.Digest, error) {
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
			&repb.FileNode{
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
	envVars := []*repb.Command_EnvironmentVariable{
		{Name: "CI", Value: "1"},
		{Name: "CI_REPOSITORY_URL", Value: wf.RepoURL},
		{Name: "CI_COMMIT_SHA", Value: wd.SHA},
	}
	if wd.IsTrusted() {
		envVars = append(envVars, []*repb.Command_EnvironmentVariable{
			{Name: "BUILDBUDDY_API_KEY", Value: ak.Value},
			// TODO: For BitBucket, this will be user => username, token => app password
			{Name: "REPO_USER", Value: wf.AccessToken},
			{Name: "REPO_TOKEN", Value: ""},
		}...)
	}
	conf := ws.env.GetConfigurator()
	cmd := &repb.Command{
		EnvironmentVariables: envVars,
		Arguments: []string{
			"./" + runnerBinName,
			"--bes_backend=" + conf.GetAppEventsAPIURL(),
			"--bes_results_url=" + conf.GetAppBuildBuddyURL() + "/invocation/",
			"--repo_url=" + wf.RepoURL,
			"--commit_sha=" + wd.SHA,
			"--trigger_event=" + wd.EventName,
			"--trigger_branch=" + wd.TargetBranch,
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
	q.SetLimit(1)
	qStr, qArgs := q.Build()
	k := &tables.APIKey{}
	if err := ws.env.GetDBHandle().Raw(qStr, qArgs...).Scan(&k).Error; err != nil {
		return nil, err
	}
	return k, nil
}

func parseRequest(r *http.Request) (*webhook_data.WebhookData, error) {
	if r.Header.Get("X-Github-Event") != "" {
		return github.ParseRequest(r)
	}

	// TODO: Support non-GitHub providers
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
		log.Printf("error processing webhook request: %s", err)
		return err
	}
	if webhookData == nil {
		return nil
	}
	log.Printf("Parsed webhook payload: %+v", webhookData)
	wf, err := ws.readWorkflowForWebhook(ctx, webhookID)
	if err != nil {
		return err
	}
	log.Printf("Found workflow %v matching webhook %q", wf, webhookID)

	key, err := ws.apiKeyForWorkflow(ctx, wf)
	if err != nil {
		return err
	}
	ctx = ws.env.GetAuthenticator().AuthContextFromAPIKey(ctx, key.Value)
	if ctx, err = prefix.AttachUserPrefixToContext(ctx, ws.env); err != nil {
		return err
	}
	in := instanceName(webhookData)
	ad, err := ws.createActionForWorkflow(ctx, wf, webhookData, key, in)
	if err != nil {
		return err
	}

	executionID, err := ws.env.GetRemoteExecutionService().Dispatch(ctx, &repb.ExecuteRequest{
		InstanceName:    in,
		SkipCacheLookup: true,
		ActionDigest:    ad,
	})
	if err != nil {
		return err
	}
	log.Printf("Started workflow execution (ID: %q)", executionID)
	return nil
}

func (ws *workflowService) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	workflowMatch := workflowURLMatcher.FindStringSubmatch(r.URL.Path)
	if len(workflowMatch) != 2 {
		http.Error(w, "workflow URL not recognized", http.StatusNotFound)
		return
	}
	webhookID := workflowMatch[1]
	if err := ws.startWorkflow(webhookID, r); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	w.Write([]byte("OK"))
}
