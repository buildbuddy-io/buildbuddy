package workflow

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/query_builder"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/jinzhu/gorm"

	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	uidpb "github.com/buildbuddy-io/buildbuddy/proto/user_id"
	guuid "github.com/google/uuid"
)

// getWebhookID returns a string that can be used to uniquely identify a webhook.
func generateWebhookID() (string, error) {
	u, err := guuid.NewRandom()
	if err != nil {
		return "", err
	}
	return strings.ToLower(u.String()), nil
}

// getWebhookURL takes a webhookID and returns a fully qualified URL, on this
// server, where the specified webhook can be called.
func getWebhookURL(env environment.Env, webhookID string) (string, error) {
	base, err := url.Parse(env.GetConfigurator().GetAppBuildBuddyURL())
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
func testRepo(ctx context.Context, env environment.Env, repoURL, accessToken string) error {
	return nil
}

func CreateWorkflow(ctx context.Context, env environment.Env, req *trpb.CreateWorkflowRequest) (*trpb.CreateWorkflowResponse, error) {
	// Validate the request.
	repoReq := req.GetGitRepo()
	if repoReq.GetRepoUrl() == "" {
		return nil, status.InvalidArgumentError("A repo_url is required to create a new workflow.")
	}
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	// Ensure the request is authenticated so some user can own this workflow.
	var permissions *perms.UserGroupPerm
	if auth := env.GetAuthenticator(); auth != nil {
		if u, err := auth.AuthenticatedUser(ctx); err == nil && u.GetGroupID() != "" {
			permissions = perms.GroupAuthPermissions(u.GetGroupID())
		}
	}
	if permissions == nil {
		return nil, status.PermissionDeniedErrorf("Anonymous workflows are not supported.")
	}

	// Do a quick check to see if this is a valid repo that we can actually access.
	repoURL := repoReq.GetRepoUrl()
	accessToken := repoReq.GetAccessToken()
	if err := testRepo(ctx, env, repoURL, accessToken); err != nil {
		return nil, status.UnavailableErrorf("Repo %q is unavailable: %s", repoURL, err.Error())
	}

	webhookID, err := generateWebhookID()
	if err != nil {
		return nil, status.InternalError(err.Error())
	}
	webhookURL, err := getWebhookURL(env, webhookID)
	if err != nil {
		return nil, status.InternalError(err.Error())
	}

	rsp := &trpb.CreateWorkflowResponse{}
	err = env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
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

func DeleteWorkflow(ctx context.Context, env environment.Env, req *trpb.DeleteWorkflowRequest) (*trpb.DeleteWorkflowResponse, error) {
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	workflowID := req.GetId()
	if workflowID == "" {
		return nil, status.InvalidArgumentError("An ID is required to delete a workflow.")
	}
	auth := env.GetAuthenticator()
	if auth == nil {
		return nil, status.UnimplementedError("Not Implemented")
	}
	authenticatedUser, err := auth.AuthenticatedUser(ctx)
	if err != nil {
		return nil, err
	}
	err = env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
		var in tables.Workflow
		if err := tx.Raw(`SELECT user_id, group_id, perms FROM Workflows WHERE workflow_id = ?`, workflowID).Scan(&in).Error; err != nil {
			return err
		}
		acl := perms.ToACLProto(&uidpb.UserId{Id: in.UserID}, in.GroupID, in.Perms)
		if err := perms.AuthorizeWrite(&authenticatedUser, acl); err != nil {
			return err
		}
		return tx.Exec(`DELETE FROM Workflows WHERE invocation_id = ?`, req.GetId()).Error
	})
	return &trpb.DeleteWorkflowResponse{}, err
}

func ListWorkflow(ctx context.Context, env environment.Env, req *trpb.ListWorkflowRequest) (*trpb.ListWorkflowResponse, error) {
	if env.GetDBHandle() == nil {
		return nil, status.FailedPreconditionError("database not configured")
	}
	rsp := &trpb.ListWorkflowResponse{}
	q := query_builder.NewQuery(`SELECT workflow_id, name, repo_url FROM Workflows`)
	// Adds user / permissions check.
	if err := perms.AddPermissionsCheckToQuery(ctx, env, q); err != nil {
		return nil, err
	}
	qStr, qArgs := q.Build()
	err := env.GetDBHandle().Transaction(func(tx *gorm.DB) error {
		rows, err := tx.Raw(qStr, qArgs...).Rows()
		if err != nil {
			return err
		}
		defer rows.Close()

		rsp.Workflow = make([]*trpb.ListWorkflowResponse_Workflow, 0)
		for rows.Next() {
			wf := &trpb.ListWorkflowResponse_Workflow{}
			if err := tx.ScanRows(rows, &wf); err != nil {
				return err
			}
			rsp.Workflow = append(rsp.Workflow, wf)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return rsp, nil
}
