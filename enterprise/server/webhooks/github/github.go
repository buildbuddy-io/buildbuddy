package github

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/fieldgetter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/oauth2"

	gitutil "github.com/buildbuddy-io/buildbuddy/server/util/git"
	"github.com/google/go-github/v43/github"
	gh "github.com/google/go-github/v43/github"
)

var (
	// GitHub event names to listen for on the webhook.
	eventsToReceive = []string{"push", "pull_request", "pull_request_review"}
)

type githubGitProvider struct{}

func NewProvider() interfaces.GitProvider {
	return &githubGitProvider{}
}

func NewGitHubClient(ctx context.Context, accessToken string) *gh.Client {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	tc := oauth2.NewClient(ctx, ts)
	return gh.NewClient(tc)
}

// RegisterWebhook registers the given webhook to the repo and returns the ID of
// the registered webhook.
func (*githubGitProvider) RegisterWebhook(ctx context.Context, accessToken, repoURL, webhookURL string) (string, error) {
	owner, repo, err := ParseOwnerRepo(repoURL)
	if err != nil {
		return "", err
	}
	client := NewGitHubClient(ctx, accessToken)
	// GitHub's API documentation says this is the only allowed string for the
	// name field. TODO: Is this actually required?
	name := "web"
	hook, _, err := client.Repositories.CreateHook(ctx, owner, repo, &gh.Hook{
		Name:   &name,
		Events: eventsToReceive,
		Config: map[string]interface{}{
			"url":    webhookURL,
			"events": eventsToReceive,
		},
	})
	if err != nil {
		return "", gitHubErrorToStatus(err)
	}
	if hook.ID == nil {
		return "", status.UnknownError("GitHub returned invalid response from hooks API (missing ID field).")
	}
	return fmt.Sprintf("%d", *hook.ID), nil
}

// UnregisterWebhook removes the webhook from the Git repo.
func (*githubGitProvider) UnregisterWebhook(ctx context.Context, accessToken, repoURL, webhookID string) error {
	owner, repo, err := ParseOwnerRepo(repoURL)
	if err != nil {
		return err
	}
	client := NewGitHubClient(ctx, accessToken)
	id, err := strconv.ParseInt(webhookID, 10 /*=base*/, 64 /*=bitSize*/)
	if err != nil {
		return err
	}
	_, err = client.Repositories.DeleteHook(ctx, owner, repo, id)
	if err != nil {
		return gitHubErrorToStatus(err)
	}
	return nil
}

func gitHubErrorToStatus(err error) error {
	return status.InternalErrorf("%s", err)
}

func (*githubGitProvider) MatchRepoURL(u *url.URL) bool {
	return u.Host == "github.com"
}

func (*githubGitProvider) MatchWebhookRequest(r *http.Request) bool {
	return r.Header.Get("X-Github-Event") != ""
}

func (*githubGitProvider) ParseWebhookData(r *http.Request, webhookSecret string) (*interfaces.WebhookData, error) {
	payload, err := github.ValidatePayload(r, []byte(webhookSecret))
	if err != nil {
		return nil, status.InvalidArgumentErrorf("failed to parse webhook payload: %s", err)
	}

	// Uncomment this line to log the request payload (useful for getting more unit test data):
	// fmt.Printf("=== PAYLOAD ===\n%s\n=== END OF PAYLOAD ===\n", string(payload))

	event, err := gh.ParseWebHook(gh.WebHookType(r), payload)
	if err != nil {
		return nil, status.InvalidArgumentErrorf("failed to parse webhook payload: %s", err)
	}
	switch event := event.(type) {
	case *gh.PushEvent:
		// Ignore branch deletion events.
		if event.GetDeleted() {
			return nil, nil
		}
		v, err := fieldgetter.ExtractValues(
			event,
			"HeadCommit.ID",
			"Ref",
			"Repo.CloneURL",
			"Repo.Private",
		)
		if err != nil {
			return nil, err
		}
		branch := strings.TrimPrefix(v["Ref"], "refs/heads/")
		return &interfaces.WebhookData{
			EventName:          webhook_data.EventName.Push,
			PushedRepoURL:      v["Repo.CloneURL"],
			PushedBranch:       branch,
			SHA:                v["HeadCommit.ID"],
			TargetRepoURL:      v["Repo.CloneURL"],
			TargetBranch:       branch,
			IsTargetRepoPublic: v["Repo.Private"] == "false",
		}, nil

	case *gh.PullRequestEvent:
		// Run workflows when the PR is opened, pushed to, or reopened, to match
		// GitHub the behavior of GitHub actions. Also run workflows when the base
		// branch changes, to accommodate stacked changes.
		baseBranchChanged := event.GetAction() == "edited" && event.GetChanges().GetBase() != nil
		if !(baseBranchChanged || event.GetAction() == "opened" || event.GetAction() == "synchronize" || event.GetAction() == "reopened") {
			return nil, nil
		}
		return parsePullRequestOrReview(event)

	case *gh.PullRequestReviewEvent:
		if event.GetAction() != "submitted" {
			return nil, nil
		}
		if event.GetReview().GetState() != "approved" {
			return nil, nil
		}
		wd, err := parsePullRequestOrReview(event)
		if err != nil {
			return nil, err
		}
		wd.PullRequestApprover = event.GetReview().GetUser().GetLogin()
		return wd, nil

	case *gh.CheckSuiteEvent:
		if event.GetAction() != "requested" && event.GetAction() != "rerequested" {
			return nil, nil
		}
		// DO NOT MERGE: handle forks?
		if event.GetRepo().GetFork() {
			return nil, nil
		}
		return &interfaces.WebhookData{
			EventName:          webhook_data.EventName.CheckSuite,
			PushedRepoURL:      event.GetRepo().GetCloneURL(),
			PushedBranch:       event.GetCheckSuite().GetHeadBranch(),
			SHA:                event.GetCheckSuite().GetHeadSHA(),
			TargetRepoURL:      event.GetRepo().GetCloneURL(),
			IsTargetRepoPublic: !event.GetRepo().GetPrivate(),
			// TODO: for PRs, can we merge with the target branch?
			TargetBranch: event.GetCheckSuite().GetHeadBranch(),
			// TODO: rename PullRequestAuthor to something better
			PullRequestAuthor: event.GetSender().GetLogin(),
		}, nil

	case *gh.CheckRunEvent:
		// "created" means we're responding to a check run being created
		// in a previous event; "rerequested" means we need to create the
		// check run again, similarly to the check_suite event.
		if event.GetAction() != "created" && event.GetAction() != "rerequested" {
			return nil, nil
		}
		wd := &interfaces.WebhookData{
			EventName:          webhook_data.EventName.CheckRun,
			PushedRepoURL:      event.GetRepo().GetCloneURL(),
			PushedBranch:       event.GetCheckRun().GetCheckSuite().GetHeadBranch(),
			SHA:                event.GetCheckRun().GetCheckSuite().GetHeadSHA(),
			TargetRepoURL:      event.GetRepo().GetCloneURL(),
			TargetBranch:       event.GetCheckRun().GetCheckSuite().GetHeadBranch(),
			IsTargetRepoPublic: !event.GetRepo().GetPrivate(),
			PullRequestAuthor:  event.GetSender().GetLogin(),
			ActionName:         event.GetCheckRun().GetName(),
		}
		if event.GetAction() == "created" {
			wd.CheckRunID = event.GetCheckRun().GetID()
			wd.InvocationID = event.GetCheckRun().GetExternalID()
		}
		return wd, nil

	default:
		return nil, nil
	}
}

// parsePullRequestOrReview extracts WebhookData from a pull_request or
// pull_request_review event.
func parsePullRequestOrReview(event interface{}) (*interfaces.WebhookData, error) {
	v, err := fieldgetter.ExtractValues(
		event,
		"PullRequest.Head.Repo.CloneURL",
		"PullRequest.Head.Ref",
		"PullRequest.Head.SHA",
		"PullRequest.Base.Repo.CloneURL",
		"PullRequest.Base.Repo.Private",
		"PullRequest.Base.Ref",
		"PullRequest.User.Login",
	)
	if err != nil {
		return nil, err
	}
	isTargetRepoPublic := v["PullRequest.Base.Repo.Private"] == "false"
	return &interfaces.WebhookData{
		EventName:          webhook_data.EventName.PullRequest,
		PushedRepoURL:      v["PullRequest.Head.Repo.CloneURL"],
		PushedBranch:       v["PullRequest.Head.Ref"],
		SHA:                v["PullRequest.Head.SHA"],
		TargetRepoURL:      v["PullRequest.Base.Repo.CloneURL"],
		IsTargetRepoPublic: isTargetRepoPublic,
		TargetBranch:       v["PullRequest.Base.Ref"],
		PullRequestAuthor:  v["PullRequest.User.Login"],
	}, nil
}

func (*githubGitProvider) GetFileContents(ctx context.Context, accessToken, repoURL, filePath, ref string) ([]byte, error) {
	owner, repo, err := ParseOwnerRepo(repoURL)
	if err != nil {
		return nil, err
	}
	client := NewGitHubClient(ctx, accessToken)
	opts := &gh.RepositoryContentGetOptions{Ref: ref}
	fileContent, _, rsp, err := client.Repositories.GetContents(ctx, owner, repo, filePath, opts)
	if rsp != nil && rsp.StatusCode == http.StatusNotFound {
		return nil, status.NotFoundErrorf("%s: not found in %s", filePath, repoURL)
	}
	if err != nil {
		return nil, err
	}
	if fileContent == nil {
		return nil, status.InvalidArgumentErrorf("%s does not point to a file within %s", filePath, repoURL)
	}
	s, err := fileContent.GetContent()
	if err != nil {
		return nil, err
	}
	return []byte(s), nil
}

func (*githubGitProvider) IsTrusted(ctx context.Context, accessToken, repoURL, user string) (bool, error) {
	owner, repo, err := ParseOwnerRepo(repoURL)
	if err != nil {
		return false, err
	}
	client := NewGitHubClient(ctx, accessToken)
	isCollaborator, _, err := client.Repositories.IsCollaborator(ctx, owner, repo, user)
	if err != nil {
		return false, status.InternalErrorf("failed to determine whether %s is a collaborator in %s: %s", user, repoURL, err)
	}
	return isCollaborator, nil
}

// returns "owner", "repo" from a string like "https://github.com/owner/repo.git"
func ParseOwnerRepo(url string) (string, string, error) {
	ownerRepo, err := gitutil.OwnerRepoFromRepoURL(url)
	if err != nil {
		return "", "", status.WrapError(err, "Failed to parse owner/repo from GitHub URL")
	}
	parts := strings.Split(ownerRepo, "/")
	if len(parts) < 2 {
		return "", "", status.InvalidArgumentErrorf("Invalid owner/repo %q", ownerRepo)
	}
	return parts[0], parts[1], nil
}
