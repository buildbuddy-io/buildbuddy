package github

import (
	"context"
	"fmt"
	"io/ioutil"
	"mime"
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

func newGitHubClient(ctx context.Context, accessToken string) *gh.Client {
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: accessToken})
	tc := oauth2.NewClient(ctx, ts)
	return gh.NewClient(tc)
}

// RegisterWebhook registers the given webhook to the repo and returns the ID of
// the registered webhook.
func (*githubGitProvider) RegisterWebhook(ctx context.Context, accessToken, repoURL, webhookURL string) (string, error) {
	owner, repo, err := parseOwnerRepo(repoURL)
	if err != nil {
		return "", err
	}
	client := newGitHubClient(ctx, accessToken)
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
	owner, repo, err := parseOwnerRepo(repoURL)
	if err != nil {
		return err
	}
	client := newGitHubClient(ctx, accessToken)
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

func (*githubGitProvider) ParseWebhookData(r *http.Request) (*interfaces.WebhookData, error) {
	payload, err := webhookJSONPayload(r)
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
			// The push handler will not receive events from forked repositories,
			// so if a commit was pushed to this repo then it is trusted.
			IsTrusted: true,
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
		if !isTrustedAssociation(event.GetReview().GetAuthorAssociation()) {
			return nil, nil
		}
		// If the PR author is trusted, we don't need an approving review to run
		// the workflow -- the workflow will already have been run as part of the
		// pull_request synchronize event.
		if isTrustedAssociation(event.GetPullRequest().GetAuthorAssociation()) {
			return nil, nil
		}
		wd, err := parsePullRequestOrReview(event)
		if err != nil {
			return nil, err
		}
		// We now trust the state that the PR is in, since we have an approved review.
		wd.IsTrusted = true
		return wd, nil

	default:
		return nil, nil
	}
}

// isTrustedAssociation returns whether a user should be allowed to execute
// trusted workflows for a repo, given their association to the repo.
func isTrustedAssociation(association string) bool {
	return association == "OWNER" || association == "MEMBER" || association == "COLLABORATOR"
}

// parsePullRequestOrReview extracts WebhookData from a pull_request or
// pull_request_review event.
func parsePullRequestOrReview(event interface{}) (*interfaces.WebhookData, error) {
	v, err := fieldgetter.ExtractValues(
		event,
		"Action",
		"PullRequest.Base.Ref",
		"PullRequest.Base.Repo.CloneURL",
		"PullRequest.Head.Ref",
		"PullRequest.Head.SHA",
		"PullRequest.Head.Repo.CloneURL",
	)
	if err != nil {
		return nil, err
	}
	isFork := v["PullRequest.Base.Repo.CloneURL"] != v["PullRequest.Head.Repo.CloneURL"]
	return &interfaces.WebhookData{
		EventName:     webhook_data.EventName.PullRequest,
		PushedRepoURL: v["PullRequest.Head.Repo.CloneURL"],
		PushedBranch:  v["PullRequest.Head.Ref"],
		SHA:           v["PullRequest.Head.SHA"],
		TargetRepoURL: v["PullRequest.Base.Repo.CloneURL"],
		TargetBranch:  v["PullRequest.Base.Ref"],
		IsTrusted:     !isFork,
	}, nil
}

func (*githubGitProvider) GetFileContents(ctx context.Context, accessToken, repoURL, filePath, ref string) ([]byte, error) {
	owner, repo, err := parseOwnerRepo(repoURL)
	if err != nil {
		return nil, err
	}
	client := newGitHubClient(ctx, accessToken)
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

func webhookJSONPayload(r *http.Request) ([]byte, error) {
	contentType, _, err := mime.ParseMediaType(r.Header.Get("content-type"))
	if err != nil {
		return nil, status.InvalidArgumentErrorf("failed to parse content type: %s", err)
	}
	switch contentType {
	case "application/json":
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, status.InternalErrorf("failed to read request body: %s", err)
		}
		return body, nil
	case "application/x-www-form-urlencoded":
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			return nil, status.InternalErrorf("failed to read request body: %s", err)
		}
		form, err := url.ParseQuery(string(body))
		if err != nil {
			return nil, err
		}
		const payloadFormParam = "payload"
		return []byte(form.Get(payloadFormParam)), nil
	default:
		return nil, status.InvalidArgumentErrorf("unhandled MIME type: %q", contentType)
	}
}

// returns "owner", "repo" from a string like "https://github.com/owner/repo.git"
func parseOwnerRepo(url string) (string, string, error) {
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
