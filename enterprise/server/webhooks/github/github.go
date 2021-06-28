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
	gh "github.com/google/go-github/github"
)

var (
	// GitHub event names to listen for on the webhook.
	eventsToReceive = []string{"push", "pull_request"}
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

// IsRepoPrivate returns whether the given GitHub repo is private.
func (*githubGitProvider) IsRepoPrivate(ctx context.Context, accessToken, repoURL string) (bool, error) {
	owner, repo, err := parseOwnerRepo(repoURL)
	if err != nil {
		return false, err
	}
	client := newGitHubClient(ctx, accessToken)
	r, _, err := client.Repositories.Get(ctx, owner, repo)
	if err != nil {
		return false, gitHubErrorToStatus(err)
	}
	if r.Private == nil {
		return false, status.UnknownError(`GitHub returned invalid response from hooks API (missing "private" field)`)
	}
	return *r.Private, nil
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
			EventName:     webhook_data.EventName.Push,
			PushedBranch:  branch,
			TargetBranch:  branch,
			RepoURL:       v["Repo.CloneURL"],
			IsRepoPrivate: v["Repo.Private"] == "true",
			// For some reason, "HeadCommit.SHA" is nil, but ID has the commit SHA,
			// so we use that instead.
			SHA: v["HeadCommit.ID"],
		}, nil

	case *gh.PullRequestEvent:
		v, err := fieldgetter.ExtractValues(
			event,
			"Action",
			"PullRequest.Base.Ref",
			"PullRequest.Head.Repo.CloneURL",
			"PullRequest.Base.Repo.Private",
			"PullRequest.Head.Ref",
			"PullRequest.Head.SHA",
		)
		if err != nil {
			return nil, err
		}
		// Only build when the PR is opened or pushed to.
		if !(v["Action"] == "opened" || v["Action"] == "synchronize") {
			return nil, nil
		}
		return &interfaces.WebhookData{
			EventName:     webhook_data.EventName.PullRequest,
			PushedBranch:  v["PullRequest.Head.Ref"],
			TargetBranch:  v["PullRequest.Base.Ref"],
			RepoURL:       v["PullRequest.Head.Repo.CloneURL"],
			IsRepoPrivate: v["PullRequest.Base.Repo.Private"] == "true",
			SHA:           v["PullRequest.Head.SHA"],
		}, nil

	default:
		return nil, nil
	}
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
