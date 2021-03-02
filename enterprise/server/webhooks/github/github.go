package github

import (
	"fmt"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/fieldgetter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	gh "github.com/google/go-github/github"
)

func ParseRequest(r *http.Request) (*webhook_data.WebhookData, error) {
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
	log.Printf("Received GitHub webhook event: %T\n", event)
	switch event := event.(type) {
	case *gh.PushEvent:
		v, err := fieldgetter.ExtractValues(event, "Ref", "HeadCommit.ID", "Repo.DefaultBranch", "Repo.CloneURL")
		if err != nil {
			return nil, err
		}
		defaultBranchRef := fmt.Sprintf("refs/heads/%s", v["Repo.DefaultBranch"])
		// Only run workflows on the default branch.
		if v["Ref"] != defaultBranchRef {
			log.Printf("Ignoring push event for non-default branch %q", v["Ref"])
			return nil, nil
		}
		return &webhook_data.WebhookData{
			RepoURL: v["Repo.CloneURL"],
			// For some reason, "HeadCommit.SHA" is nil, but ID has the commit SHA,
			// so we use that instead.
			SHA: v["HeadCommit.ID"],
		}, nil

	case *gh.PullRequestEvent:
		v, err := fieldgetter.ExtractValues(event, "Action", "PullRequest.Head.SHA", "PullRequest.Head.Repo.CloneURL")
		if err != nil {
			return nil, err
		}
		// Only build when the PR is opened or pushed to.
		if !(v["Action"] == "opened" || v["Action"] == "synchronize") {
			log.Printf("Ignoring pull_request webhook event (action=%q)", v["Action"])
			return nil, nil
		}
		return &webhook_data.WebhookData{
			RepoURL: v["PullRequest.Head.Repo.CloneURL"],
			SHA:     v["PullRequest.Head.SHA"],
		}, nil

	default:
		log.Printf("Ignoring webhook event: %T", event)
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
