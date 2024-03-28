package bitbucket

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/url"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/fieldgetter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	expectedUserAgent = "Bitbucket-Webhooks/2.0"
	repoBaseURL       = "https://bitbucket.org/"
)

type bitbucketGitProvider struct{}

func NewProvider() interfaces.GitProvider {
	return &bitbucketGitProvider{}
}

func (*bitbucketGitProvider) ParseWebhookData(r *http.Request) (*interfaces.WebhookData, error) {
	if userAgent := r.Header.Get("User-Agent"); userAgent != expectedUserAgent {
		return nil, status.UnimplementedErrorf("unexpected user agent: %q; only %q is supported", userAgent, expectedUserAgent)
	}
	switch eventName := r.Header.Get("X-Event-Key"); eventName {
	case "repo:push":
		payload := &PushEventPayload{}
		if err := unmarshalBody(r, payload); err != nil {
			return nil, status.InvalidArgumentErrorf("failed to unmarshal push event payload: %s", err)
		}
		v, err := fieldgetter.ExtractValues(
			payload,
			"Push.Changes.0.New.Name",
			"Push.Changes.0.New.Target.Hash",
			"Push.Changes.0.New.Type",
			"Repository.Links.HTML.Href",
		)
		if err != nil {
			return nil, err
		}
		if t := v["Push.Changes.0.New.Type"]; t != "branch" {
			log.Printf("Ignoring non-branch push event (type %q)", t)
			return nil, nil
		}
		branch := v["Push.Changes.0.New.Name"]
		return &interfaces.WebhookData{
			EventName:     webhook_data.EventName.Push,
			PushedRepoURL: v["Repository.Links.HTML.Href"],
			PushedBranch:  branch,
			TargetRepoURL: v["Repository.Links.HTML.Href"],
			TargetBranch:  branch,
			SHA:           v["Push.Changes.0.New.Target.Hash"],
		}, nil
	case "pullrequest:created", "pullrequest:updated":
		payload := &PullRequestEventPayload{}
		if err := unmarshalBody(r, payload); err != nil {
			return nil, status.InvalidArgumentErrorf("failed to unmarshal %q event payload: %s", eventName, err)
		}
		v, err := fieldgetter.ExtractValues(
			payload,
			"PullRequest.Destination.Repository.UUID",
			"PullRequest.Destination.Repository.Links.HTML.Href",
			"PullRequest.Destination.Branch.Name",
			"PullRequest.Source.Repository.UUID",
			"PullRequest.Source.Repository.Links.HTML.Href",
			"PullRequest.Source.Branch.Name",
			"PullRequest.Source.Commit.Hash",
		)
		if err != nil {
			return nil, err
		}
		return &interfaces.WebhookData{
			EventName:     webhook_data.EventName.PullRequest,
			PushedRepoURL: v["PullRequest.Source.Repository.Links.HTML.Href"],
			PushedBranch:  v["PullRequest.Source.Branch.Name"],
			SHA:           v["PullRequest.Source.Commit.Hash"],
			TargetRepoURL: v["PullRequest.Destination.Repository.Links.HTML.Href"],
			TargetBranch:  v["PullRequest.Destination.Branch.Name"],
		}, nil
	default:
		log.Printf("Ignoring webhook event: %s", eventName)
		return nil, nil
	}
}

func (*bitbucketGitProvider) MatchRepoURL(u *url.URL) bool {
	return u.Host == "bitbucket.com"
}

func (*bitbucketGitProvider) MatchWebhookRequest(r *http.Request) bool {
	return r.Header.Get("X-Event-Key") != ""
}

func (*bitbucketGitProvider) RegisterWebhook(ctx context.Context, accessToken, repoURL, webhookURL string) (string, error) {
	return "", status.UnimplementedError("Not implemented")
}

func (*bitbucketGitProvider) UnregisterWebhook(ctx context.Context, accessToken, repoURL, webhookID string) error {
	return status.UnimplementedError("Not implemented")
}

func (*bitbucketGitProvider) GetFileContents(ctx context.Context, accessToken, repoURL, filePath, ref string) ([]byte, error) {
	return nil, status.UnimplementedError("Not implemented")
}

func (*bitbucketGitProvider) IsTrusted(ctx context.Context, accessToken, repoURL, user string) (bool, error) {
	return false, status.UnimplementedError("Not implemented")
}

func (*bitbucketGitProvider) CreateStatus(ctx context.Context, accessToken, repoURL, commitSHA string, payload any) error {
	return status.UnimplementedError("Not implemented")
}

func unmarshalBody(r *http.Request, payload interface{}) error {
	b, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, payload)
}

// PushEventPayload represents a subset of Bitbucket's Push event schema.
// See https://support.atlassian.com/bitbucket-cloud/docs/event-payloads/#Push
type PushEventPayload struct {
	Push       *PushDetails `json:"push"`
	Repository *Repository  `json:"repository"`
}
type PushDetails struct {
	Changes []*PushedChange `json:"changes"`
}
type PushedChange struct {
	New *RefState `json:"new"`
}
type RefState struct {
	// Target contains the details of the most recent commit after the push.
	Target *CommitDetails `json:"target"`
	// Type contains the type of change.
	// NOTE: We're only interested in "branch".
	Type string `json:"type"`
	Name string `json:"name"`
}

// PullRequestEventPayload represents a subset of Bitbucket's Pull Request event schema.
// See https://support.atlassian.com/bitbucket-cloud/docs/event-payloads/#Pull-request
type PullRequestEventPayload struct {
	PullRequest *PullRequestDetails `json:"pullrequest"`
	Repository  *Repository         `json:"repository"`
}
type PullRequestDetails struct {
	Source      *PullRequestSide `json:"source"`
	Destination *PullRequestSide `json:"destination"`
}
type PullRequestSide struct {
	Branch     *Branch        `json:"branch"`
	Commit     *CommitDetails `json:"commit"`
	Repository *Repository    `json:"repository"`
}
type Branch struct {
	Name string `json:"name"`
}

// Repository represents a subset of Bitbucket's Repository schema, which is
// a common entity used in multiple webhook events.
// See https://support.atlassian.com/bitbucket-cloud/docs/event-payloads/#Repository
type Repository struct {
	Links     *RepositoryLinks `json:"links"`
	IsPrivate bool             `json:"is_private"`
	UUID      string           `json:"uuid"`
}
type RepositoryLinks struct {
	HTML *RepositoryLink `json:"html"`
}
type RepositoryLink struct {
	Href string `json:"href"`
}

// Commit represents a subset of Bitbucket's Commit schema, which is
// a common entity used in multiple webhook events.
// It isn't officially documented, but multiple event types use this
// format for representing commit details in their payloads.
type CommitDetails struct {
	Hash string `json:"hash"`
}
