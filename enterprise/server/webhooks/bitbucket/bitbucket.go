package bitbucket

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/fieldgetter"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/webhook_data"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
)

const (
	expectedUserAgent = "Bitbucket-Webhooks/2.0"
	repoBaseURL       = "https://bitbucket.org/"
)

func missingFieldError(path string) error {
	return status.InvalidArgumentErrorf("missing field %q", path)
}

func ParseRequest(r *http.Request) (*webhook_data.WebhookData, error) {
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
		return &webhook_data.WebhookData{
			RepoURL: v["Repository.Links.HTML.Href"],
			SHA:     v["Push.Changes.0.New.Target.Hash"],
		}, nil
	case "pullrequest:created", "pullrequest:updated":
		payload := &PullRequestEventPayload{}
		if err := unmarshalBody(r, payload); err != nil {
			return nil, status.InvalidArgumentErrorf("failed to unmarshal %q event payload: %s", eventName, err)
		}
		v, err := fieldgetter.ExtractValues(
			payload,
			"PullRequest.Source.Commit.Hash",
			"PullRequest.Source.Repository.Links.HTML.Href",
		)
		if err != nil {
			return nil, err
		}
		return &webhook_data.WebhookData{
			RepoURL: v["PullRequest.Source.Repository.Links.HTML.Href"],
			SHA:     v["PullRequest.Source.Commit.Hash"],
		}, nil
	default:
		log.Printf("Ignoring webhook event: %s", eventName)
		return nil, nil
	}
}

func unmarshalBody(r *http.Request, payload interface{}) error {
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, payload)
}

// PushEventPayload represents a subset of Bitbucket's Push event schema.
// See https://support.atlassian.com/bitbucket-cloud/docs/event-payloads/#Push
type PushEventPayload struct {
	Repository *Repository  `json:"repository"`
	Push       *PushDetails `json:"push"`
}
type PushDetails struct {
	Changes []*PushedChange `json:"changes"`
}
type PushedChange struct {
	New *RefState `json:"new"`
}
type RefState struct {
	// Type contains the type of change.
	// NOTE: We're only interested in "branch".
	Type string `json:"type"`
	Name string `json:"name"`
	// Target contains the details of the most recent commit after the push.
	Target *CommitDetails `json:"target"`
}

// PullRequestEventPayload represents a subset of Bitbucket's Pull Request event schema.
// See https://support.atlassian.com/bitbucket-cloud/docs/event-payloads/#Pull-request
type PullRequestEventPayload struct {
	PullRequest *PullRequestDetails `json:"pullrequest"`
}
type PullRequestDetails struct {
	Source *PullRequestBranch `json:"source"`
}
type PullRequestBranch struct {
	Commit     *CommitDetails `json:"commit"`
	Repository *Repository    `json:"repository"`
}

// Repository represents a subset of Bitbucket's Repository schema, which is
// a common entity used in multiple webhook events.
// See https://support.atlassian.com/bitbucket-cloud/docs/event-payloads/#Repository
type Repository struct {
	Links *RepositoryLinks `json:"links"`
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
