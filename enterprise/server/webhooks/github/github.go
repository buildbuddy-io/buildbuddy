package github

import (
	"fmt"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"reflect"
	"strings"

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
		v, err := fieldValues(event, "Ref", "HeadCommit.ID", "Repo.DefaultBranch")
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
			// For some reason, "HeadCommit.SHA" is nil, but ID has the commit SHA,
			// so we use that instead.
			SHA: v["HeadCommit.ID"],
		}, nil

	case *gh.PullRequestEvent:
		v, err := fieldValues(event, "Action", "PullRequest.Head.SHA", "PullRequest.Head.Repo.CloneURL")
		if err != nil {
			return nil, err
		}
		// Only build when the PR is opened or pushed to.
		if !(v["Action"] == "opened" || v["Action"] == "synchronize") {
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

// fieldValues extracts values from an object given field paths which may include dot
// separators. As the paths are traversed, pointers will be de-referenced. If a nil
// value is encountered in any path, an error is returned.
func fieldValues(obj interface{}, fieldPaths ...string) (map[string]string, error) {
	values := map[string]string{}
	objVal := reflect.Indirect(reflect.ValueOf(obj))
	if !objVal.IsValid() {
		return nil, status.InvalidArgumentError("cannot get fieldValues on a nil value")
	}
	for _, path := range fieldPaths {
		fields := strings.Split(path, ".")
		cur := objVal
		curPath := []string{}
		for _, name := range fields {
			curPath = append(curPath, name)
			cur = cur.FieldByName(name)
			if !cur.IsValid() {
				return nil, status.InternalErrorf("encountered invalid field name at %q", strings.Join(curPath, "."))
			}
			cur = reflect.Indirect(cur)
			if !cur.IsValid() {
				return nil, status.InvalidArgumentErrorf("encountered nil value at %q", strings.Join(curPath, "."))
			}
		}
		el := cur.Interface()
		str, ok := el.(string)
		if !ok {
			return nil, status.InternalErrorf("encountered non-string value at %q", path)
		}
		values[path] = str
	}
	return values, nil
}
