package checks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-github/v43/github"

	gh_webhooks "github.com/buildbuddy-io/buildbuddy/enterprise/server/webhooks/github"
)

const (
	// Name of the file where the user can write custom GitHub Check
	// annotations, which will be included in the check update request at the
	// conclusion of the workflow.
	checkAnnotationsFileName = "check_annotations.json"

	// Name of the env var exposing the absolute path to the check annotations
	// file.
	checkAnnotationsFileEnvVarName = "CHECK_ANNOTATIONS_FILE"
)

// Run represents a GitHub Check run executed by a BuildBuddy
// workflow.
type Run struct {
	ID            int64
	Name          string
	RepoURL       string
	AccessToken   string
	WorkspaceRoot string
	DetailsURL    string
}

func (r *Run) ProvisionAnnotationsFile() error {
	path := filepath.Join(r.WorkspaceRoot, checkAnnotationsFileName)
	if err := os.RemoveAll(path); err != nil {
		return err
	}
	if err := os.Setenv(checkAnnotationsFileEnvVarName, path); err != nil {
		return err
	}
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return f.Close()
}

func (r *Run) Start(ctx context.Context) error {
	return r.update(ctx, "in_progress", "" /*=conclusion*/, nil /*=output*/)
}

func (r *Run) Finish(ctx context.Context, exitCode int, exitCodeName string) error {
	annotations, err := r.readAnnotations()
	if err != nil {
		return err
	}
	// Set the conclusion to "failure" if either the check command returned
	// a non-zero exit code, or one of the annotations includes a "failure"
	// annotation level.
	conclusion := "success"
	if exitCode != 0 {
		conclusion = "failure"
	}
	for _, annotation := range annotations {
		if annotation.GetAnnotationLevel() == "failure" {
			conclusion = "failure"
		}
	}
	summary := exitCodeName
	if summary == "" {
		if exitCode == 0 {
			summary = "OK"
		} else {
			summary = "Failed"
		}
	}
	log.Infof("Read ")
	if len(annotations) > 0 {
		// Append annotation stats like "(1 info, 2 warning)"
		// TODO: Does GitHub do this for us automatically?
		counts := map[string]int{}
		for _, annotation := range annotations {
			if annotation.GetAnnotationLevel() != "" {
				counts[annotation.GetAnnotationLevel()]++
			}
		}
		keys := make([]string, 0, len(counts))
		for k := range counts {
			keys = append(keys, k)
		}
		sort.StringSlice(keys).Sort()
		countStrs := make([]string, 0, len(counts))
		for _, k := range keys {
			countStrs = append(countStrs, fmt.Sprintf("%d %s", counts[k], k))
		}
		summary += fmt.Sprintf(" (%s)", strings.Join(countStrs, ", "))
	}
	annotationsCount := len(annotations)
	title := summary
	output := &github.CheckRunOutput{
		Title:            &title,
		Summary:          &summary,
		Annotations:      annotations,
		AnnotationsCount: &annotationsCount,
	}
	if err := r.update(ctx, "completed", conclusion, output); err != nil {
		return err
	}
	return nil
}

func (r *Run) readAnnotations() ([]*github.CheckRunAnnotation, error) {
	path := filepath.Join(r.WorkspaceRoot, checkAnnotationsFileName)
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var annotations []*github.CheckRunAnnotation

	// Users can either write a list of JSON (starting with "[") or omit the
	// list wrapper and have a stream of JSON objects.
	if len(b) == 0 {
		return nil, nil
	}
	if b[0] == '[' {
		dec := json.NewDecoder(bytes.NewReader(b))
		_, err := dec.Token()
		if err != nil {
			return nil, err
		}
		for dec.More() {
			annotation := &github.CheckRunAnnotation{}
			if err := dec.Decode(annotation); err != nil {
				return nil, err
			}
			annotations = append(annotations, annotation)
		}
		return annotations, nil
	}

	dec := json.NewDecoder(bytes.NewReader(b))
	for {
		annotation := &github.CheckRunAnnotation{}
		if err := dec.Decode(annotation); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		annotations = append(annotations, annotation)
	}
	return annotations, nil
}

func (r *Run) update(ctx context.Context, runStatus, conclusion string, output *github.CheckRunOutput) error {
	client := gh_webhooks.NewGitHubClient(ctx, r.AccessToken)
	owner, repo, err := gh_webhooks.ParseOwnerRepo(r.RepoURL)
	if err != nil {
		return err
	}
	opts := github.UpdateCheckRunOptions{
		Name:       r.Name,
		Status:     &runStatus,
		DetailsURL: &r.DetailsURL,
		Output:     output,
	}
	if conclusion != "" {
		opts.Conclusion = &conclusion
	}
	_, res, err := client.Checks.UpdateCheckRun(ctx, owner, repo, r.ID, opts)
	if err != nil {
		return err
	}
	if res.StatusCode >= 300 {
		return status.UnknownErrorf("unexpected HTTP status %d", res.StatusCode)
	}
	return nil
}
