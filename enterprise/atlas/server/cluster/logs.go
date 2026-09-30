package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"k8s.io/client-go/rest"
)

// LogOptions selects what to read from a pod's log subresource.
type LogOptions struct {
	Container string
	// TailLines caps how far back the log starts. Zero means the server
	// default (everything).
	TailLines int64
	// Follow keeps the stream open as new lines arrive.
	Follow bool
	// Previous reads the prior container run — what you want for a pod in
	// CrashLoopBackOff, whose current run has produced nothing yet.
	Previous bool
}

// LogSource reads pod logs.
type LogSource interface {
	Logs(ctx context.Context, namespace, pod string, opts LogOptions) (io.ReadCloser, error)
}

// WithLogSource replaces the cluster's log source and returns the cluster,
// for tests.
func (c *Cluster) WithLogSource(ls LogSource) *Cluster {
	c.logs = ls
	return c
}

// Logs streams the named pod's logs.
func (c *Cluster) Logs(ctx context.Context, namespace, pod string, opts LogOptions) (io.ReadCloser, error) {
	if c.logs == nil {
		return nil, fmt.Errorf("cluster %q has no log access", c.name)
	}
	return c.logs.Logs(ctx, namespace, pod, opts)
}

// restLogSource fetches logs over the cluster's authenticated HTTP client.
type restLogSource struct {
	base   *url.URL
	client *http.Client
}

func newRESTLogSource(rc *rest.Config) (*restLogSource, error) {
	httpClient, err := rest.HTTPClientFor(rc)
	if err != nil {
		return nil, err
	}
	base, _, err := rest.DefaultServerUrlFor(rc)
	if err != nil {
		return nil, err
	}
	return &restLogSource{base: base, client: httpClient}, nil
}

func (s *restLogSource) Logs(ctx context.Context, namespace, pod string, opts LogOptions) (io.ReadCloser, error) {
	u := s.base.JoinPath("api", "v1", "namespaces", url.PathEscape(namespace), "pods", url.PathEscape(pod), "log")
	q := url.Values{}
	if opts.Container != "" {
		q.Set("container", opts.Container)
	}
	if opts.TailLines > 0 {
		q.Set("tailLines", strconv.FormatInt(opts.TailLines, 10))
	}
	if opts.Follow {
		q.Set("follow", "true")
	}
	if opts.Previous {
		q.Set("previous", "true")
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		// The apiserver explains refusals well ("previous terminated container
		// not found", "a container name must be specified"), so surface its
		// words rather than just the code.
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("%s", statusMessage(body, resp.Status))
	}
	return resp.Body, nil
}

// statusMessage extracts the message from a k8s Status JSON body, falling
// back to the raw body or HTTP status.
func statusMessage(body []byte, httpStatus string) string {
	type status struct {
		Message string `json:"message"`
	}
	var st status
	if err := json.Unmarshal(body, &st); err == nil && st.Message != "" {
		return st.Message
	}
	if len(body) > 0 {
		return string(body)
	}
	return httpStatus
}
