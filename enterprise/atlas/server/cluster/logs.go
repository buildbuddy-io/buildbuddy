package cluster

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, apiError(resp.StatusCode, body)
	}
	return resp.Body, nil
}

// apiError turns an apiserver error response into the *StatusError the typed
// clients return.
func apiError(code int, body []byte) error {
	st := &metav1.Status{}
	if err := json.Unmarshal(body, st); err != nil || st.Kind != "Status" {
		st = &metav1.Status{Status: metav1.StatusFailure, Message: strings.TrimSpace(string(body))}
		if st.Message == "" {
			st.Message = http.StatusText(code)
		}
	}
	if st.Code == 0 {
		st.Code = int32(code)
	}
	return &apierrors.StatusError{ErrStatus: *st}
}
