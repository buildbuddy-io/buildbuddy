package cluster

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
)

func TestRESTLogSource(t *testing.T) {
	var gotPath, gotQuery string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		io.WriteString(w, "line 1\nline 2\n")
	}))
	defer srv.Close()

	// A server URL may carry a path prefix, as behind a proxy like Rancher.
	for _, prefix := range []string{"", "/k8s/clusters/c-abc"} {
		t.Run("prefix"+prefix, func(t *testing.T) {
			ls, err := newRESTLogSource(&rest.Config{Host: srv.URL + prefix})
			require.NoError(t, err)

			stream, err := ls.Logs(context.Background(), "prod", "web-1", LogOptions{
				Container: "app",
				TailLines: 200,
				Previous:  true,
			})
			require.NoError(t, err)
			defer stream.Close()

			body, err := io.ReadAll(stream)
			require.NoError(t, err)
			require.Equal(t, "line 1\nline 2\n", string(body))
			require.Equal(t, prefix+"/api/v1/namespaces/prod/pods/web-1/log", gotPath)
			require.Equal(t, "container=app&previous=true&tailLines=200", gotQuery)
		})
	}
}

func TestRESTLogSourceSurfacesAPIServerRefusal(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		body string
		is   func(error) bool
		msg  string
	}{
		{"missing pod", 404, `{"kind":"Status","status":"Failure","message":"pods \"web-1\" not found","reason":"NotFound","code":404}`, apierrors.IsNotFound, `pods "web-1" not found`},
		{"no previous run", 400, `{"kind":"Status","status":"Failure","message":"previous terminated container \"app\" in pod \"web-1\" not found","reason":"BadRequest","code":400}`, apierrors.IsBadRequest, `previous terminated container "app" in pod "web-1" not found`},
		{"not a Status body", 503, `upstream unavailable`, apierrors.IsServiceUnavailable, `upstream unavailable`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
				io.WriteString(w, tc.body)
			}))
			defer srv.Close()
			ls, err := newRESTLogSource(&rest.Config{Host: srv.URL})
			require.NoError(t, err)

			_, err = ls.Logs(context.Background(), "prod", "web-1", LogOptions{Previous: true})
			require.True(t, tc.is(err), "got %v", err)
			require.ErrorContains(t, err, tc.msg)
		})
	}
}

func TestLogsWithoutSource(t *testing.T) {
	c := NewWithClients("test", nil, nil, nil, nil)
	_, err := c.Logs(context.Background(), "prod", "web-1", LogOptions{})
	require.ErrorContains(t, err, "no log access")
}
