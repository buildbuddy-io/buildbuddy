package cluster

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, `{"kind":"Status","message":"previous terminated container \"app\" in pod \"web-1\" not found"}`)
	}))
	defer srv.Close()

	ls, err := newRESTLogSource(&rest.Config{Host: srv.URL})
	require.NoError(t, err)

	_, err = ls.Logs(context.Background(), "prod", "web-1", LogOptions{Previous: true})
	require.ErrorContains(t, err, `previous terminated container "app" in pod "web-1" not found`)
}

func TestLogsWithoutSource(t *testing.T) {
	c := NewWithClients("test", nil, nil, nil, nil)
	_, err := c.Logs(context.Background(), "prod", "web-1", LogOptions{})
	require.ErrorContains(t, err, "no log access")
}
