package xds_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/util/xds"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/rest"
)

func newNodeServer(t *testing.T) (*httptest.Server, *http.Request) {
	var last http.Request
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		last = *r
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/prefix/api/v1/nodes/node-a":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"kind":       "Node",
				"apiVersion": "v1",
				"metadata": map[string]any{
					"name":   "node-a",
					"labels": map[string]string{"topology.kubernetes.io/zone": "us-west1-b", "other": "x"},
				},
			})
		case "/prefix/api/v1/nodes/forbidden":
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"kind":"Status","status":"Failure","message":"nodes \"forbidden\" is forbidden: no access","code":403}`))
		case "/prefix/api/v1/nodes/garbage":
			_, _ = w.Write([]byte(`{"metadata": [not json`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"kind":"Status","status":"Failure","reason":"NotFound","message":"nodes \"` + strings.TrimPrefix(r.URL.Path, "/prefix/api/v1/nodes/") + `\" not found","code":404}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv, &last
}

func TestRestNodeLabelGetter(t *testing.T) {
	srv, last := newNodeServer(t)
	getter, err := xds.NewNodeLabelGetter(&rest.Config{Host: srv.URL + "/prefix", BearerToken: "test-token"})
	require.NoError(t, err)

	labels, err := getter.NodeLabels(context.Background(), "node-a")
	require.NoError(t, err)
	require.Equal(t, map[string]string{"topology.kubernetes.io/zone": "us-west1-b", "other": "x"}, labels)
	require.Equal(t, http.MethodGet, last.Method)
	require.Equal(t, "/prefix/api/v1/nodes/node-a", last.URL.Path)
	require.Equal(t, "Bearer test-token", last.Header.Get("Authorization"))
	require.Equal(t, "application/json", last.Header.Get("Accept"))
	require.Contains(t, last.Header.Get("User-Agent"), "/")

	_, err = getter.NodeLabels(context.Background(), "forbidden")
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 403")
	require.Contains(t, err.Error(), "no access")

	_, err = getter.NodeLabels(context.Background(), "missing")
	require.Error(t, err)
	require.Contains(t, err.Error(), "HTTP 404")
	require.Contains(t, err.Error(), `nodes "missing" not found`)

	_, err = getter.NodeLabels(context.Background(), "garbage")
	require.Error(t, err)
	require.Contains(t, err.Error(), "decode node")
}

func TestNewNodeLabelGetter_HostForms(t *testing.T) {
	for _, host := range []string{"10.0.0.1:443", "https://10.0.0.1:443", "https://10.0.0.1:443/prefix", "kubernetes.default.svc"} {
		getter, err := xds.NewNodeLabelGetter(&rest.Config{Host: host})
		require.NoError(t, err, host)
		require.NotNil(t, getter, host)
	}
	_, err := xds.NewNodeLabelGetter(&rest.Config{Host: ""})
	require.Error(t, err)
}
