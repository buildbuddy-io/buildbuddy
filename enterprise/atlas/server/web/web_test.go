package web

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/atlas_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/cluster"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/k8singest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/summaries"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/encoding/protojson"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	atlaspb "github.com/buildbuddy-io/buildbuddy/proto/atlas"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var podRes = summaries.ResourceType{Cluster: "uswest1", Version: "v1", Resource: "pods", Kind: "Pod", Namespaced: true}

func podU() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]any{"name": "web-7d9f-abcde", "namespace": "prod"},
		"spec":       map[string]any{"nodeName": "node-a"},
		"status":     map[string]any{"phase": "Running", "podIP": "10.24.3.7"},
	}}
}

type fakeLogs struct{}

func (fakeLogs) Logs(ctx context.Context, ns, pod string, opts cluster.LogOptions) (io.ReadCloser, error) {
	if opts.Previous {
		return nil, apierrors.NewBadRequest("previous terminated container not found")
	}
	return io.NopCloser(strings.NewReader("log line 1\nlog line 2\n")), nil
}

// newTestServer serves the UI over a one-pod index and a fake bundle whose
// hash is "abc123".
func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	ix := summaries.New()
	require.NoError(t, k8singest.NewStore(ix.NewStore(podRes)).Add(podU()))
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			{Version: "v1", Resource: "pods"}:   "PodList",
			{Version: "v1", Resource: "events"}: "EventList",
		}, podU())
	c := cluster.NewWithClients("uswest1", ix, dyn, nil, nil).WithLogSource(fakeLogs{})
	svc := atlas_service.New(c)

	grpcServer := grpc.NewServer()
	atlaspb.RegisterAtlasServiceServer(grpcServer, svc)
	t.Cleanup(grpcServer.Stop)

	appFS := fstest.MapFS{
		"sha.sum":           {Data: []byte("abc123\n")},
		"app_bundle/app.js": {Data: []byte("console.log('atlas');")},
		"style.css":         {Data: []byte("body {}")},
	}
	env := real_environment.NewRealEnv(healthcheck.NewHealthChecker("atlas-test"))
	h, err := Handler(env, Options{
		AppFS: appFS, Service: svc, GRPCServer: grpcServer,
		ClusterName: "uswest1",
		ClusterLinks: []*atlaspb.ClusterLink{
			{Name: "uswest1", Url: "https://atlas.example"},
			{Name: "sjc", Url: "https://atlas.sjc.example"},
		},
	})
	require.NoError(t, err)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv
}

// frontendConfig extracts the config the index page hands the UI.
func frontendConfig(t *testing.T, indexHTML string) *atlaspb.FrontendConfig {
	t.Helper()
	m := regexp.MustCompile(`window\.atlasConfig = (\{.*\});`).FindStringSubmatch(indexHTML)
	require.Len(t, m, 2, "index page has no config: %s", indexHTML)
	cfg := &atlaspb.FrontendConfig{}
	require.NoError(t, protojson.Unmarshal([]byte(m[1]), cfg))
	return cfg
}

func get(t *testing.T, srv *httptest.Server, path string) (*http.Response, string) {
	t.Helper()
	client := srv.Client()
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Get(srv.URL + path)
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	return resp, string(body)
}

func TestIndexAndAssets(t *testing.T) {
	srv := newTestServer(t)

	// The index carries the frontend config and asset URLs, and is served for
	// client-side routes too.
	// Object names may have dots and must not be mistaken for asset requests.
	for _, path := range []string{
		"/", "/clusters", "/object/uswest1/-/v1/pods/prod/web",
		"/object/uswest1/-/v1/configmaps/prod/kube-root-ca.crt",
		"/object/uswest1/-/v1/nodes/-/node-1.sjc.example",
	} {
		resp, body := get(t, srv, path)
		require.Equal(t, 200, resp.StatusCode, path)
		require.Contains(t, resp.Header.Get("Content-Type"), "text/html")
		cfg := frontendConfig(t, body)
		require.Equal(t, "abc123", cfg.GetAppBundleHash())
		require.Equal(t, "uswest1", cfg.GetClusterName())
		require.Len(t, cfg.GetClusterLinks(), 2, "the picker's instances ride along")
		require.Equal(t, "https://atlas.sjc.example", cfg.GetClusterLinks()[1].GetUrl())
		require.Contains(t, body, `src="/app/app_bundle/app.js?hash=abc123"`)
		require.Contains(t, body, `href="/app/style.css?hash=abc123"`)
	}

	resp, body := get(t, srv, "/app/style.css?hash=abc123")
	require.Equal(t, 200, resp.StatusCode)
	require.Equal(t, "body {}", body)
	require.Equal(t, "public, max-age=31536000, immutable", resp.Header.Get("Cache-Control"))

	resp, _ = get(t, srv, "/app/style.css?hash=stale")
	require.Equal(t, "no-cache", resp.Header.Get("Cache-Control"))

	// Paths with an extension are stray asset requests, not UI routes.
	for _, path := range []string{"/app/missing.css", "/app/", "/favicon.ico", "/summaries.html"} {
		resp, _ := get(t, srv, path)
		require.Equal(t, 404, resp.StatusCode, path)
	}
}

func TestUnaryRPCOverHTTP(t *testing.T) {
	srv := newTestServer(t)

	// Binary protos, as the UI sends them.
	reqBytes, err := proto.Marshal(&atlaspb.SearchRequest{Query: "web kind:pod"})
	require.NoError(t, err)
	resp, err := srv.Client().Post(srv.URL+"/rpc/AtlasService/Search", "application/proto", bytes.NewReader(reqBytes))
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, string(body))
	rsp := &atlaspb.SearchResponse{}
	require.NoError(t, proto.Unmarshal(body, rsp))
	require.EqualValues(t, 1, rsp.GetTotal())
	require.Equal(t, "web-7d9f-abcde", rsp.GetGroups()[0].GetResults()[0].GetEntry().GetName())

	// JSON too, which is what curl wants.
	resp, err = srv.Client().Post(srv.URL+"/rpc/AtlasService/Search", "application/json", strings.NewReader(`{"query":"web"}`))
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, string(body))
	require.Contains(t, string(body), `"total":1`)

	resp, err = srv.Client().Post(srv.URL+"/rpc/AtlasService/Nope", "application/proto", bytes.NewReader(nil))
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, 404, resp.StatusCode)
}

// prefixed frames a message the way the browser does for streaming RPCs.
func prefixed(t *testing.T, m proto.Message) *bytes.Reader {
	t.Helper()
	b, err := proto.Marshal(m)
	require.NoError(t, err)
	frame := make([]byte, 5+len(b))
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(b)))
	copy(frame[5:], b)
	return bytes.NewReader(frame)
}

// readFrames splits a length-prefixed response into messages and the gRPC
// trailers, if any were sent in-band.
func readFrames(t *testing.T, body []byte) (messages [][]byte, trailers string) {
	t.Helper()
	for len(body) > 0 {
		require.GreaterOrEqual(t, len(body), 5, "truncated frame header")
		flags, n := body[0], binary.BigEndian.Uint32(body[1:5])
		require.GreaterOrEqual(t, len(body), int(5+n), "truncated frame")
		payload := body[5 : 5+n]
		if flags&0x80 != 0 {
			trailers = string(payload)
		} else {
			messages = append(messages, payload)
		}
		body = body[5+n:]
	}
	return messages, trailers
}

func TestStreamingRPCOverHTTP(t *testing.T) {
	srv := newTestServer(t)
	url := srv.URL + "/rpc/AtlasService/StreamLogs"

	resp, err := srv.Client().Post(url, "application/proto+prefixed", prefixed(t, &atlaspb.StreamLogsRequest{
		Cluster: "uswest1", Namespace: "prod", Name: "web-7d9f-abcde", Follow: true,
	}))
	require.NoError(t, err)
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	require.Equal(t, 200, resp.StatusCode, string(body))
	require.Equal(t, "application/proto+prefixed", resp.Header.Get("Content-Type"))
	messages, trailers := readFrames(t, body)
	var logs []byte
	for _, m := range messages {
		chunk := &atlaspb.StreamLogsResponse{}
		require.NoError(t, proto.Unmarshal(m, chunk))
		logs = append(logs, chunk.GetData()...)
	}
	require.Equal(t, "log line 1\nlog line 2\n", string(logs))
	require.Contains(t, trailers, fmt.Sprintf("Grpc-Status: %d", codes.OK))

	// A refusal comes back as a gRPC status the client can classify, with the
	// apiserver's explanation as the message.
	resp, err = srv.Client().Post(url, "application/proto+prefixed", prefixed(t, &atlaspb.StreamLogsRequest{
		Cluster: "uswest1", Namespace: "prod", Name: "web-7d9f-abcde", Previous: true,
	}))
	require.NoError(t, err)
	body, err = io.ReadAll(resp.Body)
	resp.Body.Close()
	require.NoError(t, err)
	status := resp.Header.Get("Grpc-Status")
	message := resp.Header.Get("Grpc-Message")
	if status == "" {
		_, trailers := readFrames(t, body)
		require.Contains(t, trailers, fmt.Sprintf("Grpc-Status: %d", codes.FailedPrecondition))
		require.Contains(t, trailers, "previous terminated container not found")
	} else {
		require.Equal(t, fmt.Sprintf("%d", codes.FailedPrecondition), status)
		require.Contains(t, message, "previous terminated container not found")
	}
}
