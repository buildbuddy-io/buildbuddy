package atlas_service

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/cluster"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/k8singest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/summaries"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	atlaspb "github.com/buildbuddy-io/buildbuddy/proto/atlas"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	dynamicfake "k8s.io/client-go/dynamic/fake"
)

var (
	podRes = summaries.ResourceType{Cluster: "uswest1", Version: "v1", Resource: "pods", Kind: "Pod", Namespaced: true}
	svcRes = summaries.ResourceType{Cluster: "uswest1", Version: "v1", Resource: "services", Kind: "Service", Namespaced: true}
	rsRes  = summaries.ResourceType{Cluster: "uswest1", Group: "apps", Version: "v1", Resource: "replicasets", Kind: "ReplicaSet", Namespaced: true}
	depRes = summaries.ResourceType{Cluster: "uswest1", Group: "apps", Version: "v1", Resource: "deployments", Kind: "Deployment", Namespaced: true}
)

func podU(name string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name": name, "namespace": "prod",
			"creationTimestamp": "2024-01-02T03:04:05Z",
			"labels":            map[string]any{"app": "web"},
			"ownerReferences": []any{
				map[string]any{"kind": "ReplicaSet", "name": "web-7d9f", "controller": true},
			},
		},
		"spec": map[string]any{
			"nodeName": "node-a",
			"containers": []any{map[string]any{
				"name": "app", "image": "web:1",
				"ports": []any{
					map[string]any{"name": "http", "containerPort": int64(8080)},
					map[string]any{"name": "grpc", "containerPort": int64(1985)},
				},
			}},
		},
		"status": map[string]any{"phase": "Running", "podIP": "10.24.3.7"},
	}}
}

func svcU() *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]any{"name": "web", "namespace": "prod"},
		"spec": map[string]any{
			"type":     "ClusterIP",
			"selector": map[string]any{"app": "web"},
			"ports":    []any{map[string]any{"name": "http", "port": int64(80)}},
		},
	}}
}

func ownedU(kind, apiVersion, name, ownerKind, ownerName string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": apiVersion,
		"kind":       kind,
		"metadata": map[string]any{
			"name": name, "namespace": "prod",
			"ownerReferences": []any{
				map[string]any{"kind": ownerKind, "name": ownerName, "controller": true},
			},
		},
	}}
}

// newTestService builds a service over a populated index and one fake-backed
// cluster with tunnel zones configured.
func newTestService(t *testing.T) (*AtlasService, *cluster.Cluster) {
	t.Helper()
	ix := summaries.New()
	require.NoError(t, k8singest.NewStore(ix.NewStore(podRes)).Add(podU("web-7d9f-abcde")))
	require.NoError(t, k8singest.NewStore(ix.NewStore(svcRes)).Add(svcU()))
	require.NoError(t, k8singest.NewStore(ix.NewStore(rsRes)).Add(ownedU("ReplicaSet", "apps/v1", "web-7d9f", "Deployment", "web")))
	require.NoError(t, k8singest.NewStore(ix.NewStore(depRes)).Add(&unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1", "kind": "Deployment",
		"metadata": map[string]any{"name": "web", "namespace": "prod"},
	}}))

	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(),
		map[schema.GroupVersionResource]string{
			{Version: "v1", Resource: "pods"}:   "PodList",
			{Version: "v1", Resource: "events"}: "EventList",
		}, podU("web-7d9f-abcde"))
	flags.Set(t, "atlas.svc_zone", "svc.uswest1.buildbuddy.internal")
	flags.Set(t, "atlas.pod_zone", "pod.uswest1.buildbuddy.internal")
	c := cluster.NewWithClients("uswest1", ix, dyn, nil, nil)
	return New(c), c
}

func TestGuessScheme(t *testing.T) {
	for _, tc := range []struct {
		port summaries.Port
		want string
	}{
		{summaries.Port{Name: "http", Port: 1234}, "http"},
		{summaries.Port{Name: "metrics", Port: 1234}, "http"},
		{summaries.Port{Name: "https-admin", Port: 1234}, "https"},
		{summaries.Port{Name: "", Port: 9090}, "http"},
		{summaries.Port{Name: "", Port: 8443}, "https"},
		{summaries.Port{Name: "grpc", Port: 1985}, ""},
		{summaries.Port{Name: "", Port: 5432}, ""},
		{summaries.Port{Name: "http", Port: 53, Protocol: "UDP"}, ""},
	} {
		require.Equal(t, tc.want, guessScheme(tc.port), "%+v", tc.port)
	}
}

func TestGetObjectPodPortsAndRelations(t *testing.T) {
	s, _ := newTestService(t)
	rsp, err := s.GetObject(context.Background(), &atlaspb.GetObjectRequest{
		Cluster: "uswest1", Version: "v1", Resource: "pods", Namespace: "prod", Name: "web-7d9f-abcde",
	})
	require.NoError(t, err)
	require.Equal(t, "Pod", rsp.GetKind())
	require.NotNil(t, rsp.GetEntry())
	require.Equal(t, "web-7d9f-abcde", rsp.GetEntry().GetName())
	require.Contains(t, rsp.GetYaml(), "nodeName: node-a")

	// Pod ports link via the dashed-IP pod record; the selecting service's
	// port is offered too, via the service record.
	byVia := map[atlaspb.PortLink_Via][]*atlaspb.PortLink{}
	for _, p := range rsp.GetPorts() {
		byVia[p.GetVia()] = append(byVia[p.GetVia()], p)
	}
	pod, svc := byVia[atlaspb.PortLink_POD], byVia[atlaspb.PortLink_SERVICE]
	require.Len(t, pod, 2)
	require.Equal(t, "10-24-3-7.prod.pod.uswest1.buildbuddy.internal", pod[0].GetHost())
	require.Equal(t, "http://10-24-3-7.prod.pod.uswest1.buildbuddy.internal:8080", pod[0].GetUrl())
	require.Empty(t, pod[1].GetUrl(), "grpc port gets no browser URL")
	require.Equal(t, "10-24-3-7.prod.pod.uswest1.buildbuddy.internal:1985", pod[1].GetHostPort())
	require.Len(t, svc, 1)
	require.Equal(t, "web.prod.svc.uswest1.buildbuddy.internal:80", svc[0].GetHostPort())
	require.Equal(t, "web", svc[0].GetViaName())

	// The ownership chain resolves Pod -> ReplicaSet -> Deployment, and the
	// selecting service appears in relations.
	rel := rsp.GetRelations()
	require.NotNil(t, rel)
	require.Len(t, rel.GetOwners(), 2)
	require.Equal(t, "ReplicaSet", rel.GetOwners()[0].GetKind())
	require.Equal(t, "Deployment", rel.GetOwners()[1].GetKind())
	require.Len(t, rel.GetServices(), 1)
}

func TestRelationsDeploymentFindsPodsAcrossReplicaSets(t *testing.T) {
	s, _ := newTestService(t)
	dep, ok := s.c.Index().GetEntry("uswest1", "apps", "deployments", "prod", "web")
	require.True(t, ok)
	rel := s.relationsFor(dep)
	require.Equal(t, 1, rel.PodsTotal)
	require.Equal(t, "web-7d9f-abcde", rel.Pods[0].Name)
}

func TestDebugPageLinks(t *testing.T) {
	flags.Set(t, "atlas.pod_zone", "pod.example")
	flags.Set(t, "atlas.svc_zone", "svc.example")
	c := cluster.NewWithClients("uswest1", nil, nil, nil, nil)
	names := func(links []*atlaspb.PortLink) (out []string) {
		for _, l := range links {
			if l.GetUrl() != "" {
				out = append(out, l.GetName()+" "+l.GetUrl())
			}
		}
		return out
	}

	// A BuildBuddy binary's monitoring port serves the whole debug set.
	bb := &summaries.Entry{
		Kind: "Pod", Namespace: "prod", Name: "app-1", IPs: []string{"10.1.2.3"},
		Images: []string{"registry.example/buildbuddy-app:v2"},
		Ports:  []summaries.Port{{Name: "http", Port: 8080}, {Name: "monitoring", Port: 9091}, {Name: "https", Port: 8443}},
	}
	links := portLinks(c, bb, nil)
	require.Equal(t, []string{
		"http http://10-1-2-3.prod.pod.example:8080",
		"monitoring http://10-1-2-3.prod.pod.example:9091",
		"https https://10-1-2-3.prod.pod.example:8443",
		"statusz http://10-1-2-3.prod.pod.example:9091/statusz",
		"metrics http://10-1-2-3.prod.pod.example:9091/metrics",
		"pprof http://10-1-2-3.prod.pod.example:9091/debug/pprof/",
		"flagz http://10-1-2-3.prod.pod.example:9091/flagz",
		"rpcz http://10-1-2-3.prod.pod.example:9091/rpcz",
		"channelz http://10-1-2-3.prod.pod.example:9091/channelz/",
	}, names(links), "ports first, in spec order, then pages")
	var paths []string
	for _, l := range links {
		if l.GetPath() != "" {
			paths = append(paths, l.GetPath())
		}
	}
	require.Equal(t, []string{"/statusz", "/metrics", "/debug/pprof/", "/flagz", "/rpcz", "/channelz/"}, paths, "page links say which page")

	// Any image: a port named for metrics links to /metrics; 9090 alone is
	// not a BuildBuddy signal.
	other := &summaries.Entry{
		Kind: "Pod", Namespace: "prod", Name: "exporter-1", IPs: []string{"10.1.2.4"},
		Images: []string{"quay.example/exporter:1"},
		Ports: []summaries.Port{
			{Name: "metrics", Port: 9100},
			{Name: "web", Port: 9090},
			{Name: "metrics", Port: 9125, Protocol: "UDP"},
			{Name: "admin", Port: 8081, Protocol: "TCP"}, // spelled out, still TCP
			{Name: "https-metrics", Port: 10250},         // a TLS listener, as on kubelets
		},
	}
	require.Equal(t, []string{
		"metrics http://10-1-2-4.prod.pod.example:9100",
		"web http://10-1-2-4.prod.pod.example:9090",
		"admin http://10-1-2-4.prod.pod.example:8081",
		"https-metrics https://10-1-2-4.prod.pod.example:10250",
		"metrics http://10-1-2-4.prod.pod.example:9100/metrics",
		"metrics https://10-1-2-4.prod.pod.example:10250/metrics",
	}, names(portLinks(c, other, nil)), "page links use the port's scheme; the UDP port gets neither kind")

	// A Service has no image; on its page the pods behind it tell, in search
	// results nothing does.
	svc := &summaries.Entry{Kind: "Service", Namespace: "prod", Name: "app", Ports: []summaries.Port{{Name: "monitoring", Port: 9091}, {Name: "http", Port: 8080}}}
	withPods := names(portLinks(c, svc, &relations{Pods: []*summaries.Entry{bb}}))
	require.Len(t, withPods, 2+len(buildBuddyDebugPages))
	require.Equal(t, "statusz http://app.prod.svc.example:9091/statusz", withPods[2])
	require.Equal(t, []string{
		"monitoring http://app.prod.svc.example:9091",
		"http http://app.prod.svc.example:8080",
	}, names(portLinks(c, svc, nil)))
}

func TestStatefulSetPodLinks(t *testing.T) {
	flags.Set(t, "atlas.pod_zone", "pod.example")
	flags.Set(t, "atlas.svc_zone", "svc.example")
	c := cluster.NewWithClients("uswest1", nil, nil, nil, nil)
	pod := &summaries.Entry{
		Kind: "Pod", Namespace: "data", Name: "redis-0", IPs: []string{"10.52.25.24"},
		Hostname: "redis-0", Subdomain: "redis",
		Ports: []summaries.Port{{Name: "redis", Port: 6379}},
	}
	links := portLinks(c, pod, nil)
	require.Len(t, links, 1)
	require.Equal(t, "redis-0.redis.data.svc.example:6379", links[0].GetHostPort(), "the headless service's per-pod record, not the pod IP")

	pod.Subdomain = ""
	require.Equal(t, "10-52-25-24.data.pod.example:6379", portLinks(c, pod, nil)[0].GetHostPort(), "without one, the pod record")
}

func TestSearch(t *testing.T) {
	s, _ := newTestService(t)
	rsp, err := s.Search(context.Background(), &atlaspb.SearchRequest{Query: "web kind:pod"})
	require.NoError(t, err)
	require.EqualValues(t, 1, rsp.GetTotal())
	require.Len(t, rsp.GetGroups(), 1)
	require.Equal(t, "Pod", rsp.GetGroups()[0].GetKind())
	require.Equal(t, "web-7d9f-abcde", rsp.GetGroups()[0].GetResults()[0].GetEntry().GetName())
	require.NotNil(t, rsp.GetGroups()[0].GetResults()[0].GetEntry().GetCreated(), "timestamps are carried as protos")
	links := rsp.GetGroups()[0].GetResults()[0].GetLinks()
	require.Len(t, links, 2, "a result carries its own port links")
	require.Equal(t, "http://10-24-3-7.prod.pod.uswest1.buildbuddy.internal:8080", links[0].GetUrl())
	require.Empty(t, links[1].GetUrl(), "grpc port gets no browser URL")

	rsp, err = s.Search(context.Background(), &atlaspb.SearchRequest{Query: "web kind:deployment"})
	require.NoError(t, err)
	require.Len(t, rsp.GetGroups(), 1)
	require.Equal(t, "apps", rsp.GetGroups()[0].GetGroup(), "the api group rides along with the kind")
}

func TestGetStatus(t *testing.T) {
	s, _ := newTestService(t)
	rsp, err := s.GetStatus(context.Background(), &atlaspb.GetStatusRequest{})
	require.NoError(t, err)
	require.Len(t, rsp.GetClusters(), 1)
	require.Equal(t, "uswest1", rsp.GetClusters()[0].GetName())
	require.Equal(t, "svc.uswest1.buildbuddy.internal", rsp.GetClusters()[0].GetSvcZone())
}

func TestGetObjectUnknownClusterAndBadRequests(t *testing.T) {
	s, _ := newTestService(t)
	_, err := s.GetObject(context.Background(), &atlaspb.GetObjectRequest{
		Cluster: "nope", Version: "v1", Resource: "pods", Namespace: "prod", Name: "x",
	})
	require.True(t, status.IsNotFoundError(err), "got %v", err)

	_, err = s.GetObject(context.Background(), &atlaspb.GetObjectRequest{Cluster: "uswest1"})
	require.True(t, status.IsInvalidArgumentError(err), "got %v", err)
}

type fakeLogs struct {
	lastNS, lastPod string
	lastOpts        cluster.LogOptions
	err             error // returned when set
}

func (f *fakeLogs) Logs(ctx context.Context, ns, pod string, opts cluster.LogOptions) (io.ReadCloser, error) {
	f.lastNS, f.lastPod, f.lastOpts = ns, pod, opts
	if f.err != nil {
		return nil, f.err
	}
	if opts.Previous {
		return nil, apierrors.NewBadRequest("previous terminated container not found")
	}
	return io.NopCloser(strings.NewReader("log line 1\nlog line 2\n")), nil
}

// logStream collects what StreamLogs sends.
type logStream struct {
	grpc.ServerStream
	ctx  context.Context
	data []byte
}

func (l *logStream) Context() context.Context { return l.ctx }
func (l *logStream) Send(m *atlaspb.StreamLogsResponse) error {
	l.data = append(l.data, m.GetData()...)
	return nil
}

func TestStreamLogs(t *testing.T) {
	s, c := newTestService(t)
	logs := &fakeLogs{}
	c.WithLogSource(logs)

	stream := &logStream{ctx: context.Background()}
	err := s.StreamLogs(&atlaspb.StreamLogsRequest{
		Cluster: "uswest1", Namespace: "prod", Name: "web-7d9f-abcde",
		Container: "app", TailLines: 500, Follow: true,
	}, stream)
	require.NoError(t, err)
	require.Equal(t, "log line 1\nlog line 2\n", string(stream.data))
	require.Equal(t, "prod", logs.lastNS)
	require.Equal(t, "web-7d9f-abcde", logs.lastPod)
	require.Equal(t, cluster.LogOptions{Container: "app", TailLines: 500, Follow: true}, logs.lastOpts)

	// A refusal (e.g. previous with no prior run) surfaces the apiserver's
	// explanation as the status message.
	err = s.StreamLogs(&atlaspb.StreamLogsRequest{
		Cluster: "uswest1", Namespace: "prod", Name: "web-7d9f-abcde", Previous: true,
	}, &logStream{ctx: context.Background()})
	require.True(t, status.IsFailedPreconditionError(err), "got %v", err)
	require.Contains(t, err.Error(), "previous terminated container not found")
	require.EqualValues(t, 200, logs.lastOpts.TailLines, "default tail applies")

	// A missing pod is NotFound; a failure the apiserver did not answer is
	// Unavailable, so callers can tell them apart.
	req := &atlaspb.StreamLogsRequest{Cluster: "uswest1", Namespace: "prod", Name: "web-7d9f-abcde"}
	logs.err = apierrors.NewNotFound(schema.GroupResource{Resource: "pods"}, "web-7d9f-abcde")
	err = s.StreamLogs(req, &logStream{ctx: context.Background()})
	require.True(t, status.IsNotFoundError(err), "got %v", err)
	logs.err = errors.New("dial tcp: connection refused")
	err = s.StreamLogs(req, &logStream{ctx: context.Background()})
	require.True(t, status.IsUnavailableError(err), "got %v", err)

	err = s.StreamLogs(&atlaspb.StreamLogsRequest{Cluster: "nope", Namespace: "prod", Name: "x"}, &logStream{ctx: context.Background()})
	require.True(t, status.IsNotFoundError(err), "got %v", err)
}
