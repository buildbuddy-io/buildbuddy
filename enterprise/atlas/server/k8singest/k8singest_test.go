package k8singest

import (
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/summaries"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"
)

var (
	podRes = summaries.ResourceType{Cluster: "uswest1", Version: "v1", Resource: "pods", Kind: "Pod", Namespaced: true}
	depRes = summaries.ResourceType{Cluster: "uswest1", Group: "apps", Version: "v1", Resource: "deployments", Kind: "Deployment", Namespaced: true}
	svcRes = summaries.ResourceType{Cluster: "uswest1", Version: "v1", Resource: "services", Kind: "Service", Namespaced: true}
	cmRes  = summaries.ResourceType{Cluster: "sjc", Version: "v1", Resource: "configmaps", Kind: "ConfigMap", Namespaced: true}
)

func testPod(name, ns string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata": map[string]any{
			"name":              name,
			"namespace":         ns,
			"uid":               "uid-" + name,
			"creationTimestamp": "2026-07-29T10:00:00Z",
			"labels":            map[string]any{"app": "web"},
			"ownerReferences": []any{
				map[string]any{"kind": "ReplicaSet", "name": "web-7d9f", "controller": true},
			},
		},
		"spec": map[string]any{
			"nodeName": "node-a",
			"containers": []any{
				map[string]any{
					"name":  "app",
					"image": "registry.example/web:1.2.3",
					"ports": []any{
						map[string]any{"name": "http", "containerPort": int64(8080), "protocol": "TCP"},
						map[string]any{"name": "grpc", "containerPort": int64(1985), "protocol": "TCP"},
					},
				},
			},
		},
		"status": map[string]any{
			"phase": "Running",
			"podIP": "10.24.3.7",
			"containerStatuses": []any{
				map[string]any{"name": "app", "ready": true, "restartCount": int64(2)},
			},
		},
	}}
}

func TestSummarizePod(t *testing.T) {
	e, err := Summarize(podRes, testPod("web-7d9f-abcde", "prod"))
	require.NoError(t, err)
	require.Equal(t, "uswest1", e.Cluster)
	require.Equal(t, "Pod", e.Kind)
	require.Equal(t, "prod/web-7d9f-abcde", e.Key())
	require.Equal(t, "ReplicaSet/web-7d9f", e.Owner)
	require.Equal(t, "Running", e.Phase)
	require.Equal(t, "1/1", e.Ready)
	require.EqualValues(t, 2, e.Restarts)
	require.Equal(t, "node-a", e.Node)
	require.Equal(t, []string{"10.24.3.7"}, e.IPs)
	require.Equal(t, []string{"registry.example/web:1.2.3"}, e.Images)
	require.Equal(t, []string{"app"}, e.Containers)
	require.Equal(t, []summaries.Port{{Name: "http", Port: 8080}, {Name: "grpc", Port: 1985}}, e.Ports)
}

func TestSummarizePodWaitingReasonWins(t *testing.T) {
	pod := testPod("web-1", "prod")
	statuses := []any{
		map[string]any{
			"name": "app", "ready": false, "restartCount": int64(7),
			"state": map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
		},
	}
	require.NoError(t, unstructured.SetNestedSlice(pod.Object, statuses, "status", "containerStatuses"))
	e, err := Summarize(podRes, pod)
	require.NoError(t, err)
	require.Equal(t, "CrashLoopBackOff", e.Phase)
	require.Equal(t, "0/1", e.Ready)
}

func TestSummarizeDeployment(t *testing.T) {
	dep := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "Deployment",
		"metadata":   map[string]any{"name": "web", "namespace": "prod"},
		"spec": map[string]any{
			"replicas": int64(3),
			"selector": map[string]any{"matchLabels": map[string]any{"app": "web"}},
			"template": map[string]any{"spec": map[string]any{"containers": []any{
				map[string]any{"image": "registry.example/web:1.2.3"},
			}}},
		},
		"status": map[string]any{"readyReplicas": int64(2)},
	}}
	e, err := Summarize(depRes, dep)
	require.NoError(t, err)
	require.Equal(t, "2/3", e.Ready)
	require.Equal(t, map[string]string{"app": "web"}, e.Selector)
	require.Equal(t, []string{"registry.example/web:1.2.3"}, e.Images)
}

func TestSummarizeService(t *testing.T) {
	svc := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Service",
		"metadata":   map[string]any{"name": "web", "namespace": "prod"},
		"spec": map[string]any{
			"type":      "ClusterIP",
			"clusterIP": "10.0.0.5",
			"selector":  map[string]any{"app": "web"},
			"ports": []any{
				map[string]any{"name": "http", "port": int64(80), "protocol": "TCP"},
				map[string]any{"name": "dns", "port": int64(53), "protocol": "UDP"},
			},
		},
	}}
	e, err := Summarize(svcRes, svc)
	require.NoError(t, err)
	require.Equal(t, "ClusterIP", e.Phase)
	require.Equal(t, []string{"10.0.0.5"}, e.IPs)
	require.Equal(t, []summaries.Port{{Name: "http", Port: 80}, {Name: "dns", Port: 53, Protocol: "UDP"}}, e.Ports)
	require.Equal(t, map[string]string{"app": "web"}, e.Selector)
}

func TestSummarizeNode(t *testing.T) {
	node := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Node",
		"metadata": map[string]any{
			"name":   "sjc-prod-abc",
			"labels": map[string]any{"node-role.kubernetes.io/control-plane": ""},
		},
		"status": map[string]any{
			"conditions": []any{
				map[string]any{"type": "Ready", "status": "True"},
			},
			"addresses": []any{
				map[string]any{"type": "InternalIP", "address": "205.164.0.82"},
				map[string]any{"type": "Hostname", "address": "sjc-prod-abc"},
			},
			"nodeInfo": map[string]any{"kubeletVersion": "v1.31.2"},
		},
	}}
	e, err := Summarize(summaries.ResourceType{Cluster: "sjc", Version: "v1", Resource: "nodes", Kind: "Node"}, node)
	require.NoError(t, err)
	require.Equal(t, "Ready", e.Phase)
	require.Equal(t, "sjc-prod-abc", e.Key(), "cluster-scoped keys have no namespace")
	require.Equal(t, []string{"205.164.0.82"}, e.IPs)
	require.Equal(t, "v1.31.2", e.Extra["kubelet"])
	require.Equal(t, "control-plane", e.Extra["roles"])
}

func TestSummarizeJob(t *testing.T) {
	jobRes := summaries.ResourceType{Cluster: "uswest1", Group: "batch", Version: "v1", Resource: "jobs", Kind: "Job", Namespaced: true}
	job := func(status map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "batch/v1", "kind": "Job",
			"metadata": map[string]any{"name": "backup", "namespace": "prod"},
			"spec":     map[string]any{"completions": int64(1)},
			"status":   status,
		}}
	}
	for _, tc := range []struct {
		name   string
		status map[string]any
		want   string
	}{
		{"running", map[string]any{"active": int64(1)}, "Active"},
		{"in backoff between retries", map[string]any{"failed": int64(1)}, "Retrying"},
		{"failed for good", map[string]any{"failed": int64(3), "conditions": []any{map[string]any{"type": "Failed", "status": "True"}}}, "Failed"},
		{"complete", map[string]any{"succeeded": int64(1), "conditions": []any{map[string]any{"type": "Complete", "status": "True"}}}, "Complete"},
		{"not started", map[string]any{}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := Summarize(jobRes, job(tc.status))
			require.NoError(t, err)
			require.Equal(t, tc.want, e.Phase)
		})
	}
}

func TestSummarizePartialMetadata(t *testing.T) {
	cm := &metav1.PartialObjectMetadata{
		Name:      "app-config",
		Namespace: "prod",
		Labels:    map[string]string{"team": "core"},
	}
	e, err := Summarize(cmRes, cm)
	require.NoError(t, err)
	require.Equal(t, "ConfigMap", e.Kind)
	require.Equal(t, "prod/app-config", e.Key())
	require.Equal(t, map[string]string{"team": "core"}, e.Labels)
	require.Empty(t, e.Images)
}

func TestStoreAdapter(t *testing.T) {
	ix := summaries.New()
	s := NewStore(ix.NewStore(podRes))

	require.NoError(t, s.Add(testPod("a", "prod")))
	require.NoError(t, s.Update(testPod("a", "prod")))
	require.NoError(t, s.Add(testPod("b", "prod")))
	require.ElementsMatch(t, []string{"prod/a", "prod/b"}, s.ListKeys())
	got, ok, err := s.Get(testPod("a", "prod"))
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "a", got.(*summaries.Entry).Name)
	_, ok, err = s.GetByKey("prod/zzz")
	require.NoError(t, err)
	require.False(t, ok)

	// Replace swaps the full contents, as a reflector relist does.
	require.NoError(t, s.Replace([]any{testPod("c", "prod")}, ""))
	require.Equal(t, []string{"prod/c"}, s.ListKeys())
	_, ok = ix.GetEntry("uswest1", "", "pods", "prod", "c")
	require.True(t, ok, "entries land in the index the search runs over")
	require.Equal(t, 1, ix.Search("node-a", 10).Total, "and are searchable by summarized fields")

	// What List returned can be handed back, as cache.Store promises.
	listed := s.List()
	require.Len(t, listed, 1)
	_, ok, err = s.Get(listed[0])
	require.NoError(t, err)
	require.True(t, ok)
	require.NoError(t, s.Delete(listed[0]))
	require.Empty(t, s.List())

	// Tombstones from a missed delete carry only the key.
	require.NoError(t, s.Add(testPod("d", "prod")))
	require.NoError(t, s.Delete(cache.DeletedFinalStateUnknown{Key: "prod/d"}))
	require.Empty(t, s.List())

	require.Error(t, s.Add("not a kubernetes object"))
}
