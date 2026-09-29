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

func TestSummarizePodInitContainerFailing(t *testing.T) {
	pod := testPod("web-1", "prod")
	require.NoError(t, unstructured.SetNestedField(pod.Object, "Pending", "status", "phase"))
	require.NoError(t, unstructured.SetNestedSlice(pod.Object, []any{
		map[string]any{"name": "migrate", "image": "registry.example/migrate:1"},
	}, "spec", "initContainers"))
	require.NoError(t, unstructured.SetNestedSlice(pod.Object, []any{
		map[string]any{
			"name": "migrate", "ready": false, "restartCount": int64(4),
			"state": map[string]any{"waiting": map[string]any{"reason": "CrashLoopBackOff"}},
		},
	}, "status", "initContainerStatuses"))
	require.NoError(t, unstructured.SetNestedSlice(pod.Object, []any{
		map[string]any{
			"name": "app", "ready": false, "restartCount": int64(0),
			"state": map[string]any{"waiting": map[string]any{"reason": "PodInitializing"}},
		},
	}, "status", "containerStatuses"))
	e, err := Summarize(podRes, pod)
	require.NoError(t, err)
	require.Equal(t, "Init:CrashLoopBackOff", e.Phase)
	require.EqualValues(t, 4, e.Restarts)
	require.Equal(t, "0/1", e.Ready)
	require.Equal(t, []string{"app"}, e.Containers, "plain init containers are not listed")
}

func TestSummarizePodSidecar(t *testing.T) {
	pod := testPod("web-1", "prod")
	require.NoError(t, unstructured.SetNestedSlice(pod.Object, []any{
		map[string]any{"name": "migrate", "image": "registry.example/migrate:1"},
		map[string]any{"name": "proxy", "image": "registry.example/proxy:2", "restartPolicy": "Always"},
	}, "spec", "initContainers"))
	require.NoError(t, unstructured.SetNestedSlice(pod.Object, []any{
		map[string]any{
			"name": "migrate", "ready": false, "restartCount": int64(3),
			"state": map[string]any{"terminated": map[string]any{"exitCode": int64(0)}},
		},
		map[string]any{
			"name": "proxy", "ready": true, "restartCount": int64(1),
			"state": map[string]any{"running": map[string]any{}},
		},
	}, "status", "initContainerStatuses"))
	e, err := Summarize(podRes, pod)
	require.NoError(t, err)
	require.Equal(t, "Running", e.Phase)
	require.Equal(t, "2/2", e.Ready, "the sidecar counts like a regular container")
	require.EqualValues(t, 3, e.Restarts, "sidecar 1 + app 2; the finished init container's are dropped")
	require.Equal(t, []string{"proxy", "app"}, e.Containers)
	require.Equal(t, []string{"registry.example/proxy:2", "registry.example/web:1.2.3"}, e.Images)
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

func TestSummarizeDaemonSet(t *testing.T) {
	ds := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apps/v1",
		"kind":       "DaemonSet",
		"metadata":   map[string]any{"name": "node-exporter", "namespace": "monitoring"},
		"spec":       map[string]any{"selector": map[string]any{"matchLabels": map[string]any{"app": "node-exporter"}}},
		"status":     map[string]any{"desiredNumberScheduled": int64(5), "numberReady": int64(4)},
	}}
	dsRes := summaries.ResourceType{Cluster: "uswest1", Group: "apps", Version: "v1", Resource: "daemonsets", Kind: "DaemonSet", Namespaced: true}
	e, err := Summarize(dsRes, ds)
	require.NoError(t, err)
	require.Equal(t, "4/5", e.Ready, "daemonsets count from status, not spec.replicas")
	require.Equal(t, map[string]string{"app": "node-exporter"}, e.Selector)
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

func TestSummarizeServiceAddresses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		spec   map[string]any
		status map[string]any
		want   []string
	}{
		{
			"dual-stack with external ips behind a hostname load balancer",
			map[string]any{"clusterIP": "10.0.0.5", "clusterIPs": []any{"10.0.0.5", "fd00::5"}, "externalIPs": []any{"192.0.2.10"}},
			map[string]any{"loadBalancer": map[string]any{"ingress": []any{
				map[string]any{"hostname": "lb.example"},
				map[string]any{"ip": "203.0.113.7"},
			}}},
			[]string{"10.0.0.5", "fd00::5", "192.0.2.10", "lb.example", "203.0.113.7"},
		},
		{"headless", map[string]any{"clusterIP": "None", "clusterIPs": []any{"None"}}, nil, nil},
		{"external name", map[string]any{"type": "ExternalName", "externalName": "db.example"}, nil, []string{"db.example"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			svc := &unstructured.Unstructured{Object: map[string]any{
				"apiVersion": "v1", "kind": "Service",
				"metadata": map[string]any{"name": "web", "namespace": "prod"},
				"spec":     tc.spec,
				"status":   tc.status,
			}}
			e, err := Summarize(svcRes, svc)
			require.NoError(t, err)
			require.Equal(t, tc.want, e.IPs)
		})
	}
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
	job := func(spec, status map[string]any) *unstructured.Unstructured {
		return &unstructured.Unstructured{Object: map[string]any{
			"apiVersion": "batch/v1", "kind": "Job",
			"metadata": map[string]any{"name": "backup", "namespace": "prod"},
			"spec":     spec,
			"status":   status,
		}}
	}
	one := map[string]any{"completions": int64(1)}
	complete := []any{map[string]any{"type": "Complete", "status": "True"}}
	for _, tc := range []struct {
		name         string
		spec, status map[string]any
		phase, ready string
	}{
		{"running", one, map[string]any{"active": int64(1)}, "Active", "0/1"},
		{"in backoff between retries", one, map[string]any{"failed": int64(1)}, "Retrying", "0/1"},
		{"failed for good", one, map[string]any{"failed": int64(3), "conditions": []any{map[string]any{"type": "Failed", "status": "True"}}}, "Failed", "0/1"},
		{"complete", one, map[string]any{"succeeded": int64(1), "conditions": complete}, "Complete", "1/1"},
		{"not started", one, map[string]any{}, "", "0/1"},
		{"suspended", map[string]any{"completions": int64(1), "suspend": true}, map[string]any{}, "Suspended", "0/1"},
		{"fixed completion count", map[string]any{"completions": int64(3)}, map[string]any{"active": int64(1), "succeeded": int64(2)}, "Active", "2/3"},
		{"work queue", map[string]any{"parallelism": int64(5)}, map[string]any{"succeeded": int64(5), "conditions": complete}, "Complete", "5/1 of 5"},
		{"single-worker queue", map[string]any{"parallelism": int64(1)}, map[string]any{"active": int64(1)}, "Active", "0/1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, err := Summarize(jobRes, job(tc.spec, tc.status))
			require.NoError(t, err)
			require.Equal(t, tc.phase, e.Phase)
			require.Equal(t, tc.ready, e.Ready)
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
	keys := func() []string {
		var out []string
		for _, e := range s.s.Entries() {
			out = append(out, e.Key())
		}
		return out
	}

	require.NoError(t, s.Add(testPod("a", "prod")))
	require.NoError(t, s.Update(testPod("a", "prod")))
	require.NoError(t, s.Add(testPod("b", "prod")))
	require.ElementsMatch(t, []string{"prod/a", "prod/b"}, keys())

	// Replace swaps the full contents, as a reflector relist does.
	require.NoError(t, s.Replace([]any{testPod("c", "prod")}, ""))
	require.Equal(t, []string{"prod/c"}, keys())
	_, ok := ix.GetEntry("uswest1", "", "pods", "prod", "c")
	require.True(t, ok, "entries land in the index the search runs over")
	require.Equal(t, 1, ix.Search("node-a", 10).Total, "and are searchable by summarized fields")

	require.NoError(t, s.Delete(testPod("c", "prod")))
	require.Empty(t, keys())

	// Tombstones from a missed delete carry only the key.
	require.NoError(t, s.Add(testPod("d", "prod")))
	require.NoError(t, s.Delete(cache.DeletedFinalStateUnknown{Key: "prod/d"}))
	require.Empty(t, keys())

	require.Error(t, s.Add("not a kubernetes object"))
}
