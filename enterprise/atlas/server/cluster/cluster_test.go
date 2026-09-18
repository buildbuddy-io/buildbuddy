package cluster

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/summaries"
	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	fakedisco "k8s.io/client-go/discovery/fake"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	metadatafake "k8s.io/client-go/metadata/fake"
	clienttesting "k8s.io/client-go/testing"
)

var (
	podsGVR   = schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	eventsGVR = schema.GroupVersionResource{Version: "v1", Resource: "events"}
)

func discoveryWith(resources []*metav1.APIResourceList) *fakedisco.FakeDiscovery {
	return &fakedisco.FakeDiscovery{Fake: &clienttesting.Fake{Resources: resources}}
}

func testResources() []*metav1.APIResourceList {
	return []*metav1.APIResourceList{
		{
			GroupVersion: "v1",
			APIResources: []metav1.APIResource{
				{Name: "pods", Kind: "Pod", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
				{Name: "pods/log", Kind: "Pod", Namespaced: true, Verbs: metav1.Verbs{"get"}},
				{Name: "configmaps", Kind: "ConfigMap", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
				{Name: "events", Kind: "Event", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
				{Name: "bindings", Kind: "Binding", Namespaced: true, Verbs: metav1.Verbs{"create"}},
			},
		},
		{
			GroupVersion: "apps/v1",
			APIResources: []metav1.APIResource{
				{Name: "deployments", Kind: "Deployment", Namespaced: true, Verbs: metav1.Verbs{"get", "list", "watch"}},
			},
		},
	}
}

func podU(name, ns string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"spec":       map[string]any{"nodeName": "node-a"},
		"status":     map[string]any{"phase": "Running"},
	}}
}

func newFakeCluster(t *testing.T, ix *summaries.Index, objs ...runtime.Object) *Cluster {
	t.Helper()
	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		podsGVR:   "PodList",
		eventsGVR: "EventList",
		{Group: "apps", Version: "v1", Resource: "deployments"}: "DeploymentList",
	}, objs...)

	metaScheme := metadatafake.NewTestScheme()
	require.NoError(t, metav1.AddMetaToScheme(metaScheme))
	metaClient := metadatafake.NewSimpleMetadataClient(metaScheme,
		&metav1.PartialObjectMetadata{
			APIVersion: "v1", Kind: "ConfigMap",
			Name: "app-config", Namespace: "prod",
		},
	)
	return NewWithClients("test", ix, dyn, metaClient, discoveryWith(testResources()))
}

func TestListWatchableResources(t *testing.T) {
	c := newFakeCluster(t, summaries.New())
	resources, err := c.listWatchableResources()
	require.NoError(t, err)

	byName := map[string]discoveredResource{}
	for _, r := range resources {
		byName[r.res.Resource] = r
	}
	require.Contains(t, byName, "pods")
	require.Contains(t, byName, "configmaps")
	require.Contains(t, byName, "deployments")
	require.True(t, byName["pods"].rich)
	require.True(t, byName["deployments"].rich)
	require.False(t, byName["configmaps"].rich, "configmaps are metadata-only")
	require.NotContains(t, byName, "events", "ignored by default")
	require.NotContains(t, byName, "bindings", "not watchable")
	require.NotContains(t, byName, "pods/log", "subresource")
	require.Equal(t, "Pod", byName["pods"].res.Kind)
	require.True(t, byName["pods"].res.Namespaced)
}

func TestWatchFeedsIndex(t *testing.T) {
	ix := summaries.New()
	c := newFakeCluster(t, ix, podU("web-1", "prod"))
	ctx := t.Context()
	c.discover(ctx)

	// The seeded pod arrives with the initial list, the configmap through the
	// metadata tier.
	require.Eventually(t, func() bool {
		_, ok := ix.GetEntry("test", "", "pods", "prod", "web-1")
		return ok
	}, 10*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		_, ok := ix.GetEntry("test", "", "configmaps", "prod", "app-config")
		return ok
	}, 10*time.Second, 10*time.Millisecond)

	e, _ := ix.GetEntry("test", "", "pods", "prod", "web-1")
	require.Equal(t, "Running", e.Phase, "rich tier carries status")
	cm, _ := ix.GetEntry("test", "", "configmaps", "prod", "app-config")
	require.Equal(t, "ConfigMap", cm.Kind)

	// A pod created after the initial list arrives via the watch.
	dyn := c.dyn.(*dynamicfake.FakeDynamicClient)
	_, err := dyn.Resource(podsGVR).Namespace("prod").Create(ctx, podU("web-2", "prod"), metav1.CreateOptions{})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		_, ok := ix.GetEntry("test", "", "pods", "prod", "web-2")
		return ok
	}, 10*time.Second, 10*time.Millisecond)

	// And deletes are removed.
	require.NoError(t, dyn.Resource(podsGVR).Namespace("prod").Delete(ctx, "web-1", metav1.DeleteOptions{}))
	require.Eventually(t, func() bool {
		_, ok := ix.GetEntry("test", "", "pods", "prod", "web-1")
		return !ok
	}, 10*time.Second, 10*time.Millisecond)

	st := c.Status()
	require.Equal(t, "test", st.Name)
	require.NotEmpty(t, st.Resources)
}

func TestGetObjectRedactsSecrets(t *testing.T) {
	secret := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Secret",
		"metadata": map[string]any{
			"name":      "db-creds",
			"namespace": "prod",
			"annotations": map[string]any{
				"kubectl.kubernetes.io/last-applied-configuration": `{"data":{"password":"aHVudGVyMg=="}}`,
				"harmless": "kept",
			},
			"managedFields": []any{map[string]any{"manager": "kubectl"}},
		},
		"data": map[string]any{"password": "aHVudGVyMg=="},
	}}

	scheme := runtime.NewScheme()
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(scheme, map[schema.GroupVersionResource]string{
		{Version: "v1", Resource: "secrets"}: "SecretList",
	}, secret)
	c := NewWithClients("test", summaries.New(), dyn, nil, nil)

	u, err := c.GetObject(context.Background(), summaries.ResourceType{
		Cluster: "test", Version: "v1", Resource: "secrets", Kind: "Secret", Namespaced: true,
	}, "prod", "db-creds")
	require.NoError(t, err)

	data, _, _ := unstructured.NestedMap(u.Object, "data")
	require.Equal(t, "<redacted 12 bytes>", data["password"])
	require.Equal(t, "<redacted>", u.GetAnnotations()["kubectl.kubernetes.io/last-applied-configuration"])
	require.Equal(t, "kept", u.GetAnnotations()["harmless"])
	_, found, _ := unstructured.NestedSlice(u.Object, "metadata", "managedFields")
	require.False(t, found)
}

func eventU(name, ns, about, reason, ts string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Event",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"involvedObject": map[string]any{
			"name": about, "namespace": ns,
		},
		"type":          "Warning",
		"reason":        reason,
		"message":       "msg-" + reason,
		"count":         int64(3),
		"lastTimestamp": ts,
	}}
}

func TestEvents(t *testing.T) {
	ix := summaries.New()
	c := newFakeCluster(t, ix,
		eventU("e1", "prod", "web-1", "BackOff", "2026-07-30T10:00:00Z"),
		eventU("e2", "prod", "web-1", "Unhealthy", "2026-07-30T11:00:00Z"),
		eventU("e3", "prod", "other-pod", "Killing", "2026-07-30T12:00:00Z"),
	)

	events, err := c.Events(context.Background(), "prod", "web-1")
	require.NoError(t, err)
	require.Len(t, events, 2, "events about other objects are excluded")
	require.Equal(t, "Unhealthy", events[0].Reason, "most recent first")
	require.Equal(t, "BackOff", events[1].Reason)
	require.EqualValues(t, 3, events[0].Count)
}

func TestRestConfigFallsBackToKubeconfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "kubeconfig")
	t.Setenv("KUBECONFIG", path)
	t.Setenv("KUBERNETES_SERVICE_HOST", "") // not in a pod
	require.NoError(t, os.WriteFile(path, []byte(`
apiVersion: v1
kind: Config
clusters:
- name: sjc
  cluster: {server: https://sjc-api.example:6443, insecure-skip-tls-verify: true}
- name: nuq
  cluster: {server: https://nuq-api.example:6443, insecure-skip-tls-verify: true}
users:
- name: dev
  user: {token: t0k3n}
contexts:
- name: sjc
  context: {cluster: sjc, user: dev}
- name: nuq
  context: {cluster: nuq, user: dev}
current-context: nuq
`), 0o600))

	rc, err := restConfig()
	require.NoError(t, err)
	require.Equal(t, "https://nuq-api.example:6443", rc.Host, "the kubeconfig's current context")
	require.Equal(t, "t0k3n", rc.BearerToken)
	require.EqualValues(t, 50, rc.QPS)
	require.Equal(t, "atlas", rc.UserAgent)
}

func TestRestConfigNothingAvailable(t *testing.T) {
	t.Setenv("KUBECONFIG", filepath.Join(t.TempDir(), "missing"))
	t.Setenv("KUBERNETES_SERVICE_HOST", "")
	_, err := restConfig()
	require.Error(t, err)
}
