package cluster

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/summaries"
	"github.com/stretchr/testify/require"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
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
	// client-go's fakes ignore field selectors. Apply them to event lists so
	// the tests exercise the selector Events sends.
	dyn.PrependReactor("list", "events", func(action clienttesting.Action) (bool, runtime.Object, error) {
		la := action.(clienttesting.ListAction)
		obj, err := dyn.Tracker().List(eventsGVR, schema.GroupVersionKind{Version: "v1", Kind: "Event"}, la.GetNamespace())
		if err != nil {
			return true, nil, err
		}
		list := obj.(*unstructured.UnstructuredList)
		list.Items = slices.DeleteFunc(list.Items, func(e unstructured.Unstructured) bool {
			about, _, _ := unstructured.NestedStringMap(e.Object, "involvedObject")
			set := fields.Set{}
			for k, v := range about {
				set["involvedObject."+k] = v
			}
			return !la.GetListRestrictions().Fields.Matches(set)
		})
		return true, list, nil
	})

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

// overrideWatcherReadyTimeout changes the watcher readiness timeout for testing.
func overrideWatcherReadyTimeout(t *testing.T, d time.Duration) {
	t.Helper()
	was := watcherReadyTimeout
	watcherReadyTimeout = d
	t.Cleanup(func() { watcherReadyTimeout = was })
}

func TestReady(t *testing.T) {
	overrideWatcherReadyTimeout(t, 100*time.Millisecond)
	ix := summaries.New()
	c := newFakeCluster(t, ix, podU("web-1", "prod"))
	require.ErrorContains(t, c.Ready(), "waiting for API discovery")

	// Deployments are refused by the server (a type the service account may
	// not read), and configmaps take their time to list.
	var forbidDeployments atomic.Bool
	forbidDeployments.Store(true)
	deploymentsMayList := make(chan struct{})
	t.Cleanup(func() { close(deploymentsMayList) })
	c.dyn.(*dynamicfake.FakeDynamicClient).PrependReactor("list", "deployments", func(clienttesting.Action) (bool, runtime.Object, error) {
		if forbidDeployments.Load() {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, "", errors.New("no"))
		}
		<-deploymentsMayList
		return false, nil, nil
	})
	configmapsMayList := make(chan struct{})
	releaseConfigmaps := sync.OnceFunc(func() { close(configmapsMayList) })
	t.Cleanup(releaseConfigmaps)
	c.meta.(*metadatafake.FakeMetadataClient).PrependReactor("list", "configmaps", func(clienttesting.Action) (bool, runtime.Object, error) {
		<-configmapsMayList
		return false, nil, nil
	})
	ctx := t.Context()
	c.discover(ctx)

	// Pods sync, deployments are given up on once they have failed for a
	// while; configmaps are what is still missing.
	require.Eventually(t, func() bool {
		err := c.Ready()
		return err != nil && err.Error() == "1 of 3 resource types still syncing"
	}, 10*time.Second, 10*time.Millisecond, "last: %v", c.Ready())
	releaseConfigmaps()
	require.Eventually(t, func() bool { return c.Ready() == nil }, 10*time.Second, 10*time.Millisecond)

	// Later rediscovery brings deployments back as a type that never finishes
	// listing. The instance stays ready; the new type syncs in the background.
	forbidDeployments.Store(false)
	disco := c.disco.(*fakedisco.FakeDiscovery)
	var without []*metav1.APIResourceList
	for _, list := range testResources() {
		list.APIResources = slices.DeleteFunc(list.APIResources, func(r metav1.APIResource) bool { return r.Name == "deployments" })
		without = append(without, list)
	}
	disco.Resources = without
	c.discover(ctx)
	disco.Resources = testResources()
	c.discover(ctx)
	require.NoError(t, c.Ready())
	for _, r := range c.Status().Resources {
		if r.Resource == "deployments" {
			require.False(t, r.Synced, "deployments really are still listing")
		}
	}
}

func TestReadyWaitsOutABlip(t *testing.T) {
	// Longer than the first retry, so a type that fails once is waited for.
	overrideWatcherReadyTimeout(t, 3*time.Second)
	ix := summaries.New()
	c := newFakeCluster(t, ix, podU("web-1", "prod"))
	var lists atomic.Int32
	c.dyn.(*dynamicfake.FakeDynamicClient).PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		if lists.Add(1) == 1 {
			return true, nil, apierrors.NewTooManyRequests("slow down", 1)
		}
		return false, nil, nil
	})
	c.discover(t.Context())

	require.Eventually(t, func() bool { return c.Ready() == nil }, 10*time.Second, 10*time.Millisecond)
	for _, r := range c.Status().Resources {
		if r.Resource == "pods" {
			require.True(t, r.Synced, "ready only once the retry had listed pods")
		}
	}
	require.EqualValues(t, 2, lists.Load())
}

func TestReadyNeedsOneSyncedType(t *testing.T) {
	overrideWatcherReadyTimeout(t, 100*time.Millisecond)
	ix := summaries.New()
	c := newFakeCluster(t, ix)
	refuse := func(clienttesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{}, "", errors.New("no"))
	}
	dyn := c.dyn.(*dynamicfake.FakeDynamicClient)
	dyn.PrependReactor("list", "pods", refuse)
	dyn.PrependReactor("list", "deployments", refuse)
	c.meta.(*metadatafake.FakeMetadataClient).PrependReactor("list", "configmaps", refuse)
	c.discover(t.Context())

	// Every type is given up on, and that must not pass for caught up.
	require.Eventually(t, func() bool {
		err := c.Ready()
		return err != nil && err.Error() == "no resource type has synced"
	}, 10*time.Second, 10*time.Millisecond, "last: %v", c.Ready())
}

func TestRediscoveryDropsVanishedTypes(t *testing.T) {
	ix := summaries.New()
	c := newFakeCluster(t, ix, podU("web-1", "prod"))
	ctx := t.Context()
	c.discover(ctx)
	require.Eventually(t, func() bool {
		_, ok := ix.GetEntry("test", "", "pods", "prod", "web-1")
		return ok
	}, 10*time.Second, 10*time.Millisecond)
	c.mu.Lock()
	pods := c.watchers[podsGVR]
	c.mu.Unlock()
	require.NotNil(t, pods)

	// Discovery stops reporting pods, as after a CRD is deleted or its
	// preferred version moves.
	var remaining []*metav1.APIResourceList
	for _, list := range testResources() {
		list.APIResources = slices.DeleteFunc(list.APIResources, func(r metav1.APIResource) bool { return r.Name == "pods" })
		remaining = append(remaining, list)
	}
	c.disco.(*fakedisco.FakeDiscovery).Resources = remaining
	c.discover(ctx)

	_, ok := ix.GetEntry("test", "", "pods", "prod", "web-1")
	require.False(t, ok, "the store left the index")
	require.Equal(t, 0, ix.Search("web-1", 10).Total)
	c.mu.Lock()
	_, watching := c.watchers[podsGVR]
	c.mu.Unlock()
	require.False(t, watching)
	select {
	case <-pods.done:
	case <-time.After(10 * time.Second):
		t.Fatal("the old watcher kept running")
	}
}

func TestEventsKeepsNewest(t *testing.T) {
	var objs []runtime.Object
	for i := range maxEvents + 5 {
		ts := time.Date(2026, 7, 30, 0, 0, i, 0, time.UTC).Format(time.RFC3339)
		objs = append(objs, eventU(fmt.Sprintf("e%d", i), "prod", "Pod", "web-1", "", fmt.Sprintf("r%d", i), ts))
	}
	c := newFakeCluster(t, summaries.New(), objs...)

	events, err := c.Events(context.Background(), podU("web-1", "prod"))
	require.NoError(t, err)
	require.Len(t, events, maxEvents)
	require.Equal(t, fmt.Sprintf("r%d", maxEvents+4), events[0].Reason, "the newest survive the cut")
	require.Equal(t, "r5", events[maxEvents-1].Reason, "the oldest are dropped")
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

func eventU(name, ns, aboutKind, about, aboutUID, reason, ts string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1",
		"kind":       "Event",
		"metadata":   map[string]any{"name": name, "namespace": ns},
		"involvedObject": map[string]any{
			"kind": aboutKind, "name": about, "namespace": ns, "uid": aboutUID,
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
		eventU("e1", "prod", "Pod", "web-1", "pod-uid", "BackOff", "2026-07-30T10:00:00Z"),
		eventU("e2", "prod", "Pod", "web-1", "", "Unhealthy", "2026-07-30T11:00:00Z"), // no uid recorded
		eventU("e3", "prod", "Pod", "other-pod", "", "Killing", "2026-07-30T12:00:00Z"),
		eventU("e4", "prod", "Service", "web-1", "", "SyncFailed", "2026-07-30T12:00:00Z"),
		eventU("e5", "prod", "Pod", "web-1", "old-pod-uid", "Started", "2026-07-30T13:00:00Z"),
	)

	pod := podU("web-1", "prod")
	pod.SetUID("pod-uid")
	events, err := c.Events(context.Background(), pod)
	require.NoError(t, err)
	require.Len(t, events, 2, "other objects, same-named other kinds and an earlier pod of that name are excluded")
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
