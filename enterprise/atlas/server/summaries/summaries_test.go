package summaries

import (
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	podRes = ResourceType{Cluster: "uswest1", Version: "v1", Resource: "pods", Kind: "Pod", Namespaced: true}
	cmRes  = ResourceType{Cluster: "sjc", Version: "v1", Resource: "configmaps", Kind: "ConfigMap", Namespaced: true}
)

// podEntry is what an ingester would produce for a running pod.
func podEntry(name, ns string) *Entry {
	return &Entry{
		Cluster: podRes.Cluster, Version: podRes.Version, Resource: podRes.Resource, Kind: podRes.Kind,
		Namespace:  ns,
		Name:       name,
		Labels:     map[string]string{"app": "web", "app.kubernetes.io/managed-by": "Helm"},
		Owner:      "ReplicaSet/web-7d9f",
		Phase:      "Running",
		Ready:      "1/1",
		Node:       "node-a",
		IPs:        []string{"10.24.3.7"},
		Images:     []string{"registry.example/web:1.2.3"},
		Containers: []string{"app"},
	}
}

func cmEntry(name, ns string) *Entry {
	return &Entry{
		Cluster: cmRes.Cluster, Version: cmRes.Version, Resource: cmRes.Resource, Kind: cmRes.Kind,
		Namespace: ns,
		Name:      name,
	}
}

func TestStoreLifecycle(t *testing.T) {
	ix := New()
	s := ix.NewStore(podRes)
	require.False(t, s.Synced())

	s.Put(podEntry("a", "prod"))
	s.Put(podEntry("b", "prod"))
	s.Put(podEntry("b", "prod"))
	require.Equal(t, 2, s.Count())

	// Replace swaps the full contents, as an ingester does after a full listing.
	s.Replace([]*Entry{podEntry("c", "prod")})
	require.True(t, s.Synced())
	require.Equal(t, 1, s.Count())
	e, ok := ix.GetEntry("uswest1", "", "pods", "prod", "c")
	require.True(t, ok)
	require.Equal(t, "prod/c", e.Key())

	s.Delete("prod/c")
	s.Delete("prod/c")
	require.Equal(t, 0, s.Count())
	require.Empty(t, s.Entries())
}

func TestRemoveStore(t *testing.T) {
	ix := New()
	s := ix.NewStore(podRes)
	s.Put(podEntry("a", "prod"))
	require.Equal(t, 1, ix.Search("kind:pod", 10).Total)

	ix.RemoveStore(s)
	require.Equal(t, 0, ix.Search("kind:pod", 10).Total)
	_, ok := ix.GetEntry("uswest1", "", "pods", "prod", "a")
	require.False(t, ok)
}

func TestParseQuery(t *testing.T) {
	q := ParseQuery("kind:pod ns:prod cluster:uswest1 label:app=web redis:7.2 Cache")
	require.Equal(t, "pod", q.Kind)
	require.Equal(t, "prod", q.Namespace)
	require.Equal(t, "uswest1", q.Cluster)
	require.Equal(t, []string{"app=web"}, q.Labels)
	// A colon in a non-filter token stays part of the term (image tags).
	require.Equal(t, []string{"redis:7.2", "cache"}, q.Terms)
}

func TestSearch(t *testing.T) {
	ix := New()
	pods := ix.NewStore(podRes)
	pods.Put(podEntry("web-7d9f-abcde", "prod"))
	pods.Put(podEntry("web-7d9f-fghij", "prod"))
	pods.Put(podEntry("cache-0", "staging"))

	cms := ix.NewStore(cmRes)
	cms.Put(cmEntry("web-settings", "prod"))

	t.Run("term matches name substring across kinds", func(t *testing.T) {
		// Three name hits plus cache-0, which matches via its app=web label.
		res := ix.Search("web", 10)
		require.Equal(t, 4, res.Total)
		require.Len(t, res.Groups, 2)
		require.Equal(t, "Pod", res.Groups[0].Kind, "pods rank above configmaps")
	})

	t.Run("exact name floats its kind's group to the top", func(t *testing.T) {
		res := ix.Search("web-settings", 10)
		require.Equal(t, "ConfigMap", res.Groups[0].Kind)
	})

	t.Run("kind filter", func(t *testing.T) {
		res := ix.Search("web kind:configmap", 10)
		require.Equal(t, 1, res.Total)
		require.Equal(t, "ConfigMap", res.Groups[0].Kind)
	})

	t.Run("namespace filter", func(t *testing.T) {
		res := ix.Search("kind:pod ns:staging", 10)
		require.Equal(t, 1, res.Total)
		require.Equal(t, "cache-0", res.Groups[0].Items[0].Name)
	})

	t.Run("cluster filter", func(t *testing.T) {
		require.Equal(t, 1, ix.Search("cluster:sjc", 10).Total)
		require.Equal(t, 3, ix.Search("cluster:uswest1", 10).Total)
	})

	t.Run("label filter", func(t *testing.T) {
		require.Equal(t, 3, ix.Search("label:app=web", 10).Total)
		require.Equal(t, 0, ix.Search("label:app=db", 10).Total)
		require.Equal(t, 3, ix.Search("label:app", 10).Total)
		// Label values are matched case-insensitively, like everything else.
		require.Equal(t, 3, ix.Search("label:app.kubernetes.io/managed-by=Helm", 10).Total)
		require.Equal(t, 3, ix.Search("label:App=WEB", 10).Total)
	})

	t.Run("matches non-name fields via the blob", func(t *testing.T) {
		res := ix.Search("crashloopbackoff", 10)
		require.Equal(t, 0, res.Total)
		require.Equal(t, 3, ix.Search("node-a", 10).Total, "pods match their node name")
		require.Equal(t, 3, ix.Search("example/web:1.2.3", 10).Total, "pods match their image")
	})

	t.Run("group limit caps items but not totals", func(t *testing.T) {
		res := ix.Search("kind:pod", 1)
		require.Equal(t, 3, res.Total)
		require.Equal(t, 3, res.Groups[0].Total)
		require.Len(t, res.Groups[0].Items, 1)
	})

	t.Run("empty query returns nothing", func(t *testing.T) {
		require.Equal(t, 0, ix.Search("", 10).Total)
		require.Equal(t, 0, ix.Search("   ", 10).Total)
	})

	t.Run("a kind shared by two api groups is two groups", func(t *testing.T) {
		ix := New()
		for _, group := range []string{"fleet.example", "capi.example"} {
			res := ResourceType{Cluster: "uswest1", Group: group, Version: "v1", Resource: "clusters", Kind: "Cluster"}
			ix.NewStore(res).Put(&Entry{Cluster: "uswest1", Group: group, Version: "v1", Resource: "clusters", Kind: "Cluster", Name: "east"})
		}
		res := ix.Search("east", 10)
		require.Equal(t, 2, res.Total)
		require.Len(t, res.Groups, 2)
		require.Equal(t, "capi.example", res.Groups[0].Group, "equal scores sort by group")
		require.Equal(t, "fleet.example", res.Groups[1].Group)
	})
}

func TestSearchRanksWholeWords(t *testing.T) {
	ix := New()
	pods := ix.NewStore(podRes)
	for _, name := range []string{"webhooks-0", "web-canary-0", "web", "buildbuddy-app-7d9f-abcde", "buildbuddy-apps-0", "prod-buildbuddy-app-7d9f"} {
		pods.Put(podEntry(name, "prod"))
	}
	crdRes := ResourceType{Cluster: "uswest1", Group: "apiextensions.k8s.io", Version: "v1", Resource: "customresourcedefinitions", Kind: "CustomResourceDefinition"}
	crds := ix.NewStore(crdRes)
	for _, name := range []string{"applications.argoproj.example", "adapters.config.istio.example"} {
		crds.Put(&Entry{Cluster: "uswest1", Group: crdRes.Group, Version: "v1", Resource: crdRes.Resource, Kind: crdRes.Kind, Name: name, Labels: map[string]string{"app": "mixer"}})
	}

	// A whole word beats a prefix of a longer word: the pods of buildbuddy-app
	// outrank the CRD group that "applications" lifts.
	res := ix.Search("app", 10)
	require.Equal(t, "Pod", res.Groups[0].Kind)
	require.Equal(t, "buildbuddy-app-7d9f-abcde", res.Groups[0].Items[0].Name)
	require.Equal(t, "CustomResourceDefinition", res.Groups[1].Kind)

	// Exact, then whole word, then prefix; a hit outside the name (the
	// app=web label) comes last.
	names := []string{}
	for _, e := range ix.Search("web kind:pod", 10).Groups[0].Items {
		names = append(names, e.Name)
	}
	require.Equal(t, []string{"web", "web-canary-0", "webhooks-0", "buildbuddy-app-7d9f-abcde", "buildbuddy-apps-0", "prod-buildbuddy-app-7d9f"}, names)

	// A term spanning words is a whole-word match too, wherever it sits, and
	// beats a prefix of a longer word.
	names = names[:0]
	for _, e := range ix.Search("buildbuddy-app kind:pod", 10).Groups[0].Items {
		names = append(names, e.Name)
	}
	require.Equal(t, []string{"buildbuddy-app-7d9f-abcde", "prod-buildbuddy-app-7d9f", "buildbuddy-apps-0"}, names)
}

func TestSelectorMatches(t *testing.T) {
	require.True(t, SelectorMatches(map[string]string{"app": "web"}, map[string]string{"app": "web", "tier": "fe"}))
	require.False(t, SelectorMatches(map[string]string{"app": "web", "x": "y"}, map[string]string{"app": "web"}))
	require.False(t, SelectorMatches(nil, map[string]string{"app": "web"}), "empty selector selects nothing")
}

func TestScanAndGetEntry(t *testing.T) {
	ix := New()
	pods := ix.NewStore(podRes)
	pods.Put(podEntry("a", "prod"))
	cms := ix.NewStore(cmRes)
	cms.Put(cmEntry("cfg", "prod"))

	var kinds []string
	ix.Scan("", "", func(e *Entry) { kinds = append(kinds, e.Kind) })
	require.ElementsMatch(t, []string{"Pod", "ConfigMap"}, kinds)

	var names []string
	ix.Scan("uswest1", "Pod", func(e *Entry) { names = append(names, e.Name) })
	require.Equal(t, []string{"a"}, names)

	_, ok := ix.GetEntry("uswest1", "", "pods", "prod", "a")
	require.True(t, ok)
	_, ok = ix.GetEntry("uswest1", "", "pods", "prod", "zzz")
	require.False(t, ok)

	// The same resource name in two groups is two resource types.
	core := ix.NewStore(ResourceType{Cluster: "uswest1", Version: "v1", Resource: "events", Kind: "Event", Namespaced: true})
	newer := ix.NewStore(ResourceType{Cluster: "uswest1", Group: "events.k8s.io", Version: "v1", Resource: "events", Kind: "Event", Namespaced: true})
	core.Put(&Entry{Cluster: "uswest1", Version: "v1", Resource: "events", Kind: "Event", Namespace: "prod", Name: "boot"})
	newer.Put(&Entry{Cluster: "uswest1", Group: "events.k8s.io", Version: "v1", Resource: "events", Kind: "Event", Namespace: "prod", Name: "boot"})
	e, ok := ix.GetEntry("uswest1", "events.k8s.io", "events", "prod", "boot")
	require.True(t, ok)
	require.Equal(t, "events.k8s.io", e.Group)
	_, ok = ix.GetEntry("uswest1", "nosuch.example", "events", "prod", "boot")
	require.False(t, ok)
}
