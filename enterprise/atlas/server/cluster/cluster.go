// Package cluster connects atlas to a Kubernetes cluster and keeps the index
// current.
package cluster

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/k8singest"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/summaries"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/random"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/metadata"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	clusterName         = flag.String("atlas.cluster_name", "", "How the cluster appears in search results and URLs.")
	svcZone             = flag.String("atlas.svc_zone", "", "Tunnel DNS zone that rewrites to svc.cluster.local.")
	podZone             = flag.String("atlas.pod_zone", "", "Tunnel DNS zone that rewrites to pod.cluster.local.")
	ignoreResourceTypes = flag.Slice("atlas.ignore_resource_types", []string{
		// High-churn resource types.
		"events", "leases", "endpointslices", "endpoints",
		// Listing componentstatuses is deprecated and fails on modern servers.
		"componentstatuses",
	}, "Resource names to exclude from indexing.")
	discoveryInterval = flag.Duration("atlas.discovery_interval", time.Hour, "How often to re-run API discovery to pick up new resource types (CRDs).")
)

// richKinds are fetched as full objects so their index entries carry status,
// ports and images. Everything else is indexed from metadata alone.
var richKinds = map[schema.GroupResource]bool{
	{Resource: "pods"}:                                  true,
	{Resource: "services"}:                              true,
	{Resource: "nodes"}:                                 true,
	{Resource: "namespaces"}:                            true,
	{Group: "apps", Resource: "deployments"}:            true,
	{Group: "apps", Resource: "statefulsets"}:           true,
	{Group: "apps", Resource: "daemonsets"}:             true,
	{Group: "apps", Resource: "replicasets"}:            true,
	{Group: "batch", Resource: "jobs"}:                  true,
	{Group: "batch", Resource: "cronjobs"}:              true,
	{Group: "networking.k8s.io", Resource: "ingresses"}: true,
}

// Cluster is one connected cluster.
type Cluster struct {
	name  string
	ix    *summaries.Index
	dyn   dynamic.Interface
	meta  metadata.Interface
	disco discovery.DiscoveryInterface
	logs  LogSource

	mu            sync.Mutex
	watchers      map[schema.GroupVersionResource]*watcher
	discoveryErr  error
	lastDiscovery time.Time
}

// New connects the configured cluster.
func New(ix *summaries.Index) (*Cluster, error) {
	if *clusterName == "" {
		return nil, fmt.Errorf("atlas.cluster_name is required")
	}
	rc, err := restConfig()
	if err != nil {
		return nil, err
	}
	dyn, err := dynamic.NewForConfig(rc)
	if err != nil {
		return nil, err
	}
	meta, err := metadata.NewForConfig(rc)
	if err != nil {
		return nil, err
	}
	disco, err := discovery.NewDiscoveryClientForConfig(rc)
	if err != nil {
		return nil, err
	}
	logs, err := newRESTLogSource(rc)
	if err != nil {
		return nil, err
	}
	return NewWithClients(*clusterName, ix, dyn, meta, disco).WithLogSource(logs), nil
}

// NewWithClients wires a cluster from explicit clients, for tests.
func NewWithClients(name string, ix *summaries.Index, dyn dynamic.Interface, meta metadata.Interface, disco discovery.DiscoveryInterface) *Cluster {
	return &Cluster{
		name:     name,
		ix:       ix,
		dyn:      dyn,
		meta:     meta,
		disco:    disco,
		watchers: map[schema.GroupVersionResource]*watcher{},
	}
}

// restConfig uses the pod's service account when running in a cluster and
// otherwise the current context of the default kubeconfig ($KUBECONFIG or
// ~/.kube/config), for running atlas on a workstation.
func restConfig() (*rest.Config, error) {
	rc, err := rest.InClusterConfig()
	if errors.Is(err, rest.ErrNotInCluster) {
		rc, err = clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
			clientcmd.NewDefaultClientConfigLoadingRules(),
			&clientcmd.ConfigOverrides{},
		).ClientConfig()
	}
	if err != nil {
		return nil, err
	}
	rc.QPS = 50
	rc.Burst = 100
	rc.UserAgent = "atlas"
	return rc, nil
}

func (c *Cluster) Name() string             { return c.name }
func (c *Cluster) Zones() (svc, pod string) { return *svcZone, *podZone }
func (c *Cluster) Index() *summaries.Index  { return c.ix }

// Run discovers resource types and keeps their watchers running until ctx
// ends. New types (CRDs) are picked up on periodic rediscovery.
func (c *Cluster) Run(ctx context.Context) {
	for {
		c.discover(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(*discoveryInterval):
		}
	}
}

func (c *Cluster) discover(ctx context.Context) {
	resources, err := c.listWatchableResources()
	c.mu.Lock()
	c.discoveryErr = err
	c.lastDiscovery = time.Now()
	c.mu.Unlock()
	if err != nil && len(resources) == 0 {
		log.Warningf("Cluster %q: API discovery failed: %s", c.name, err)
		return
	}
	if err != nil {
		// Partial discovery (a broken aggregated API) still yields the rest.
		log.Warningf("Cluster %q: partial API discovery: %s", c.name, err)
	}
	want := map[schema.GroupVersionResource]bool{}
	for _, r := range resources {
		want[r.gvr] = true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Types discovery no longer reports (a deleted CRD, a preferred version
	// that moved) stop being watched and leave the index. Not after a partial
	// discovery, which is missing whole groups that still exist.
	if err == nil {
		for gvr, w := range c.watchers {
			if !want[gvr] {
				w.cancel()
				c.ix.RemoveStore(w.store)
				delete(c.watchers, gvr)
			}
		}
	}
	for _, r := range resources {
		if _, exists := c.watchers[r.gvr]; !exists {
			c.watchers[r.gvr] = c.startWatcher(ctx, r)
		}
	}
}

type discoveredResource struct {
	gvr  schema.GroupVersionResource
	res  summaries.ResourceType
	rich bool
}

// listWatchableResources returns every resource type the server can list and
// watch, one preferred version per group, minus the ignored types.
func (c *Cluster) listWatchableResources() ([]discoveredResource, error) {
	groups, lists, err := c.disco.ServerGroupsAndResources()
	// A discovery error can still come with usable partial results.
	if len(lists) == 0 && err != nil {
		return nil, err
	}

	preferred := map[string]string{} // group -> preferred groupVersion
	for _, g := range groups {
		preferred[g.Name] = g.PreferredVersion.GroupVersion
	}

	ignored := map[string]bool{}
	for _, r := range *ignoreResourceTypes {
		ignored[r] = true
	}

	var out []discoveredResource
	seenGroupResource := map[schema.GroupResource]bool{}
	for _, list := range lists {
		gv, perr := schema.ParseGroupVersion(list.GroupVersion)
		if perr != nil {
			continue
		}
		if want, ok := preferred[gv.Group]; ok && want != "" && want != list.GroupVersion {
			continue
		}
		for _, r := range list.APIResources {
			if strings.Contains(r.Name, "/") { // subresource
				continue
			}
			if ignored[r.Name] {
				continue
			}
			verbs := map[string]bool{}
			for _, v := range r.Verbs {
				verbs[v] = true
			}
			if !verbs["list"] || !verbs["watch"] {
				continue
			}
			gr := schema.GroupResource{Group: gv.Group, Resource: r.Name}
			if seenGroupResource[gr] {
				continue
			}
			seenGroupResource[gr] = true
			out = append(out, discoveredResource{
				gvr: gv.WithResource(r.Name),
				res: summaries.ResourceType{
					Cluster:    c.name,
					Group:      gv.Group,
					Version:    gv.Version,
					Resource:   r.Name,
					Kind:       r.Kind,
					Namespaced: r.Namespaced,
				},
				rich: richKinds[gr],
			})
		}
	}
	slices.SortFunc(out, func(a, b discoveredResource) int { return cmp.Compare(a.res.Resource, b.res.Resource) })
	return out, err
}

// watcher keeps one resource type synced into its index store.
type watcher struct {
	res    summaries.ResourceType
	store  *summaries.Store
	cancel context.CancelFunc
	done   chan struct{} // closed when run returns

	mu      sync.Mutex
	lastErr error
	errTime time.Time
}

func (c *Cluster) startWatcher(ctx context.Context, r discoveredResource) *watcher {
	ctx, cancel := context.WithCancel(ctx)
	w := &watcher{res: r.res, store: c.ix.NewStore(r.res), cancel: cancel, done: make(chan struct{})}

	var lw cache.ListerWatcher
	var example runtime.Object
	if r.rich {
		ri := c.dyn.Resource(r.gvr)
		lw = cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) {
				return ri.List(ctx, o)
			},
			WatchFuncWithContext: ri.Watch,
		}, c.dyn)
		example = &unstructured.Unstructured{}
	} else {
		ri := c.meta.Resource(r.gvr)
		lw = cache.ToListWatcherWithWatchListSemantics(&cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, o metav1.ListOptions) (runtime.Object, error) {
				return ri.List(ctx, o)
			},
			WatchFuncWithContext: ri.Watch,
		}, c.meta)
		example = &metav1.PartialObjectMetadata{}
	}

	go w.run(ctx, lw, example)
	return w
}

func (w *watcher) run(ctx context.Context, lw cache.ListerWatcher, example runtime.Object) {
	defer close(w.done)
	r := cache.NewReflectorWithOptions(lw, example, k8singest.NewStore(w.store), cache.ReflectorOptions{
		Name: w.res.String(),
	})
	backoff := time.Second
	for {
		err := r.ListAndWatch(ctx.Done())
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			w.mu.Lock()
			w.lastErr = err
			w.errTime = time.Now()
			w.mu.Unlock()
			log.Warningf("Watch %s: %s", w.res, err)
		}
		delay := backoff + time.Duration(random.RandUint64()%uint64(backoff/2+1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
		if backoff *= 2; backoff > 2*time.Minute {
			backoff = 2 * time.Minute
		}
	}
}

// ResourceTypeStatus describes one watched resource type for the status API.
type ResourceTypeStatus struct {
	summaries.ResourceType
	Count   int
	Synced  bool
	Error   string
	ErrTime time.Time
}

// Status describes one cluster for the status API.
type Status struct {
	Name           string
	SvcZone        string
	PodZone        string
	DiscoveryError string
	LastDiscovery  time.Time
	TotalObjects   int
	Resources      []ResourceTypeStatus
}

func (c *Cluster) Status() Status {
	c.mu.Lock()
	st := Status{
		Name:          c.name,
		SvcZone:       *svcZone,
		PodZone:       *podZone,
		LastDiscovery: c.lastDiscovery,
	}
	if c.discoveryErr != nil {
		st.DiscoveryError = c.discoveryErr.Error()
	}
	watchers := make([]*watcher, 0, len(c.watchers))
	for _, w := range c.watchers {
		watchers = append(watchers, w)
	}
	c.mu.Unlock()

	for _, w := range watchers {
		rs := ResourceTypeStatus{
			ResourceType: w.res,
			Count:        w.store.Count(),
			Synced:       w.store.Synced(),
		}
		w.mu.Lock()
		if w.lastErr != nil {
			rs.Error = w.lastErr.Error()
			rs.ErrTime = w.errTime
		}
		w.mu.Unlock()
		st.TotalObjects += rs.Count
		st.Resources = append(st.Resources, rs)
	}
	slices.SortFunc(st.Resources, func(a, b ResourceTypeStatus) int {
		return cmp.Compare(a.ResourceType.Resource, b.ResourceType.Resource)
	})
	return st
}

// GetObject fetches the live object, redacted for display.
func (c *Cluster) GetObject(ctx context.Context, res summaries.ResourceType, namespace, name string) (*unstructured.Unstructured, error) {
	ri := c.dyn.Resource(schema.GroupVersionResource{Group: res.Group, Version: res.Version, Resource: res.Resource})
	var u *unstructured.Unstructured
	var err error
	if res.Namespaced {
		u, err = ri.Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	} else {
		u, err = ri.Get(ctx, name, metav1.GetOptions{})
	}
	if err != nil {
		return nil, err
	}
	Redact(u)
	return u, nil
}

// Redact strips noise and secret material from an object before display.
func Redact(u *unstructured.Unstructured) {
	unstructured.RemoveNestedField(u.Object, "metadata", "managedFields")
	if u.GetKind() != "Secret" {
		return
	}
	if data, ok, _ := unstructured.NestedMap(u.Object, "data"); ok {
		for k, v := range data {
			n := 0
			if s, ok := v.(string); ok {
				n = len(s)
			}
			data[k] = fmt.Sprintf("<redacted %d bytes>", n)
		}
		_ = unstructured.SetNestedMap(u.Object, data, "data")
	}
	if data, ok, _ := unstructured.NestedMap(u.Object, "stringData"); ok {
		for k := range data {
			data[k] = "<redacted>"
		}
		_ = unstructured.SetNestedMap(u.Object, data, "stringData")
	}
	// The last-applied annotation embeds the full secret too.
	annotations := u.GetAnnotations()
	if _, ok := annotations["kubectl.kubernetes.io/last-applied-configuration"]; ok {
		annotations["kubectl.kubernetes.io/last-applied-configuration"] = "<redacted>"
		u.SetAnnotations(annotations)
	}
}

// Event is a display-ready cluster event.
type Event struct {
	Type     string
	Reason   string
	Message  string
	Count    int64
	LastSeen time.Time
}

// Events returns recent events about the object, most recent first.
func (c *Cluster) Events(ctx context.Context, obj *unstructured.Unstructured) ([]Event, error) {
	namespace, name, kind, uid := obj.GetNamespace(), obj.GetName(), obj.GetKind(), string(obj.GetUID())
	ri := c.dyn.Resource(schema.GroupVersionResource{Version: "v1", Resource: "events"})
	sel := "involvedObject.name=" + name + ",involvedObject.kind=" + kind
	if namespace != "" {
		sel += ",involvedObject.namespace=" + namespace
	}
	list, err := ri.Namespace(namespace).List(ctx, metav1.ListOptions{FieldSelector: sel})
	if err != nil {
		return nil, err
	}
	var out []Event
	for _, item := range list.Items {
		// An event that recorded a uid must be about this object, not an
		// earlier one of the same name. Those without one are kept, which is
		// why uid is not in the selector.
		if u, _, _ := unstructured.NestedString(item.Object, "involvedObject", "uid"); u != "" && uid != "" && u != uid {
			continue
		}
		ev := Event{}
		ev.Type, _, _ = unstructured.NestedString(item.Object, "type")
		ev.Reason, _, _ = unstructured.NestedString(item.Object, "reason")
		ev.Message, _, _ = unstructured.NestedString(item.Object, "message")
		ev.Count, _, _ = unstructured.NestedInt64(item.Object, "count")
		ev.LastSeen = latestEventTime(&item)
		out = append(out, ev)
	}
	slices.SortStableFunc(out, func(a, b Event) int { return b.LastSeen.Compare(a.LastSeen) })
	return out[:min(len(out), maxEvents)], nil
}

// maxEvents caps how many of an object's events are returned, newest first.
const maxEvents = 100

func latestEventTime(u *unstructured.Unstructured) time.Time {
	for _, path := range [][]string{{"lastTimestamp"}, {"eventTime"}, {"firstTimestamp"}} {
		if s, ok, _ := unstructured.NestedString(u.Object, path...); ok && s != "" {
			if t, err := time.Parse(time.RFC3339, s); err == nil {
				return t
			}
			// eventTime carries microseconds.
			if t, err := time.Parse("2006-01-02T15:04:05.000000Z07:00", s); err == nil {
				return t
			}
		}
	}
	return u.GetCreationTimestamp().Time
}
