// Package kuberesolver provides a gRPC resolver that uses the Kubernetes API
// to resolve pod FQDNs to pod IPs. It registers a "kube" scheme resolver.
// Unlike the default DNS resolver, the k8s watch API allows the resolver to
// be notified promptly when the IP changes.
//
// Usage:
//
//	import _ "github.com/buildbuddy-io/buildbuddy/server/util/kuberesolver"
//
//	conn, err := grpc.Dial("kube:///pod-name.service.namespace.svc.cluster.local:4772", ...)
//
// The client pod must have get/list/watch pod API permissions.
//
// Non-gRPC consumers can use the PodWatcherManager directly:
//
//	cancel, err := kuberesolver.DefaultManager().WatchPodIP("pod.svc.ns.svc.cluster.local:7238", func(ipPort string, err error) { ... })
package kuberesolver

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/resolver"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RunningInKubernetes reports whether the process is running inside a
// Kubernetes cluster, based on the KUBERNETES_SERVICE_HOST env var that
// the kubelet injects into every pod.
func RunningInKubernetes() bool {
	return os.Getenv("KUBERNETES_SERVICE_HOST") != ""
}

const scheme = "kube"
const apiMaxBackoff = 30 * time.Second

// podTarget holds the parsed components of a pod FQDN target.
type podTarget struct {
	podName     string
	serviceName string
	namespace   string
	port        string
}

// parsePodTarget parses a target like
// "metadata-server-0.headless.metadata-server-dev.svc.cluster.local:4772"
// into its component parts.
func parsePodTarget(endpoint string) (podTarget, error) {
	host := endpoint
	port := ""

	if idx := strings.LastIndex(endpoint, ":"); idx != -1 {
		host = endpoint[:idx]
		port = endpoint[idx+1:]
	}

	// Expected format: <podName>.<serviceName>.<namespace>.svc.cluster.local
	parts := strings.SplitN(host, ".", 4)
	if len(parts) < 3 {
		return podTarget{}, fmt.Errorf("invalid pod FQDN %q: expected <pod>.<service>.<namespace>[.svc.cluster.local]", host)
	}

	return podTarget{
		podName:     parts[0],
		serviceName: parts[1],
		namespace:   parts[2],
		port:        port,
	}, nil
}

type podResolution struct {
	pod *corev1.Pod
	err error
}

func (pr podResolution) String() string {
	if pr.err != nil {
		return fmt.Sprintf("error: %s", pr.err)
	}
	if pr.pod.Status.PodIP == "" {
		return "no pod IP available yet"
	}
	return fmt.Sprintf("pod resolved to IP %s", pr.pod.Status.PodIP)
}

// subscription is an opaque token returned by subscribe, used as a map key
// for unsubscribing. The dummy field ensures each &subscription{} allocation
// has a unique address (zero-sized types in Go may share addresses).
type subscription struct{ _ int }

// podWatcher owns a single Get+Watch loop for a specific (namespace, podName)
// pair. Multiple consumers can subscribe to the same podWatcher.
type podWatcher struct {
	client    kubernetes.Interface
	namespace string
	podName   string
	ctx       context.Context

	// Only accessed from the resolveAndWatch goroutine.
	resourceVersion string

	mu               sync.Mutex
	subscribers      map[*subscription]func(podResolution)
	cancel           context.CancelFunc
	latestResolution *podResolution
}

func newPodWatcher(ctx context.Context, client kubernetes.Interface, namespace, podName string) *podWatcher {
	ctx, cancel := context.WithCancel(ctx)
	pw := &podWatcher{
		client:      client,
		namespace:   namespace,
		podName:     podName,
		ctx:         ctx,
		subscribers: make(map[*subscription]func(podResolution)),
		cancel:      cancel,
	}
	go pw.resolveAndWatch(ctx)
	return pw
}

// subscribe adds a subscriber with callback cb. cb will be called immediately
// with the latest known state (pod or error) and then whenever the pod state
// changes. cb must not block. Returns a subscription token for unsubscribing.
func (pw *podWatcher) subscribe(cb func(podResolution)) *subscription {
	s := &subscription{}
	pw.mu.Lock()
	pw.subscribers[s] = cb
	if pw.latestResolution != nil {
		log.Infof("Sending existing state to new subscriber for pod %s/%s. Notification: %s", pw.namespace, pw.podName, pw.latestResolution)
		cb(*pw.latestResolution)
		log.Infof("Finished sending existing state to new subscriber for pod %s/%s.", pw.namespace, pw.podName)
	}
	pw.mu.Unlock()
	return s
}

// unsubscribe removes a subscriber. It returns true if no subscribers remain.
func (pw *podWatcher) unsubscribe(s *subscription) bool {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	delete(pw.subscribers, s)
	return len(pw.subscribers) == 0
}

func (pw *podWatcher) notifySubscribers(r podResolution) {
	pw.mu.Lock()
	pw.latestResolution = &r
	callbacks := make([]func(podResolution), 0, len(pw.subscribers))
	for _, cb := range pw.subscribers {
		callbacks = append(callbacks, cb)
	}
	pw.mu.Unlock()

	log.Infof("Notifying %d subscribers for pod %s/%s. Notification: %s", len(callbacks), pw.namespace, pw.podName, r)
	for _, cb := range callbacks {
		cb(r)
	}
	log.Infof("Finished notifying %d subscribers for pod %s/%s", len(callbacks), pw.namespace, pw.podName)
}

// resolve does a one-shot List for the pod and notifies subscribers.
// Returns true on success.
func (pw *podWatcher) resolve(ctx context.Context) bool {
	log.Infof("Resolving pod %s/%s", pw.namespace, pw.podName)
	podList, err := pw.client.CoreV1().Pods(pw.namespace).List(ctx, metav1.ListOptions{
		FieldSelector: "metadata.name=" + pw.podName,
	})
	if err != nil {
		log.Warningf("Failed to list pod %s/%s from k8s: %s", pw.namespace, pw.podName, err)
		pw.notifySubscribers(podResolution{err: fmt.Errorf("failed to list pod: %w", err)})
		return false
	}
	pw.resourceVersion = podList.ResourceVersion
	if len(podList.Items) == 0 {
		log.Infof("Failed to list pod %s/%s from k8s: pod not found", pw.namespace, pw.podName)
		pw.notifySubscribers(podResolution{err: fmt.Errorf("pod %s/%s not found", pw.namespace, pw.podName)})
		// Treat this as a success since we can rely on the Watch API to
		// tell us when the pod comes into existence.
		return true
	}
	pw.notifySubscribers(podResolution{pod: &podList.Items[0]})
	return true
}

// resolveAndWatch retrieves the pod information and then watches the pod for
// changes using the k8s Watch API.
func (pw *podWatcher) resolveAndWatch(ctx context.Context) {
	for {
		// Resolve the pod via a Get call. Retry until it succeeds so
		// that the watch has a valid resourceVersion to start from.
		backoff := time.Second
		for !pw.resolve(ctx) {
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, apiMaxBackoff)
			case <-ctx.Done():
				return
			}
		}

		if done := pw.watch(ctx); done {
			return
		}
		// If the watch failed, retry the entire Get+Watch loop.
		pw.resourceVersion = ""
	}
}

// watch uses the k8s Watch API to watch the pod for changes.
// The watch is started at version pw.resourceVersion.
// The return value indicates if the watch process is terminated (because the
// the context was done). If the value is false, the caller should retrieve
// the pod information and retry the watch call with a new pw.resourceVersion
// value.
func (pw *podWatcher) watch(ctx context.Context) (done bool) {
	backoff := time.Second

	for {
		select {
		case <-ctx.Done():
			return true
		default:
		}

		log.Infof("Watching pod %s/%s for updates.", pw.namespace, pw.podName)
		watcher, err := pw.client.CoreV1().Pods(pw.namespace).Watch(ctx, metav1.ListOptions{
			FieldSelector:   "metadata.name=" + pw.podName,
			ResourceVersion: pw.resourceVersion,
		})
		if err != nil {
			if errors.IsResourceExpired(err) {
				log.Infof("Failed to watch pod %s/%s due to old resource version, will refetch pod: %s", pw.namespace, pw.podName, err)
				return false
			}
			log.Warningf("Failed to watch pod %s/%s: %s", pw.namespace, pw.podName, err)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, apiMaxBackoff)
			case <-ctx.Done():
				return true
			}
			continue
		}

		if err := pw.processWatchEvents(ctx, watcher); err != nil {
			watcher.Stop()
			if errors.IsResourceExpired(err) {
				log.Infof("Watch for for pod %s/%s failed due to old resource version, will refetch pod", pw.namespace, pw.podName)
				return false
			}
			if status.IsCanceledError(err) {
				log.Infof("Watch for for pod %s/%s closed by server, will refetch pod", pw.namespace, pw.podName)
				return false
			}
			log.Warningf("Watch for pod %s/%s ended abruptly: %s", pw.namespace, pw.podName, err)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, apiMaxBackoff)
			case <-ctx.Done():
				return true
			}
			continue
		}

		backoff = time.Second
		watcher.Stop()
	}
}

// processWatchEvents processes events received from the resolveAndWatch stream.
// A non-nil error is returned when the resolveAndWatch ends abruptly.
func (pw *podWatcher) processWatchEvents(ctx context.Context, watcher watch.Interface) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				// There's a limit on how long the server will keep the watch
				// open and the server can restart at any time as well.
				return status.CanceledErrorf("watch closed by server")
			}
			if obj, ok := event.Object.(metav1.ObjectMetaAccessor); ok {
				if rv := obj.GetObjectMeta().GetResourceVersion(); rv != "" {
					pw.resourceVersion = rv
				}
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				pod, ok := event.Object.(*corev1.Pod)
				if !ok {
					continue
				}
				pw.notifySubscribers(podResolution{pod: pod})
			case watch.Deleted:
				log.Warningf("Pod %s/%s was deleted", pw.namespace, pw.podName)
				pw.notifySubscribers(podResolution{err: fmt.Errorf("pod %s/%s was deleted", pw.namespace, pw.podName)})
			case watch.Bookmark:
				// No-op: bookmark events are informational.
			case watch.Error:
				err := errors.FromObject(event.Object)
				log.Warningf("Watch error for pod %s/%s: %s", pw.namespace, pw.podName, err)
				return fmt.Errorf("watch error: %w", err)
			}
		}
	}
}

// PodWatcherManager manages pod watchers. One watcher is created per unique
// (namespace, podName) pair regardless of how many consumers subscribe.
// It can be used by both the gRPC resolver and non-gRPC consumers (e.g., the
// raft registry) to get instant pod IP updates via the k8s Watch API.
type PodWatcherManager struct {
	mu       sync.Mutex
	client   kubernetes.Interface
	watchers map[string]*podWatcher
}

// NewPodWatcherManager creates a PodWatcherManager. If client is nil,
// the manager will lazily create a client using InClusterConfig.
func NewPodWatcherManager(client kubernetes.Interface) *PodWatcherManager {
	return &PodWatcherManager{
		client:   client,
		watchers: make(map[string]*podWatcher),
	}
}

func (m *PodWatcherManager) getOrCreateClient() (kubernetes.Interface, error) {
	// Caller must hold m.mu or call from a context where client is already set.
	if m.client != nil {
		return m.client, nil
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("could not create k8s client: %w", err)
	}
	m.client, err = kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("could not create k8s client: %w", err)
	}
	return m.client, nil
}

// WatchPodIP starts watching the pod IP for the given hostPort string
// (format: "podName.serviceName.namespace[.svc.cluster.local]:port").
// The callback is called immediately with the current state and then on
// every change. The returned function stops the watch.
// If hostPort cannot be parsed as a pod FQDN, returns an error.
func (m *PodWatcherManager) WatchPodIP(hostPort string, cb func(ipPort string, err error)) (cancel func(), retErr error) {
	pt, err := parsePodTarget(hostPort)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	client, err := m.getOrCreateClient()
	if err != nil {
		m.mu.Unlock()
		return nil, err
	}
	key := watcherKey(pt.namespace, pt.podName)
	pw := m.watchers[key]
	if pw == nil {
		pw = newPodWatcher(context.Background(), client, pt.namespace, pt.podName)
		m.watchers[key] = pw
	}
	m.mu.Unlock()

	sub := pw.subscribe(func(r podResolution) {
		if r.err != nil {
			cb("", r.err)
			return
		}
		ip := r.pod.Status.PodIP
		if ip == "" {
			cb("", fmt.Errorf("pod %s/%s doesn't have an IP yet", pt.namespace, pt.podName))
			return
		}
		addr := ip
		if pt.port != "" {
			addr = ip + ":" + pt.port
		}
		cb(addr, nil)
	})

	return func() {
		m.mu.Lock()
		defer m.mu.Unlock()
		if pw.unsubscribe(sub) {
			pw.cancel()
			delete(m.watchers, watcherKey(pw.namespace, pw.podName))
		}
	}, nil
}

var defaultManager = NewPodWatcherManager(nil)

// DefaultManager returns the package-level PodWatcherManager instance.
// This is the same manager used by the gRPC resolver registered via init().
func DefaultManager() *PodWatcherManager {
	return defaultManager
}

func watcherKey(namespace, podName string) string {
	return namespace + "/" + podName
}

type kubeResolverBuilder struct {
	manager *PodWatcherManager
}

func (b *kubeResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	endpoint := target.Endpoint()
	pt, err := parsePodTarget(endpoint)
	if err != nil {
		return nil, err
	}

	r := &kubeResolver{
		cc:        cc,
		podTarget: pt,
	}

	cancel, err := b.manager.WatchPodIP(endpoint, func(addr string, watchErr error) {
		if watchErr != nil {
			r.cc.ReportError(watchErr)
			return
		}
		r.updateState(addr)
	})
	if err != nil {
		return nil, err
	}
	r.cancel = cancel
	return r, nil
}

func (b *kubeResolverBuilder) Scheme() string {
	return scheme
}

type kubeResolver struct {
	cc        resolver.ClientConn
	podTarget podTarget
	cancel    func()
}

func (r *kubeResolver) updateState(addr string) {
	err := r.cc.UpdateState(resolver.State{
		Addresses: []resolver.Address{{Addr: addr}},
	})
	if err != nil {
		log.Warningf("failed to update state: %s", err)
	}
}

func (r *kubeResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	// Per the documentation, ResolveNow is a hint.
	// We already resolve and watch the target for changes as soon as the
	// resolver is created so there's no benefit in doing anything here.
}

func (r *kubeResolver) Close() {
	r.cancel()
}

func init() {
	resolver.Register(&kubeResolverBuilder{manager: defaultManager})
}
