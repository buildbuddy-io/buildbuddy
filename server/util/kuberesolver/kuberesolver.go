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
package kuberesolver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/resolver"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

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

// podWatcher owns a single Get+Watch loop for a specific (namespace, podName)
// pair. Multiple kubeResolvers can subscribe to the same podWatcher.
type podWatcher struct {
	client    kubernetes.Interface
	namespace string
	podName   string

	// Only accessed from the resolveAndWatch goroutine.
	resourceVersion string

	mu               sync.Mutex
	subscribers      map[*kubeResolver]func(podResolution)
	cancel           context.CancelFunc
	latestResolution *podResolution
}

func newPodWatcher(ctx context.Context, client kubernetes.Interface, namespace, podName string) *podWatcher {
	ctx, cancel := context.WithCancel(ctx)
	pw := &podWatcher{
		client:      client,
		namespace:   namespace,
		podName:     podName,
		subscribers: make(map[*kubeResolver]func(podResolution)),
		cancel:      cancel,
	}
	go pw.resolveAndWatch(ctx)
	return pw
}

// subscribe adds a subscriber with callback cb. cb will be called immediately
// with the latestResolution known state (pod or error) and then whenever the pod state
// changes. cb must not block.
func (pw *podWatcher) subscribe(r *kubeResolver, cb func(podResolution)) {
	pw.mu.Lock()
	pw.subscribers[r] = cb
	if pw.latestResolution != nil {
		cb(*pw.latestResolution)
	}
	pw.mu.Unlock()
}

// unsubscribe removes a subscriber. It returns true if no subscribers remain.
func (pw *podWatcher) unsubscribe(r *kubeResolver) bool {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	delete(pw.subscribers, r)
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

	for _, cb := range callbacks {
		cb(r)
	}
}

// resolve does a one-shot Get and notifies subscribers. Returns true on success.
func (pw *podWatcher) resolve(ctx context.Context) bool {
	log.Infof("Resolving pod %s/%s", pw.namespace, pw.podName)
	pod, err := pw.client.CoreV1().Pods(pw.namespace).Get(ctx, pw.podName, metav1.GetOptions{})
	if err != nil {
		log.Warningf("Failed to get pod %s/%s from k8s: %s", pw.namespace, pw.podName, err)
		pw.notifySubscribers(podResolution{err: fmt.Errorf("failed to get pod: %w", err)})
		return false
	}
	pw.notifySubscribers(podResolution{pod: pod})
	pw.resourceVersion = pod.ResourceVersion
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
				return fmt.Errorf("watch channel closed")
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

type kubeResolverBuilder struct {
	mu       sync.Mutex
	client   kubernetes.Interface
	watchers map[string]*podWatcher
}

func watcherKey(namespace, podName string) string {
	return namespace + "/" + podName
}

func (b *kubeResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	endpoint := target.Endpoint()
	pt, err := parsePodTarget(endpoint)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if b.client == nil {
		config, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("could not create k8s client: %w", err)
		}
		b.client, err = kubernetes.NewForConfig(config)
		if err != nil {
			return nil, fmt.Errorf("could not create k8s client: %w", err)
		}
	}

	// gRPC creates one resolver per connection.
	// To avoid making redundant calls to the k8s APIs we create a single
	// watcher for each namespace/pod pair.
	key := watcherKey(pt.namespace, pt.podName)
	pw := b.watchers[key]
	if pw == nil {
		if b.watchers == nil {
			b.watchers = make(map[string]*podWatcher)
		}
		pw = newPodWatcher(context.Background(), b.client, pt.namespace, pt.podName)
		b.watchers[key] = pw
	}

	r := &kubeResolver{
		cc:        cc,
		podTarget: pt,
		watcher:   pw,
		builder:   b,
	}

	pw.subscribe(r, func(result podResolution) {
		if result.err != nil {
			r.cc.ReportError(result.err)
			return
		}
		r.updateStateFromPod(result.pod)
	})

	return r, nil
}

func (b *kubeResolverBuilder) Scheme() string {
	return scheme
}

// removeWatcher unsubscribes a resolver from its podWatcher and cleans up
// the watcher if no subscribers remain.
func (b *kubeResolverBuilder) removeWatcher(r *kubeResolver) {
	b.mu.Lock()
	defer b.mu.Unlock()
	pw := r.watcher
	if pw.unsubscribe(r) {
		pw.cancel()
		delete(b.watchers, watcherKey(pw.namespace, pw.podName))
	}
}

type kubeResolver struct {
	cc        resolver.ClientConn
	podTarget podTarget
	watcher   *podWatcher
	builder   *kubeResolverBuilder
}

func (r *kubeResolver) updateStateFromPod(pod *corev1.Pod) {
	ip := pod.Status.PodIP
	// This will happen when a pod restarts.
	if ip == "" {
		log.Infof("Pod %s/%s doesn't have an IP yet", r.podTarget.namespace, r.podTarget.podName)
		// If we previously reported an IP, reporting an error should cause
		// the balancer to close existing connections to the old IP.
		r.cc.ReportError(fmt.Errorf("pod %s/%s doesn't have an IP yet", r.podTarget.namespace, r.podTarget.podName))
		return
	}

	addr := ip
	if r.podTarget.port != "" {
		addr = ip + ":" + r.podTarget.port
	}

	log.Infof("Pod %s/%s resolved to IP %s", r.podTarget.namespace, r.podTarget.podName, ip)

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
	r.builder.removeWatcher(r)
}

func init() {
	resolver.Register(&kubeResolverBuilder{})
}
