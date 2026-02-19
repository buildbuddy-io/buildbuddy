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

type kubeResolverBuilder struct {
	mu     sync.Mutex
	client kubernetes.Interface
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

	ctx, cancel := context.WithCancel(context.Background())
	r := &kubeResolver{
		cc:        cc,
		client:    b.client,
		podTarget: pt,
		ctx:       ctx,
		cancel:    cancel,
		resolveCh: make(chan struct{}, 1),
	}

	go r.watch()
	return r, nil
}

func (b *kubeResolverBuilder) Scheme() string {
	return scheme
}

type kubeResolver struct {
	cc        resolver.ClientConn
	client    kubernetes.Interface
	podTarget podTarget
	ctx       context.Context
	cancel    context.CancelFunc
	resolveCh chan struct{}

	// resourceVersion is the last known resource version from a Get or Watch
	// event. It is used to resume watches without missing events.
	// Only accessed from the watch goroutine, so no mutex is needed.
	resourceVersion string
}

func (r *kubeResolver) resolve() {
	log.Infof("Resolving pod %s/%s", r.podTarget.namespace, r.podTarget.podName)
	pod, err := r.client.CoreV1().Pods(r.podTarget.namespace).Get(r.ctx, r.podTarget.podName, metav1.GetOptions{})
	if err != nil {
		log.Warningf("Failed to get pod %s/%s from k8s: %s", r.podTarget.namespace, r.podTarget.podName, err)
		r.cc.ReportError(fmt.Errorf("failed to get pod: %w", err))
		return
	}
	r.updateStateFromPod(pod)
	r.resourceVersion = pod.ResourceVersion
}

func (r *kubeResolver) updateStateFromPod(pod *corev1.Pod) {
	ip := pod.Status.PodIP
	if ip == "" {
		log.Infof("Pod %s/%s doesn't have an IP yet", r.podTarget.namespace, r.podTarget.podName)
		// Setting an empty address list will return an error, but it should
		// still cause the balancer to close any existing connections.
		_ = r.cc.UpdateState(resolver.State{Addresses: nil})
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

func (r *kubeResolver) watch() {
	// Do an initial resolve before subscribing to async updates.
	r.resolve()

	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		log.Infof("Watching pod %s/%s for updates.", r.podTarget.namespace, r.podTarget.podName)
		watcher, err := r.client.CoreV1().Pods(r.podTarget.namespace).Watch(r.ctx, metav1.ListOptions{
			FieldSelector:   "metadata.name=" + r.podTarget.podName,
			ResourceVersion: r.resourceVersion,
		})
		if err != nil {
			log.Warningf("Failed to watch pod %s/%s: %s", r.podTarget.namespace, r.podTarget.podName, err)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			case <-r.ctx.Done():
				return
			}
			continue
		}

		if err := r.processWatchEvents(watcher); err != nil {
			watcher.Stop()
			log.Warningf("Watch for pod %s/%s ended abruptly: %s", r.podTarget.namespace, r.podTarget.podName, err)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			case <-r.ctx.Done():
				return
			}
			continue
		}

		backoff = time.Second
		watcher.Stop()
	}
}

// processWatchEvents processes events received from the watch stream.
// A non-nil error is returned when the watch ends abruptly.
func (r *kubeResolver) processWatchEvents(watcher watch.Interface) error {
	for {
		select {
		case <-r.ctx.Done():
			return nil
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return fmt.Errorf("watch channel closed")
			}
			if obj, ok := event.Object.(metav1.ObjectMetaAccessor); ok {
				if rv := obj.GetObjectMeta().GetResourceVersion(); rv != "" {
					r.resourceVersion = rv
				}
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				pod, ok := event.Object.(*corev1.Pod)
				if !ok {
					continue
				}
				r.updateStateFromPod(pod)
			case watch.Deleted:
				log.Warningf("Pod %s/%s was deleted", r.podTarget.namespace, r.podTarget.podName)
				r.cc.ReportError(fmt.Errorf("pod %s/%s was deleted", r.podTarget.namespace, r.podTarget.podName))
			case watch.Bookmark:
				// No-op: bookmark events are informational.
			case watch.Error:
				err := errors.FromObject(event.Object)
				log.Warningf("Watch error for pod %s/%s: %s", r.podTarget.namespace, r.podTarget.podName, err)
				return fmt.Errorf("watch error: %w", err)
			}
		case <-r.resolveCh:
			r.resolve()
		}
	}
}

func (r *kubeResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	select {
	case r.resolveCh <- struct{}{}:
	default:
	}
}

func (r *kubeResolver) Close() {
	r.cancel()
}

func init() {
	resolver.Register(&kubeResolverBuilder{})
}
