// Package kuberesolver provides a gRPC resolver that uses the Kubernetes API
// to resolve pod FQDNs to pod IPs. It registers a "kube" scheme resolver.
//
// Usage:
//
//	import _ "github.com/buildbuddy-io/buildbuddy/server/util/kuberesolver"
//
//	conn, err := grpc.Dial("kube:///pod-name.service.namespace.svc.cluster.local:4772", ...)
package kuberesolver

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"google.golang.org/grpc/resolver"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const scheme = "kube"

var (
	clientOnce sync.Once
	k8sClient  kubernetes.Interface
	clientErr  error
)

// SetK8sClientForTesting overrides the K8s client used by the resolver.
// Must be called before any resolver is built.
func SetK8sClientForTesting(client kubernetes.Interface) {
	k8sClient = client
	// Mark the Once as done so the real client is never created.
	clientOnce.Do(func() {})
}

func getK8sClient() (kubernetes.Interface, error) {
	clientOnce.Do(func() {
		config, err := rest.InClusterConfig()
		if err != nil {
			clientErr = fmt.Errorf("failed to get in-cluster config: %w", err)
			return
		}
		k8sClient, clientErr = kubernetes.NewForConfig(config)
	})
	return k8sClient, clientErr
}

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

type k8sResolverBuilder struct{}

func (b *k8sResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	endpoint := target.Endpoint()
	pt, err := parsePodTarget(endpoint)
	if err != nil {
		return nil, err
	}

	client, err := getK8sClient()
	if err != nil {
		return nil, fmt.Errorf("could not create k8s client: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &k8sResolver{
		cc:        cc,
		client:    client,
		podTarget: pt,
		ctx:       ctx,
		cancel:    cancel,
		resolveCh: make(chan struct{}, 1),
	}

	// Do an initial resolve before returning.
	r.resolve()

	go r.watch()
	return r, nil
}

func (b *k8sResolverBuilder) Scheme() string {
	return scheme
}

type k8sResolver struct {
	cc        resolver.ClientConn
	client    kubernetes.Interface
	podTarget podTarget
	ctx       context.Context
	cancel    context.CancelFunc
	resolveCh chan struct{}
}

func (r *k8sResolver) resolve() {
	log.Infof("Resolving pod %s/%s", r.podTarget.namespace, r.podTarget.podName)
	pod, err := r.client.CoreV1().Pods(r.podTarget.namespace).Get(r.ctx, r.podTarget.podName, metav1.GetOptions{})
	if err != nil {
		log.Warningf("Failed to get pod %s/%s from k8s: %s", r.podTarget.namespace, r.podTarget.podName, err)
		r.cc.ReportError(fmt.Errorf("failed to get pod: %w", err))
		return
	}
	r.updateStateFromPod(pod)
}

func (r *k8sResolver) updateStateFromPod(pod *corev1.Pod) {
	ip := pod.Status.PodIP
	if ip == "" {
		log.Warningf("pod %s/%s has no IP yet", r.podTarget.namespace, r.podTarget.podName)
		return
	}

	addr := ip
	if r.podTarget.port != "" {
		addr = ip + ":" + r.podTarget.port
	}

	log.Infof("pod %s/%s resolved to IP %s", r.podTarget.namespace, r.podTarget.podName, ip)

	err := r.cc.UpdateState(resolver.State{
		Addresses: []resolver.Address{{Addr: addr}},
	})
	if err != nil {
		log.Warningf("failed to update state: %s", err)
	}
}

func (r *k8sResolver) watch() {
	backoff := time.Second
	maxBackoff := 30 * time.Second

	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		watcher, err := r.client.CoreV1().Pods(r.podTarget.namespace).Watch(r.ctx, metav1.ListOptions{
			FieldSelector: "metadata.name=" + r.podTarget.podName,
		})
		if err != nil {
			log.Warningf("failed to watch pod %s/%s: %s", r.podTarget.namespace, r.podTarget.podName, err)
			select {
			case <-time.After(backoff):
				backoff = min(backoff*2, maxBackoff)
			case <-r.ctx.Done():
				return
			}
			continue
		}

		backoff = time.Second
		r.processWatchEvents(watcher)
		watcher.Stop()
	}
}

func (r *k8sResolver) processWatchEvents(watcher watch.Interface) {
	for {
		select {
		case <-r.ctx.Done():
			return
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				pod, ok := event.Object.(*corev1.Pod)
				if !ok {
					continue
				}
				r.updateStateFromPod(pod)
			case watch.Deleted:
				log.Warningf("pod %s/%s was deleted", r.podTarget.namespace, r.podTarget.podName)
				r.cc.ReportError(fmt.Errorf("pod %s/%s was deleted", r.podTarget.namespace, r.podTarget.podName))
			case watch.Bookmark:
				// No-op: bookmark events are informational.
			case watch.Error:
				log.Warningf("watch error for pod %s/%s", r.podTarget.namespace, r.podTarget.podName)
				return
			}
		case <-r.resolveCh:
			r.resolve()
		}
	}
}

func (r *k8sResolver) ResolveNow(_ resolver.ResolveNowOptions) {
	select {
	case r.resolveCh <- struct{}{}:
	default:
	}
}

func (r *k8sResolver) Close() {
	r.cancel()
}

func init() {
	resolver.Register(&k8sResolverBuilder{})
}
