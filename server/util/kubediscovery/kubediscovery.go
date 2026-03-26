// Package kubediscovery provides Kubernetes-based peer discovery. It watches
// all pods owned by the same controller (ReplicaSet or StatefulSet) as the
// current pod and maintains a list of peer addresses (ip:port).
//
// The pod must have RBAC permissions to get/list/watch pods and get
// replicasets/statefulsets in its namespace.
package kubediscovery

import (
	"context"
	"fmt"
	"maps"
	"net"
	"slices"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const apiMaxBackoff = 30 * time.Second

// maxPeers is a safety limit on the number of tracked peers to prevent
// unbounded memory growth from a misconfigured label selector.
const maxPeers = 10000

// PeersUpdateFn is called when the set of discovered peers changes. The map
// keys are pod names for stateful sets and node names for deployments.The
// values are "ip:port" strings. The receiver owns the map, and may modify it.
type PeersUpdateFn func(peers map[string]string)

// Config holds configuration for Kubernetes peer discovery.
type Config struct {
	// UpdateFn is called whenever the set of discovered peers changes. It
	// should be fast because it's called while holding a lock.
	UpdateFn PeersUpdateFn
	// Port is the port number peers listen on (e.g. "7999").
	Port string
	// Namespace is the K8s namespace. If empty, it is read from the
	// service account token path.
	Namespace string
	// PodName is this pod's name. If empty, HOSTNAME env var is used.
	PodName string
	// Client is an optional Kubernetes client for testing. If nil,
	// InClusterConfig is used.
	Client kubernetes.Interface
}

// PeerWatcher watches Kubernetes pods that share the same controller as this
// pod and calls UpdateFn when the peer set changes.
type PeerWatcher struct {
	client   kubernetes.Interface
	port     string
	updateFn PeersUpdateFn
	peerKey  func(pod corev1.Pod) string

	namespace     string
	podName       string
	labelSelector string

	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	peers map[string]string // peer key -> "ip:port"
}

// NewPeerWatcher creates a new Kubernetes peer discovery watcher.
func NewPeerWatcher(config *Config) (*PeerWatcher, error) {
	namespace := config.Namespace
	if namespace == "" {
		namespace = resources.GetK8sNamespace()
	}
	podName := config.PodName
	if podName == "" {
		podName = resources.GetK8sPodName()
	}
	if podName == "" {
		return nil, fmt.Errorf("could not determine pod name: set HOSTNAME env var or Config.PodName")
	}

	client := config.Client
	if client == nil {
		restConfig, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("could not create k8s in-cluster config: %w", err)
		}
		client, err = kubernetes.NewForConfig(restConfig)
		if err != nil {
			return nil, fmt.Errorf("could not create k8s client: %w", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	labelSelector, useNodeKey, err := resolveLabelSelector(ctx, client, namespace, podName)
	if err != nil {
		cancel()
		return nil, err
	}
	peerKey := func(pod corev1.Pod) string { return pod.Name }
	if useNodeKey {
		peerKey = func(pod corev1.Pod) string { return pod.Spec.NodeName }
	}
	pw := &PeerWatcher{
		client:        client,
		port:          config.Port,
		updateFn:      config.UpdateFn,
		peerKey:       peerKey,
		namespace:     namespace,
		podName:       podName,
		ctx:           ctx,
		cancel:        cancel,
		labelSelector: labelSelector,
	}

	return pw, nil
}

// SetUpdateFn replaces the peer update callback. This must be called
// before Start.
func (c *PeerWatcher) SetUpdateFn(fn PeersUpdateFn) {
	c.updateFn = fn
}

// Start begins watching for peer pods.
func (c *PeerWatcher) Start() error {
	if c.updateFn == nil {
		return fmt.Errorf("kubediscovery: UpdateFn must be set before calling Start")
	}
	go c.discoverAndWatch()
	return nil
}

// Stop stops watching for peer pods.
func (c *PeerWatcher) Stop() {
	c.cancel()
}

func (c *PeerWatcher) discoverAndWatch() {
	backoff := time.Second
	for {
		start := time.Now()
		err := c.runOnce()
		if time.Since(start) > time.Minute {
			// If we ran for a while, reset the backoff.
			backoff = time.Second
		}
		log.Infof("kubediscovery: watch loop ended: %s; retrying in %s", err, backoff)
		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, apiMaxBackoff)
		case <-c.ctx.Done():
			return
		}
	}
}

// runOnce performs a single list+watch cycle.
func (c *PeerWatcher) runOnce() error {
	podList, err := c.client.CoreV1().Pods(c.namespace).List(c.ctx, metav1.ListOptions{
		LabelSelector: c.labelSelector,
	})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	c.mu.Lock()
	c.peers = make(map[string]string)
	for _, pod := range podList.Items {
		if addr := c.podAddr(pod); addr != "" {
			c.peers[c.peerKey(pod)] = addr
		}
	}
	c.notifyLocked()
	c.mu.Unlock()

	resourceVersion := podList.ResourceVersion

	// Watch loop: restart the watch when it ends (server can close it).
	for {
		watcher, err := c.client.CoreV1().Pods(c.namespace).Watch(c.ctx, metav1.ListOptions{
			LabelSelector:   c.labelSelector,
			ResourceVersion: resourceVersion,
		})
		if err != nil {
			if errors.IsResourceExpired(err) {
				return fmt.Errorf("resource version expired, need re-list: %w", err)
			}
			return fmt.Errorf("failed to start watch: %w", err)
		}

		err = c.processEvents(watcher, &resourceVersion)
		watcher.Stop()
		if err != nil {
			return err
		}
		// Watch channel was closed by server; restart watch.
	}
}

// resolveLabelSelector fetches the pod's spec and derives the label
// selector from the controlling owner (ReplicaSet or StatefulSet).
// It also returns whether to use node names as peer keys (true for
// ReplicaSets/Deployments where pod names are ephemeral).
func resolveLabelSelector(ctx context.Context, client kubernetes.Interface, namespace, podName string) (string, bool, error) {
	myPod, err := client.CoreV1().Pods(namespace).Get(ctx, podName, metav1.GetOptions{})
	if err != nil {
		return "", false, fmt.Errorf("failed to get own pod %s/%s: %w", namespace, podName, err)
	}
	return getLabelSelectorFromOwner(ctx, client, namespace, myPod)
}

// getLabelSelectorFromOwner finds the controlling owner of the pod and
// returns the label selector string that matches all pods managed by
// that owner, plus whether to use node names as peer keys.
func getLabelSelectorFromOwner(ctx context.Context, client kubernetes.Interface, namespace string, pod *corev1.Pod) (string, bool, error) {
	i := slices.IndexFunc(pod.OwnerReferences, func(ref metav1.OwnerReference) bool {
		return ref.Controller != nil && *ref.Controller
	})
	if i < 0 {
		return "", false, fmt.Errorf("pod %s has no controller owner reference", pod.Name)
	}
	controllerRef := pod.OwnerReferences[i]

	switch controllerRef.Kind {
	case "ReplicaSet":
		rs, err := client.AppsV1().ReplicaSets(namespace).Get(ctx, controllerRef.Name, metav1.GetOptions{})
		if err != nil {
			return "", false, fmt.Errorf("failed to get ReplicaSet %s: %w", controllerRef.Name, err)
		}
		return labelSelectorString(rs.Spec.Selector), true, nil

	case "StatefulSet":
		ss, err := client.AppsV1().StatefulSets(namespace).Get(ctx, controllerRef.Name, metav1.GetOptions{})
		if err != nil {
			return "", false, fmt.Errorf("failed to get StatefulSet %s: %w", controllerRef.Name, err)
		}
		return labelSelectorString(ss.Spec.Selector), false, nil

	default:
		return "", false, fmt.Errorf("unsupported controller kind %q for pod %s", controllerRef.Kind, pod.Name)
	}
}

func labelSelectorString(sel *metav1.LabelSelector) string {
	if sel == nil {
		return ""
	}
	return "app=" + sel.MatchLabels["app"]
}

// processEvents handles watch events until the channel closes or an
// error occurs.
func (c *PeerWatcher) processEvents(watcher watch.Interface, resourceVersion *string) error {
	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case event, ok := <-watcher.ResultChan():
			if !ok {
				return nil // watch closed by server
			}
			if obj, ok := event.Object.(metav1.ObjectMetaAccessor); ok {
				if rv := obj.GetObjectMeta().GetResourceVersion(); rv != "" {
					*resourceVersion = rv
				}
			}
			switch event.Type {
			case watch.Added, watch.Modified:
				if pod, ok := event.Object.(*corev1.Pod); ok {
					c.updatePod(pod)
				}
			case watch.Deleted:
				if pod, ok := event.Object.(*corev1.Pod); ok {
					c.removePod(pod)
				}
			case watch.Bookmark:
				// no-op
			case watch.Error:
				return errors.FromObject(event.Object)
			}
		}
	}
}

// podAddr returns the "ip:port" address for a pod, or "" if the pod
// is not ready to receive traffic.
func (c *PeerWatcher) podAddr(pod corev1.Pod) string {
	if pod.Status.PodIP == "" {
		return ""
	}
	if pod.Status.Phase != corev1.PodRunning {
		return ""
	}
	for _, cond := range pod.Status.Conditions {
		// We can't wait for the PodReady condition alone, since that
		// relies on health checks, which can depend on having enough peers.
		if cond.Type == corev1.PodReady || cond.Type == corev1.ContainersReady {
			if cond.Status == corev1.ConditionTrue {
				return net.JoinHostPort(pod.Status.PodIP, c.port)
			}
		}
	}
	return ""
}

func (c *PeerWatcher) updatePod(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.peerKey(*pod)
	addr := c.podAddr(*pod)
	if addr == "" {
		if _, exists := c.peers[key]; exists {
			delete(c.peers, key)
			c.notifyLocked()
		}
		return
	}
	existing, exists := c.peers[key]
	if !exists || existing != addr {
		c.peers[key] = addr
		c.notifyLocked()
	}
	if len(c.peers) > maxPeers {
		alert.UnexpectedEvent("kubediscovery_too_many_peers", "Found %v peers, which is over the limit of %v", len(c.peers), maxPeers)
	}
}

func (c *PeerWatcher) removePod(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.peerKey(*pod)
	if _, exists := c.peers[key]; exists {
		delete(c.peers, key)
		c.notifyLocked()
	}
}

func (c *PeerWatcher) notifyLocked() {
	c.updateFn(maps.Clone(c.peers))
}
