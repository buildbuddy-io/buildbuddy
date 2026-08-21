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
	ktypes "k8s.io/apimachinery/pkg/types"
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

	mu sync.Mutex

	// pods holds every pod where phase=Running, keyed by pod UID.
	// Keeping track of all of them lets us fallback to a Running pod on the same
	// node if the pod that owns the peer key stops running (for example
	// during a rolling restart).
	pods map[ktypes.UID]podInfo
	// peers is a map from peer key -> "ip:port".
	// It is recomputed from pods after every change.
	peers map[string]string
}

type podInfo struct {
	uid         ktypes.UID
	key         string // ring key: node name, or pod name for StatefulSets
	addr        string // "ip:port"
	name        string // pod name, used only as a tiebreaker
	createdAt   time.Time
	terminating bool // pod has a deletion timestamp
}

// preferredOver reports whether e should take the peer key from other.
// A pod that isn't shutting down always wins; otherwise the newest pod wins.
// Ties break on pod name so every watcher in the cluster picks the same owner.
func (e podInfo) preferredOver(other podInfo) bool {
	if e.terminating != other.terminating {
		return !e.terminating
	}
	if !e.createdAt.Equal(other.createdAt) {
		return e.createdAt.After(other.createdAt)
	}
	return e.name > other.name
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
	c.pods = make(map[ktypes.UID]podInfo, len(podList.Items))
	for _, pod := range podList.Items {
		if e := c.getPodInfo(&pod); e != nil {
			c.pods[pod.UID] = *e
		}
	}
	c.rebuildPeerMapAndNotifyLocked()
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
		return appLabelSelectorString(rs.Spec.Selector), true, nil

	case "StatefulSet":
		ss, err := client.AppsV1().StatefulSets(namespace).Get(ctx, controllerRef.Name, metav1.GetOptions{})
		if err != nil {
			return "", false, fmt.Errorf("failed to get StatefulSet %s: %w", controllerRef.Name, err)
		}
		sel, err := labelSelectorString(ss.Spec.Selector)
		if err != nil {
			return "", false, fmt.Errorf("StatefulSet %s: %w", controllerRef.Name, err)
		}
		return sel, false, nil

	default:
		return "", false, fmt.Errorf("unsupported controller kind %q for pod %s", controllerRef.Kind, pod.Name)
	}
}

// appLabelSelectorString returns an "app"-label selector for ReplicaSet-managed
// pods. Only the app label is used because the full ReplicaSet selector
// includes pod-template-hash, which is unique to each Deployment revision and
// would hide peers from other revisions during a rolling update.
func appLabelSelectorString(sel *metav1.LabelSelector) string {
	if sel == nil {
		return ""
	}
	return "app=" + sel.MatchLabels["app"]
}

// labelSelectorString converts a StatefulSet's pod selector into a list/watch
// selector string for discovering peer pods.
func labelSelectorString(sel *metav1.LabelSelector) (string, error) {
	if sel == nil {
		return "", fmt.Errorf("controller has no pod selector")
	}
	s, err := metav1.LabelSelectorAsSelector(sel)
	if err != nil {
		return "", fmt.Errorf("invalid controller pod selector: %w", err)
	}
	if s.Empty() {
		return "", fmt.Errorf("controller pod selector is empty")
	}
	return s.String(), nil
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
// is not running. We deliberately do not gate on PodReady or
// ContainersReady, since those wait for readiness probes — and the
// readiness probe can itself depend on having enough peers, leading
// to a deadlock during cold start.
func (c *PeerWatcher) podAddr(pod corev1.Pod) string {
	if pod.Status.PodIP == "" || pod.Status.Phase != corev1.PodRunning {
		return ""
	}
	return net.JoinHostPort(pod.Status.PodIP, c.port)
}

// getPodInfo returns the podInfo for pod, or nil if the pod has no usable
// address and so can't be a candidate for any peer key.
func (c *PeerWatcher) getPodInfo(pod *corev1.Pod) *podInfo {
	addr := c.podAddr(*pod)
	if addr == "" {
		// If the pod doesn't have a usable address,
		// it can't be a candidate for a peer key.
		return nil
	}
	return &podInfo{
		uid:       pod.UID,
		key:       c.peerKey(*pod),
		addr:      addr,
		name:      pod.Name,
		createdAt: pod.CreationTimestamp.Time,
		// A terminating pod stays a candidate, so that its key keeps
		// pointing somewhere while it drains and nothing has replaced it.
		// That keeps the hash ring stable: a read to a peer that has
		// stopped serving fails over to the next replica, whereas
		// dropping the peer reshuffles the ring for every key.
		terminating: pod.DeletionTimestamp != nil,
	}
}

func (c *PeerWatcher) updatePod(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e := c.getPodInfo(pod)
	if e == nil {
		// This pod no longer has an address. Remove it from the pod list
		// and rebuild the peer map.
		if _, exists := c.pods[pod.UID]; exists {
			delete(c.pods, pod.UID)
			c.rebuildPeerMapAndNotifyLocked()
		}
		return
	}
	// Kubernetes emits lots of no-op status updates. Exit early in that case.
	// Addr and terminating are the only fields that might change, and would require a rebuild of the peer map.
	if existing, exists := c.pods[pod.UID]; exists &&
		existing.addr == e.addr && existing.terminating == e.terminating {
		return
	}
	c.pods[pod.UID] = *e
	c.rebuildPeerMapAndNotifyLocked()
	if len(c.pods) > maxPeers {
		alert.UnexpectedEvent("kubediscovery_too_many_peers", "Found %v peers, which is over the limit of %v", len(c.pods), maxPeers)
	}
}

func (c *PeerWatcher) removePod(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.pods[pod.UID]; exists {
		delete(c.pods, pod.UID)
		c.rebuildPeerMapAndNotifyLocked()
	}
}

// rebuildPeerMapAndNotifyLocked recomputes the peer key -> address map by electing an
// owner for each peer key, and publishes it if the addresses changed.
func (c *PeerWatcher) rebuildPeerMapAndNotifyLocked() {
	owners := make(map[string]podInfo, len(c.pods))
	for _, e := range c.pods {
		if owner, ok := owners[e.key]; ok && !e.preferredOver(owner) {
			continue
		}
		owners[e.key] = e
	}
	peers := make(map[string]string, len(owners))
	for key, e := range owners {
		peers[key] = e.addr
	}
	// Ownership can change without the address changing (e.g. the owner
	// starts terminating). Don't rebuild the hash ring for that.
	if maps.Equal(peers, c.peers) {
		return
	}
	c.peers = peers
	c.notifyLocked()
}

func (c *PeerWatcher) notifyLocked() {
	c.updateFn(maps.Clone(c.peers))
}
