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
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const apiMaxBackoff = 30 * time.Second

// PeersUpdateFn is called when the set of discovered peers changes.
// This matches the signature used by heartbeat.PeersUpdateFn.
type PeersUpdateFn func(peerSet ...string)

// Config holds configuration for Kubernetes peer discovery.
type Config struct {
	// UpdateFn is called whenever the set of discovered peers changes.
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

// Channel watches Kubernetes pods that share the same controller as this
// pod and calls UpdateFn when the peer set changes.
type Channel struct {
	client   kubernetes.Interface
	port     string
	updateFn PeersUpdateFn

	namespace string
	podName   string

	ctx    context.Context
	cancel context.CancelFunc

	mu    sync.Mutex
	peers map[string]string // pod name -> "ip:port"
}

// NewChannel creates a new Kubernetes peer discovery channel.
func NewChannel(config *Config) (*Channel, error) {
	namespace := config.Namespace
	if namespace == "" {
		data, err := os.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
		if err != nil {
			return nil, fmt.Errorf("could not determine namespace: %w", err)
		}
		namespace = strings.TrimSpace(string(data))
	}

	podName := config.PodName
	if podName == "" {
		podName = os.Getenv("HOSTNAME")
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
	return &Channel{
		client:    client,
		port:      config.Port,
		updateFn:  config.UpdateFn,
		namespace: namespace,
		podName:   podName,
		ctx:       ctx,
		cancel:    cancel,
		peers:     make(map[string]string),
	}, nil
}

// StartAdvertising begins watching for peer pods. The name matches the
// heartbeat.Channel interface for consistency.
func (c *Channel) StartAdvertising() {
	go c.discoverAndWatch()
}

// StopAdvertising stops watching for peer pods.
func (c *Channel) StopAdvertising() {
	c.cancel()
}

// discoverAndWatch runs the main discovery loop with exponential backoff.
func (c *Channel) discoverAndWatch() {
	backoff := time.Second
	for {
		err := c.runOnce()
		if c.ctx.Err() != nil {
			return
		}
		log.Warningf("kubediscovery: watch loop ended: %s; retrying in %s", err, backoff)
		select {
		case <-time.After(backoff):
			backoff = min(backoff*2, apiMaxBackoff)
		case <-c.ctx.Done():
			return
		}
	}
}

// runOnce performs a single cycle: look up our owner, then list+watch peers.
func (c *Channel) runOnce() error {
	myPod, err := c.client.CoreV1().Pods(c.namespace).Get(c.ctx, c.podName, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("failed to get own pod %s/%s: %w", c.namespace, c.podName, err)
	}

	labelSelector, err := c.getLabelSelectorFromOwner(myPod)
	if err != nil {
		return fmt.Errorf("failed to determine label selector: %w", err)
	}

	return c.listAndWatch(labelSelector)
}

// getLabelSelectorFromOwner finds the controlling owner of the pod and
// returns the label selector string that matches all pods managed by
// that owner.
func (c *Channel) getLabelSelectorFromOwner(pod *corev1.Pod) (string, error) {
	var controllerRef *metav1.OwnerReference
	for i := range pod.OwnerReferences {
		ref := &pod.OwnerReferences[i]
		if ref.Controller != nil && *ref.Controller {
			controllerRef = ref
			break
		}
	}
	if controllerRef == nil {
		return "", fmt.Errorf("pod %s has no controller owner reference", pod.Name)
	}

	switch controllerRef.Kind {
	case "ReplicaSet":
		rs, err := c.client.AppsV1().ReplicaSets(c.namespace).Get(c.ctx, controllerRef.Name, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("failed to get ReplicaSet %s: %w", controllerRef.Name, err)
		}
		return labelSelectorString(rs.Spec.Selector), nil

	case "StatefulSet":
		ss, err := c.client.AppsV1().StatefulSets(c.namespace).Get(c.ctx, controllerRef.Name, metav1.GetOptions{})
		if err != nil {
			return "", fmt.Errorf("failed to get StatefulSet %s: %w", controllerRef.Name, err)
		}
		return labelSelectorString(ss.Spec.Selector), nil

	default:
		return "", fmt.Errorf("unsupported controller kind %q for pod %s", controllerRef.Kind, pod.Name)
	}
}

// labelSelectorString converts a LabelSelector to a comma-separated
// key=value string suitable for use in ListOptions.LabelSelector.
// Only matchLabels are used; matchExpressions are not supported.
func labelSelectorString(sel *metav1.LabelSelector) string {
	if sel == nil {
		return ""
	}
	return "app=" + sel.MatchLabels["app"]
	// parts := make([]string, 0, len(sel.MatchLabels))
	// for k, v := range sel.MatchLabels {
	// 	parts = append(parts, k+"="+v)
	// }
	// sort.Strings(parts)
	// return strings.Join(parts, ",")
}

// listAndWatch lists all matching pods then watches for changes.
func (c *Channel) listAndWatch(labelSelector string) error {
	podList, err := c.client.CoreV1().Pods(c.namespace).List(c.ctx, metav1.ListOptions{
		LabelSelector: labelSelector,
	})
	if err != nil {
		return fmt.Errorf("failed to list pods: %w", err)
	}

	c.mu.Lock()
	c.peers = make(map[string]string)
	for i := range podList.Items {
		pod := &podList.Items[i]
		if addr := c.podAddr(pod); addr != "" {
			c.peers[pod.Name] = addr
		}
	}
	c.notifyLocked()
	c.mu.Unlock()

	resourceVersion := podList.ResourceVersion

	// Watch loop: restart the watch when it ends (server can close it).
	for {
		watcher, err := c.client.CoreV1().Pods(c.namespace).Watch(c.ctx, metav1.ListOptions{
			LabelSelector:   labelSelector,
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

// processEvents handles watch events until the channel closes or an
// error occurs.
func (c *Channel) processEvents(watcher watch.Interface, resourceVersion *string) error {
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
				pod, ok := event.Object.(*corev1.Pod)
				if !ok {
					continue
				}
				c.updatePod(pod)
			case watch.Deleted:
				pod, ok := event.Object.(*corev1.Pod)
				if !ok {
					continue
				}
				c.removePod(pod.Name)
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
func (c *Channel) podAddr(pod *corev1.Pod) string {
	if pod.Status.PodIP == "" {
		return ""
	}
	if pod.Status.Phase != corev1.PodRunning {
		return ""
	}
	for _, cond := range pod.Status.Conditions {
		switch cond.Type {
		case corev1.PodReady, corev1.PodInitialized, corev1.ContainersReady:
			// We can't wait for the PodReady condition alone, since that
			// relies on health checks, which can depend on having enough peers.
			if cond.Status == corev1.ConditionTrue {
				return net.JoinHostPort(pod.Status.PodIP, c.port)
			}
		}
	}
	return ""
}

func (c *Channel) updatePod(pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	addr := c.podAddr(pod)
	if addr == "" {
		if _, exists := c.peers[pod.Name]; exists {
			delete(c.peers, pod.Name)
			c.notifyLocked()
		}
		return
	}
	existing, exists := c.peers[pod.Name]
	if !exists || existing != addr {
		c.peers[pod.Name] = addr
		c.notifyLocked()
	}
}

func (c *Channel) removePod(podName string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, exists := c.peers[podName]; exists {
		delete(c.peers, podName)
		c.notifyLocked()
	}
}

func (c *Channel) notifyLocked() {
	addrs := make([]string, 0, len(c.peers))
	for _, addr := range c.peers {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)
	log.Infof("kubediscovery: peer set changed: %v", addrs)
	c.updateFn(addrs...)
}
