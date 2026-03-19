package kubediscovery

import (
	"context"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"
)

func new(b bool) *bool { return &b }

func readyPod(name, namespace, ip string, ownerRefs []metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       namespace,
			OwnerReferences: ownerRefs,
			Labels:          map[string]string{"app": "cache"},
		},
		Status: corev1.PodStatus{
			PodIP: ip,
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
		},
	}
}

func replicaSetOwnerRef(name string) []metav1.OwnerReference {
	return []metav1.OwnerReference{
		{
			APIVersion: "apps/v1",
			Kind:       "ReplicaSet",
			Name:       name,
			Controller: new(true),
		},
	}
}

func statefulSetOwnerRef(name string) []metav1.OwnerReference {
	return []metav1.OwnerReference{
		{
			APIVersion: "apps/v1",
			Kind:       "StatefulSet",
			Name:       name,
			Controller: new(true),
		},
	}
}

func replicaSet(name, namespace string) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "cache"},
			},
		},
	}
}

func statefulSet(name, namespace string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "cache"},
			},
		},
	}
}

// peerCollector collects peer updates from the Channel.
type peerCollector struct {
	mu      sync.Mutex
	updates [][]string
	ch      chan struct{}
}

func newPeerCollector() *peerCollector {
	return &peerCollector{ch: make(chan struct{}, 100)}
}

func (pc *peerCollector) updateFn(peers ...string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	cp := make([]string, len(peers))
	copy(cp, peers)
	pc.updates = append(pc.updates, cp)
	select {
	case pc.ch <- struct{}{}:
	default:
	}
}

func (pc *peerCollector) waitForUpdate(t *testing.T, timeout time.Duration) []string {
	t.Helper()
	select {
	case <-pc.ch:
		pc.mu.Lock()
		defer pc.mu.Unlock()
		return pc.updates[len(pc.updates)-1]
	case <-time.After(timeout):
		t.Fatal("timed out waiting for peer update")
		return nil
	}
}

func (pc *peerCollector) latestPeers() []string {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	if len(pc.updates) == 0 {
		return nil
	}
	return pc.updates[len(pc.updates)-1]
}

// waitForWatch waits until the fake client has received a watch action on pods.
func waitForWatch(t *testing.T, client *fake.Clientset, resource string) {
	t.Helper()
	require.Eventually(t, func() bool {
		for _, a := range client.Actions() {
			if a.GetVerb() == "watch" && a.GetResource().Resource == resource {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "timed out waiting for %s watch", resource)
}

func TestDiscoverPeersFromReplicaSet(t *testing.T) {
	ns := "test-ns"
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", ns, "10.0.0.1", ownerRefs)
	pod1 := readyPod("cache-1", ns, "10.0.0.2", ownerRefs)
	pod2 := readyPod("cache-2", ns, "10.0.0.3", ownerRefs)
	rs := replicaSet("cache-rs", ns)

	client := fake.NewClientset(pod0, pod1, pod2, rs)
	pc := newPeerCollector()

	ch, err := NewChannel(&Config{
		UpdateFn:  pc.updateFn,
		Port:      "7999",
		Namespace: ns,
		PodName:   "cache-0",
		Client:    client,
	})
	require.NoError(t, err)
	ch.StartAdvertising()
	defer ch.StopAdvertising()

	peers := pc.waitForUpdate(t, 5*time.Second)
	sort.Strings(peers)
	require.Equal(t, []string{"10.0.0.1:7999", "10.0.0.2:7999", "10.0.0.3:7999"}, peers)
}

func TestDiscoverPeersFromStatefulSet(t *testing.T) {
	ns := "test-ns"
	ownerRefs := statefulSetOwnerRef("cache-ss")

	pod0 := readyPod("cache-0", ns, "10.0.0.1", ownerRefs)
	pod1 := readyPod("cache-1", ns, "10.0.0.2", ownerRefs)
	ss := statefulSet("cache-ss", ns)

	client := fake.NewClientset(pod0, pod1, ss)
	pc := newPeerCollector()

	ch, err := NewChannel(&Config{
		UpdateFn:  pc.updateFn,
		Port:      "7999",
		Namespace: ns,
		PodName:   "cache-0",
		Client:    client,
	})
	require.NoError(t, err)
	ch.StartAdvertising()
	defer ch.StopAdvertising()

	peers := pc.waitForUpdate(t, 5*time.Second)
	sort.Strings(peers)
	require.Equal(t, []string{"10.0.0.1:7999", "10.0.0.2:7999"}, peers)
}

func TestPodAddedDuringWatch(t *testing.T) {
	ns := "test-ns"
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", ns, "10.0.0.1", ownerRefs)
	pod1 := readyPod("cache-1", ns, "10.0.0.2", ownerRefs)
	rs := replicaSet("cache-rs", ns)

	client := fake.NewClientset(pod0, pod1, rs)
	pc := newPeerCollector()

	ch, err := NewChannel(&Config{
		UpdateFn:  pc.updateFn,
		Port:      "7999",
		Namespace: ns,
		PodName:   "cache-0",
		Client:    client,
	})
	require.NoError(t, err)
	ch.StartAdvertising()
	defer ch.StopAdvertising()

	// Wait for initial peer set.
	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Len(t, peers, 2)

	// Wait for watch to be established.
	waitForWatch(t, client, "pods")

	// Add a new pod.
	pod2 := readyPod("cache-2", ns, "10.0.0.3", ownerRefs)
	_, err = client.CoreV1().Pods(ns).Create(context.Background(), pod2, metav1.CreateOptions{})
	require.NoError(t, err)

	peers = pc.waitForUpdate(t, 5*time.Second)
	sort.Strings(peers)
	require.Equal(t, []string{"10.0.0.1:7999", "10.0.0.2:7999", "10.0.0.3:7999"}, peers)
}

func TestPodDeletedDuringWatch(t *testing.T) {
	ns := "test-ns"
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", ns, "10.0.0.1", ownerRefs)
	pod1 := readyPod("cache-1", ns, "10.0.0.2", ownerRefs)
	pod2 := readyPod("cache-2", ns, "10.0.0.3", ownerRefs)
	rs := replicaSet("cache-rs", ns)

	client := fake.NewClientset(pod0, pod1, pod2, rs)
	pc := newPeerCollector()

	ch, err := NewChannel(&Config{
		UpdateFn:  pc.updateFn,
		Port:      "7999",
		Namespace: ns,
		PodName:   "cache-0",
		Client:    client,
	})
	require.NoError(t, err)
	ch.StartAdvertising()
	defer ch.StopAdvertising()

	// Wait for initial peer set.
	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Len(t, peers, 3)

	// Wait for watch to be established.
	waitForWatch(t, client, "pods")

	// Delete a pod.
	err = client.CoreV1().Pods(ns).Delete(context.Background(), "cache-2", metav1.DeleteOptions{})
	require.NoError(t, err)

	peers = pc.waitForUpdate(t, 5*time.Second)
	sort.Strings(peers)
	require.Equal(t, []string{"10.0.0.1:7999", "10.0.0.2:7999"}, peers)
}

func TestPodNotReadyExcluded(t *testing.T) {
	ns := "test-ns"
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", ns, "10.0.0.1", ownerRefs)
	// pod1 is not ready
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cache-1",
			Namespace:       ns,
			OwnerReferences: ownerRefs,
			Labels:          map[string]string{"app": "cache"},
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.2",
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
		},
	}
	rs := replicaSet("cache-rs", ns)

	client := fake.NewClientset(pod0, pod1, rs)
	pc := newPeerCollector()

	ch, err := NewChannel(&Config{
		UpdateFn:  pc.updateFn,
		Port:      "7999",
		Namespace: ns,
		PodName:   "cache-0",
		Client:    client,
	})
	require.NoError(t, err)
	ch.StartAdvertising()
	defer ch.StopAdvertising()

	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, []string{"10.0.0.1:7999"}, peers)
}

func TestPodBecomesUnready(t *testing.T) {
	ns := "test-ns"
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", ns, "10.0.0.1", ownerRefs)
	pod1 := readyPod("cache-1", ns, "10.0.0.2", ownerRefs)
	rs := replicaSet("cache-rs", ns)

	client := fake.NewClientset(pod0, pod1, rs)
	pc := newPeerCollector()

	ch, err := NewChannel(&Config{
		UpdateFn:  pc.updateFn,
		Port:      "7999",
		Namespace: ns,
		PodName:   "cache-0",
		Client:    client,
	})
	require.NoError(t, err)
	ch.StartAdvertising()
	defer ch.StopAdvertising()

	// Wait for initial peer set.
	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Len(t, peers, 2)

	// Wait for watch to be established.
	waitForWatch(t, client, "pods")

	// Make pod1 unready.
	pod1.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}
	_, err = client.CoreV1().Pods(ns).Update(context.Background(), pod1, metav1.UpdateOptions{})
	require.NoError(t, err)

	peers = pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, []string{"10.0.0.1:7999"}, peers)
}

func TestNoPodIPExcluded(t *testing.T) {
	ns := "test-ns"
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", ns, "10.0.0.1", ownerRefs)
	// pod1 has no IP yet
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cache-1",
			Namespace:       ns,
			OwnerReferences: ownerRefs,
			Labels:          map[string]string{"app": "cache"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}
	rs := replicaSet("cache-rs", ns)

	client := fake.NewClientset(pod0, pod1, rs)
	pc := newPeerCollector()

	ch, err := NewChannel(&Config{
		UpdateFn:  pc.updateFn,
		Port:      "7999",
		Namespace: ns,
		PodName:   "cache-0",
		Client:    client,
	})
	require.NoError(t, err)
	ch.StartAdvertising()
	defer ch.StopAdvertising()

	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, []string{"10.0.0.1:7999"}, peers)
}

func TestWatchRecoveryFromResourceExpired(t *testing.T) {
	ns := "test-ns"
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", ns, "10.0.0.1", ownerRefs)
	rs := replicaSet("cache-rs", ns)

	client := fake.NewClientset(pod0, rs)

	// Intercept the first watch to return a 410 Gone error.
	var watchCalls sync.Map
	var watchCount int32
	client.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
		n := int32(0)
		watchCalls.Range(func(_, _ any) bool { n++; return true })
		key := action.GetVerb()
		watchCalls.Store(key+string(rune(n)), true)

		mu := sync.Mutex{}
		mu.Lock()
		watchCount++
		count := watchCount
		mu.Unlock()

		if count == 1 {
			fw := watch.NewFake()
			go func() {
				fw.Error(&metav1.Status{
					Status:  metav1.StatusFailure,
					Code:    410,
					Reason:  metav1.StatusReasonExpired,
					Message: "too old resource version",
				})
			}()
			return true, fw, nil
		}
		return false, nil, nil
	})

	pc := newPeerCollector()

	ch, err := NewChannel(&Config{
		UpdateFn:  pc.updateFn,
		Port:      "7999",
		Namespace: ns,
		PodName:   "cache-0",
		Client:    client,
	})
	require.NoError(t, err)
	ch.StartAdvertising()
	defer ch.StopAdvertising()

	// Should eventually get peer after recovery.
	peers := pc.waitForUpdate(t, 10*time.Second)
	require.Equal(t, []string{"10.0.0.1:7999"}, peers)
}

func TestLabelSelectorString(t *testing.T) {
	tests := []struct {
		name string
		sel  *metav1.LabelSelector
		want string
	}{
		{
			name: "nil",
			sel:  nil,
			want: "",
		},
		{
			name: "single label",
			sel:  &metav1.LabelSelector{MatchLabels: map[string]string{"app": "cache"}},
			want: "app=cache",
		},
		{
			name: "multiple labels sorted",
			sel: &metav1.LabelSelector{MatchLabels: map[string]string{
				"app":     "cache",
				"version": "v1",
			}},
			want: "app=cache,version=v1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := labelSelectorString(tc.sel)
			require.Equal(t, tc.want, got)
		})
	}
}
