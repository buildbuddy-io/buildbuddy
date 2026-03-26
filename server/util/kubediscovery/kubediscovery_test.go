package kubediscovery

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"
)

const testNamespace = "test_ns"

func readyPod(name, ip string, ownerRefs []metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       testNamespace,
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

func replicaSet(name string) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "cache"},
			},
		},
	}
}

func statefulSet(name string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: testNamespace,
		},
		Spec: appsv1.StatefulSetSpec{
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"app": "cache"},
			},
		},
	}
}

// peerCollector collects peer updates from the PeerWatcher.
type peerCollector struct {
	mu      sync.Mutex
	updates []map[string]string
	ch      chan struct{}
}

func newPeerCollector() *peerCollector {
	return &peerCollector{ch: make(chan struct{}, 100)}
}

func (pc *peerCollector) updateFn(peers map[string]string) {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.updates = append(pc.updates, peers)
	select {
	case pc.ch <- struct{}{}:
	default:
	}
}

func (pc *peerCollector) waitForUpdate(t *testing.T, timeout time.Duration) map[string]string {
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

// waitForWatch waits until the fake client has received a watch action on pods.
func waitForWatch(t *testing.T, client *fake.Clientset, resource string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return slices.ContainsFunc(client.Actions(), func(a k8stesting.Action) bool {
			return a.GetVerb() == "watch" && a.GetResource().Resource == resource
		})
	}, 5*time.Second, 10*time.Millisecond, "timed out waiting for %s watch", resource)
}

func testingPeerWatcher(t *testing.T, client kubernetes.Interface, pc *peerCollector) *PeerWatcher {
	t.Helper()
	pw, err := NewPeerWatcher(&Config{
		UpdateFn:  pc.updateFn,
		Port:      "7999",
		Namespace: testNamespace,
		PodName:   "cache-0",
		Client:    client,
	})
	require.NoError(t, err)
	require.NoError(t, pw.Start())
	t.Cleanup(pw.Stop)
	return pw
}

func TestDiscoverPeersFromReplicaSet(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", "10.0.0.1", ownerRefs)
	pod1 := readyPod("cache-1", "10.0.0.2", ownerRefs)
	pod2 := readyPod("cache-2", "10.0.0.3", ownerRefs)
	rs := replicaSet("cache-rs")

	client := fake.NewClientset(pod0, pod1, pod2, rs)
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{"cache-0": "10.0.0.1:7999", "cache-1": "10.0.0.2:7999", "cache-2": "10.0.0.3:7999"}, peers)
}

func TestDiscoverPeersFromStatefulSet(t *testing.T) {
	ownerRefs := statefulSetOwnerRef("cache-ss")

	pod0 := readyPod("cache-0", "10.0.0.1", ownerRefs)
	pod1 := readyPod("cache-1", "10.0.0.2", ownerRefs)
	ss := statefulSet("cache-ss")

	client := fake.NewClientset(pod0, pod1, ss)
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{"cache-0": "10.0.0.1:7999", "cache-1": "10.0.0.2:7999"}, peers)
}

func TestPodAddedDuringWatch(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", "10.0.0.1", ownerRefs)
	pod1 := readyPod("cache-1", "10.0.0.2", ownerRefs)
	rs := replicaSet("cache-rs")

	client := fake.NewClientset(pod0, pod1, rs)
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	// Wait for initial peer set.
	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Len(t, peers, 2)

	// Wait for watch to be established.
	waitForWatch(t, client, "pods")

	// Add a new pod.
	pod2 := readyPod("cache-2", "10.0.0.3", ownerRefs)
	_, err := client.CoreV1().Pods(testNamespace).Create(context.Background(), pod2, metav1.CreateOptions{})
	require.NoError(t, err)

	peers = pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{"cache-0": "10.0.0.1:7999", "cache-1": "10.0.0.2:7999", "cache-2": "10.0.0.3:7999"}, peers)
}

func TestPodDeletedDuringWatch(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", "10.0.0.1", ownerRefs)
	pod1 := readyPod("cache-1", "10.0.0.2", ownerRefs)
	pod2 := readyPod("cache-2", "10.0.0.3", ownerRefs)
	rs := replicaSet("cache-rs")

	client := fake.NewClientset(pod0, pod1, pod2, rs)
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	// Wait for initial peer set.
	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Len(t, peers, 3)

	// Wait for watch to be established.
	waitForWatch(t, client, "pods")

	// Delete a pod.
	err := client.CoreV1().Pods(testNamespace).Delete(context.Background(), "cache-2", metav1.DeleteOptions{})
	require.NoError(t, err)

	peers = pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{"cache-0": "10.0.0.1:7999", "cache-1": "10.0.0.2:7999"}, peers)
}

func TestPodNotReadyExcluded(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", "10.0.0.1", ownerRefs)
	// pod1 is not ready
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cache-1",
			Namespace:       testNamespace,
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
	rs := replicaSet("cache-rs")

	client := fake.NewClientset(pod0, pod1, rs)
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{"cache-0": "10.0.0.1:7999"}, peers)
}

func TestPodBecomesUnready(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", "10.0.0.1", ownerRefs)
	pod1 := readyPod("cache-1", "10.0.0.2", ownerRefs)
	rs := replicaSet("cache-rs")

	client := fake.NewClientset(pod0, pod1, rs)
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	// Wait for initial peer set.
	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Len(t, peers, 2)

	// Wait for watch to be established.
	waitForWatch(t, client, "pods")

	// Make pod1 unready.
	pod1.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}
	_, err := client.CoreV1().Pods(testNamespace).Update(context.Background(), pod1, metav1.UpdateOptions{})
	require.NoError(t, err)

	peers = pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{"cache-0": "10.0.0.1:7999"}, peers)
}

func TestNoPodIPExcluded(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", "10.0.0.1", ownerRefs)
	// pod1 has no IP yet
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cache-1",
			Namespace:       testNamespace,
			OwnerReferences: ownerRefs,
			Labels:          map[string]string{"app": "cache"},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
		},
	}
	rs := replicaSet("cache-rs")

	client := fake.NewClientset(pod0, pod1, rs)
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{"cache-0": "10.0.0.1:7999"}, peers)
}

func TestWatchRecoveryFromResourceExpired(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", "10.0.0.1", ownerRefs)
	rs := replicaSet("cache-rs")

	client := fake.NewClientset(pod0, rs)

	// Intercept the first watch to return a 410 Gone error.
	var watchCount atomic.Int32
	client.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
		if watchCount.Add(1) == 1 {
			// Use RaceFreeFake to avoid a race between Error()
			// (in the goroutine) and Stop() (called by listAndWatch).
			fw := watch.NewRaceFreeFake()
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
	testingPeerWatcher(t, client, pc)

	// Should eventually get peer after recovery.
	peers := pc.waitForUpdate(t, 10*time.Second)
	require.Equal(t, map[string]string{"cache-0": "10.0.0.1:7999"}, peers)
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
			name: "only app label used",
			sel: &metav1.LabelSelector{MatchLabels: map[string]string{
				"app":               "cache",
				"pod-template-hash": "abc123",
			}},
			want: "app=cache",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := labelSelectorString(tc.sel)
			require.Equal(t, tc.want, got)
		})
	}
}
