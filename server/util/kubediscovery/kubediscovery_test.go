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
	ktypes "k8s.io/apimachinery/pkg/types"
	k8stesting "k8s.io/client-go/testing"
)

const testNamespace = "test_ns"

var creationTime = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func readyPod(name, ip string, ownerRefs []metav1.OwnerReference) *corev1.Pod {
	return readyPodOnNode(name, ip, name, ownerRefs)
}

func readyPodOnNode(name, ip, nodeName string, ownerRefs []metav1.OwnerReference) *corev1.Pod {
	return readyPodOnNodeAt(name, ip, nodeName, creationTime, ownerRefs)
}

func readyPodOnNodeAt(name, ip, nodeName string, createdAt time.Time, ownerRefs []metav1.OwnerReference) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         testNamespace,
			UID:               ktypes.UID("uid-" + name),
			CreationTimestamp: metav1.NewTime(createdAt),
			OwnerReferences:   ownerRefs,
			Labels:            map[string]string{"app": "cache"},
		},
		Spec: corev1.PodSpec{
			NodeName: nodeName,
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

// count returns how many peer updates have been published so far.
func (pc *peerCollector) count() int {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	return len(pc.updates)
}

// requireNoUpdate asserts that no further peer update is published, i.e. that
// the event under test left the peer set alone.
func (pc *peerCollector) requireNoUpdate(t *testing.T, d time.Duration) {
	t.Helper()
	n := pc.count()
	require.Never(t, func() bool { return pc.count() != n }, d, 10*time.Millisecond,
		"peer set changed but should have been left alone")
}

func (pc *peerCollector) mostRecentPeerMap(t *testing.T) map[string]string {
	t.Helper()
	pc.mu.Lock()
	defer pc.mu.Unlock()
	require.NotEmpty(t, pc.updates)
	return pc.updates[len(pc.updates)-1]
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

// A peer that is running but not yet passing its readiness probe must
// still be discoverable — the readiness probe can itself depend on
// peer connectivity, so gating on it would deadlock cold start.
func TestPodNotReadyIncluded(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPod("cache-0", "10.0.0.1", ownerRefs)
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:            "cache-1",
			Namespace:       testNamespace,
			OwnerReferences: ownerRefs,
			Labels:          map[string]string{"app": "cache"},
		},
		Spec: corev1.PodSpec{NodeName: "cache-1"},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.2",
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
				{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
			},
		},
	}
	rs := replicaSet("cache-rs")

	client := fake.NewClientset(pod0, pod1, rs)
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{
		"cache-0": "10.0.0.1:7999",
		"cache-1": "10.0.0.2:7999",
	}, peers)
}

// A peer that flips PodReady to false stays in the set, since we
// don't gate on readiness probes.
func TestPodStaysIncludedWhenReadyFlips(t *testing.T) {
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

	// Flip pod1 to not-ready. The peer should remain in the set.
	pod1.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.PodReady, Status: corev1.ConditionFalse},
	}
	_, err := client.CoreV1().Pods(testNamespace).Update(context.Background(), pod1, metav1.UpdateOptions{})
	require.NoError(t, err)

	// Then trigger a real change (delete pod0) and verify pod1 is
	// still present in the resulting peer map.
	err = client.CoreV1().Pods(testNamespace).Delete(context.Background(), "cache-0", metav1.DeleteOptions{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		pc.mu.Lock()
		defer pc.mu.Unlock()
		latest := pc.updates[len(pc.updates)-1]
		_, hasPod1 := latest["cache-1"]
		_, hasPod0 := latest["cache-0"]
		return hasPod1 && !hasPod0
	}, 5*time.Second, 10*time.Millisecond, "expected pod1 to remain after readiness flip and pod0 deletion")
}

func TestPodAddr(t *testing.T) {
	pw := &PeerWatcher{port: "7999"}
	tests := []struct {
		name string
		pod  corev1.Pod
		want string
	}{
		{
			name: "running with no conditions is included",
			pod: corev1.Pod{Status: corev1.PodStatus{
				PodIP: "10.0.0.1",
				Phase: corev1.PodRunning,
			}},
			want: "10.0.0.1:7999",
		},
		{
			name: "running but PodReady=False is included",
			pod: corev1.Pod{Status: corev1.PodStatus{
				PodIP: "10.0.0.1",
				Phase: corev1.PodRunning,
				Conditions: []corev1.PodCondition{
					{Type: corev1.PodReady, Status: corev1.ConditionFalse},
					{Type: corev1.ContainersReady, Status: corev1.ConditionFalse},
				},
			}},
			want: "10.0.0.1:7999",
		},
		{
			name: "no PodIP is excluded",
			pod: corev1.Pod{Status: corev1.PodStatus{
				Phase: corev1.PodRunning,
			}},
			want: "",
		},
		{
			name: "Phase=Pending is excluded even with IP",
			pod: corev1.Pod{Status: corev1.PodStatus{
				PodIP: "10.0.0.1",
				Phase: corev1.PodPending,
			}},
			want: "",
		},
		{
			name: "Phase=Succeeded is excluded",
			pod: corev1.Pod{Status: corev1.PodStatus{
				PodIP: "10.0.0.1",
				Phase: corev1.PodSucceeded,
			}},
			want: "",
		},
		{
			name: "Phase=Failed is excluded",
			pod: corev1.Pod{Status: corev1.PodStatus{
				PodIP: "10.0.0.1",
				Phase: corev1.PodFailed,
			}},
			want: "",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, pw.podAddr(tc.pod))
		})
	}
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

func TestNodeKeyForReplicaSet(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPodOnNode("cache-0", "10.0.0.1", "node-a", ownerRefs)
	pod1 := readyPodOnNode("cache-1", "10.0.0.2", "node-b", ownerRefs)
	rs := replicaSet("cache-rs")

	client := fake.NewClientset(pod0, pod1, rs)
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{
		"node-a": "10.0.0.1:7999",
		"node-b": "10.0.0.2:7999",
	}, peers)
}

func TestNodeKeyPodReplaced(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod0 := readyPodOnNode("cache-0", "10.0.0.1", "node-a", ownerRefs)
	rs := replicaSet("cache-rs")

	client := fake.NewClientset(pod0, rs)
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{"node-a": "10.0.0.1:7999"}, peers)

	waitForWatch(t, client, "pods")

	// Simulate a rolling update: old pod deleted, new pod on same node
	// with a new name and IP. The node key should stay "node-a".
	err := client.CoreV1().Pods(testNamespace).Delete(context.Background(), "cache-0", metav1.DeleteOptions{})
	require.NoError(t, err)

	peers = pc.waitForUpdate(t, 5*time.Second)
	require.Empty(t, peers)

	newPod := readyPodOnNode("cache-1", "10.0.0.99", "node-a", ownerRefs)
	_, err = client.CoreV1().Pods(testNamespace).Create(context.Background(), newPod, metav1.CreateOptions{})
	require.NoError(t, err)

	peers = pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{"node-a": "10.0.0.99:7999"}, peers)
}

func TestAppLabelSelectorString(t *testing.T) {
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
			got := appLabelSelectorString(tc.sel)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestLabelSelectorString(t *testing.T) {
	tests := []struct {
		name    string
		sel     *metav1.LabelSelector
		want    string
		wantErr bool
	}{
		{
			name:    "nil",
			sel:     nil,
			wantErr: true,
		},
		{
			name: "single label",
			sel:  &metav1.LabelSelector{MatchLabels: map[string]string{"app": "cache"}},
			want: "app=cache",
		},
		{
			name: "no app label",
			sel: &metav1.LabelSelector{MatchLabels: map[string]string{
				"app.kubernetes.io/name":     "buildbuddy-enterprise",
				"app.kubernetes.io/instance": "release",
			}},
			want: "app.kubernetes.io/instance=release,app.kubernetes.io/name=buildbuddy-enterprise",
		},
		{
			name:    "empty selector",
			sel:     &metav1.LabelSelector{},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := labelSelectorString(tc.sel)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// During a rolling restart, the replacement pod can reach Running before the
// outgoing pod is gone and both pods sit on the same node. For deployments, the node name is
// the peer key. WHhen the outgoing pod is finally deleted, it shouldn't
// delete the peer key that is now owned by the replacement pod.
func TestOutgoingPodDoesNotEvictReplacement(t *testing.T) {
	for _, tc := range []struct {
		name string
		// exit makes the outgoing pod stop being a peer.
		exit func(t *testing.T, client *fake.Clientset)
	}{
		{
			name: "outgoing pod is deleted",
			exit: func(t *testing.T, client *fake.Clientset) {
				require.NoError(t, client.CoreV1().Pods(testNamespace).Delete(
					context.Background(), "cache-0", metav1.DeleteOptions{}))
			},
		},
		{
			name: "outgoing pod is no longer Running",
			exit: func(t *testing.T, client *fake.Clientset) {
				ctx := context.Background()
				pod, err := client.CoreV1().Pods(testNamespace).Get(ctx, "cache-0", metav1.GetOptions{})
				require.NoError(t, err)
				pod.Status.Phase = corev1.PodSucceeded
				_, err = client.CoreV1().Pods(testNamespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
				require.NoError(t, err)
			},
		},
		{
			name: "outgoing pod is terminating, then deleted",
			exit: func(t *testing.T, client *fake.Clientset) {
				markTerminating(t, client, "cache-0")
				require.NoError(t, client.CoreV1().Pods(testNamespace).Delete(
					context.Background(), "cache-0", metav1.DeleteOptions{}))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ownerRefs := replicaSetOwnerRef("cache-rs")

			// node-b never changes, and acts as a control: peer set churn
			// on node-a must not disturb it.
			controlPod := readyPodOnNode("cache-1", "10.0.0.2", "node-b", ownerRefs)

			outgoingPod := readyPodOnNode("cache-0", "10.0.0.1", "node-a", ownerRefs)

			client := fake.NewClientset(outgoingPod, controlPod, replicaSet("cache-rs"))
			pc := newPeerCollector()
			testingPeerWatcher(t, client, pc)

			require.Equal(t, map[string]string{"node-a": "10.0.0.1:7999", "node-b": "10.0.0.2:7999"},
				pc.waitForUpdate(t, 5*time.Second))
			waitForWatch(t, client, "pods")

			// The replacement pod lands on the same node as outgoingPod and takes the key.
			replacement := readyPodOnNodeAt("cache-2", "10.0.0.99", "node-a", creationTime.Add(time.Hour), ownerRefs)
			_, err := client.CoreV1().Pods(testNamespace).Create(
				context.Background(), replacement, metav1.CreateOptions{})
			require.NoError(t, err)
			require.Equal(t, map[string]string{"node-a": "10.0.0.99:7999", "node-b": "10.0.0.2:7999"},
				pc.waitForUpdate(t, 5*time.Second))

			// The outgoing pod goes away. It no longer owns node-a, so
			// nothing should be published at all.
			tc.exit(t, client)
			pc.requireNoUpdate(t, 300*time.Millisecond)

			require.Equal(t, map[string]string{"node-a": "10.0.0.99:7999", "node-b": "10.0.0.2:7999"},
				pc.mostRecentPeerMap(t), "replacement should still hold the node's peer key")
		})
	}
}

// A terminating pod with no replacement keeps its peer key until its object is
// deleted, so that the ring stays stable while it drains.
func TestTerminatingPodKeepsPeerKeyUntilDeleted(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	pod := readyPodOnNode("cache-0", "10.0.0.1", "node-a", ownerRefs)
	client := fake.NewClientset(pod, replicaSet("cache-rs"))
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	peers := pc.waitForUpdate(t, 5*time.Second)
	require.Equal(t, map[string]string{"node-a": "10.0.0.1:7999"}, peers)

	waitForWatch(t, client, "pods")

	markTerminating(t, client, "cache-0")
	pc.requireNoUpdate(t, 300*time.Millisecond)

	require.NoError(t, client.CoreV1().Pods(testNamespace).Delete(
		context.Background(), "cache-0", metav1.DeleteOptions{}))
	peers = pc.waitForUpdate(t, 5*time.Second)
	require.Empty(t, peers)
}

// A draining pod keeps emitting events while it shuts down. Once its
// replacement holds the node's peer key, those events must not point the key
// back at the address that is about to stop answering.
func TestDrainingPodDoesNotReclaimPeerKey(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	outgoing := readyPodOnNode("cache-0", "10.0.0.1", "node-a", ownerRefs)
	client := fake.NewClientset(outgoing, replicaSet("cache-rs"))
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	require.Equal(t, map[string]string{"node-a": "10.0.0.1:7999"}, pc.waitForUpdate(t, 5*time.Second))
	waitForWatch(t, client, "pods")
	ctx := context.Background()

	markTerminating(t, client, "cache-0")
	replacement := readyPodOnNodeAt("cache-1", "10.0.0.99", "node-a", creationTime.Add(time.Hour), ownerRefs)
	_, err := client.CoreV1().Pods(testNamespace).Create(ctx, replacement, metav1.CreateOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"node-a": "10.0.0.99:7999"}, pc.waitForUpdate(t, 5*time.Second))

	// The outgoing pod is still Phase=Running with its IP, so its events
	// still carry a valid-looking address. It doesn't own the key any more,
	// so that address must not come back.
	pod, err := client.CoreV1().Pods(testNamespace).Get(ctx, "cache-0", metav1.GetOptions{})
	require.NoError(t, err)
	pod.Status.Message = "shutting down"
	_, err = client.CoreV1().Pods(testNamespace).UpdateStatus(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)
	pc.requireNoUpdate(t, 300*time.Millisecond)

	require.Equal(t, map[string]string{"node-a": "10.0.0.99:7999"}, pc.mostRecentPeerMap(t))
}

// If the newer pod on a node dies while an older pod is still running there
// -- i.e. during a rollback or if the replacement is evicted or unhealthy -- the node should
// stay in the peer set, pointing at the older pod that is still serving on it.
func TestOlderPodReclaimsPeerKeyWhenReplacementDies(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	old := readyPodOnNode("cache-0", "10.0.0.1", "node-a", ownerRefs)
	client := fake.NewClientset(old, replicaSet("cache-rs"))
	pc := newPeerCollector()
	testingPeerWatcher(t, client, pc)

	require.Equal(t, map[string]string{"node-a": "10.0.0.1:7999"}, pc.waitForUpdate(t, 5*time.Second))
	waitForWatch(t, client, "pods")
	ctx := context.Background()

	// A replacement pod comes up on the same node and takes the key over.
	newPod := readyPodOnNodeAt("cache-1", "10.0.0.99", "node-a", creationTime.Add(time.Hour), ownerRefs)
	_, err := client.CoreV1().Pods(testNamespace).Create(ctx, newPod, metav1.CreateOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"node-a": "10.0.0.99:7999"}, pc.waitForUpdate(t, 5*time.Second))

	// The rollout is rolled back and the new pod is deleted. The original
	// pod is still Running, so node-a must fall back to it rather than
	// disappearing from the ring.
	require.NoError(t, client.CoreV1().Pods(testNamespace).Delete(ctx, "cache-1", metav1.DeleteOptions{}))
	require.Equal(t, map[string]string{"node-a": "10.0.0.1:7999"}, pc.waitForUpdate(t, 5*time.Second))
}

func TestWatchersAgreeAfterOwnerStartsTerminating(t *testing.T) {
	ownerRefs := replicaSetOwnerRef("cache-rs")

	old := readyPodOnNode("cache-0", "10.0.0.1", "node-a", ownerRefs)
	client := fake.NewClientset(old, replicaSet("cache-rs"))
	incremental := newPeerCollector()
	testingPeerWatcher(t, client, incremental)

	require.Equal(t, map[string]string{"node-a": "10.0.0.1:7999"}, incremental.waitForUpdate(t, 5*time.Second))
	waitForWatch(t, client, "pods")

	newPod := readyPodOnNodeAt("cache-1", "10.0.0.99", "node-a", creationTime.Add(time.Hour), ownerRefs)
	_, err := client.CoreV1().Pods(testNamespace).Create(context.Background(), newPod, metav1.CreateOptions{})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"node-a": "10.0.0.99:7999"}, incremental.waitForUpdate(t, 5*time.Second))

	// The newer pod starts terminating, so the older one should own the key.
	markTerminating(t, client, "cache-1")
	require.Equal(t, map[string]string{"node-a": "10.0.0.1:7999"}, incremental.waitForUpdate(t, 5*time.Second))

	// A watcher starting now should see the same state.
	fresh := newPeerCollector()
	testingPeerWatcher(t, client, fresh)
	require.Equal(t, incremental.mostRecentPeerMap(t), fresh.waitForUpdate(t, 5*time.Second))
}

// markTerminating sets a deletion timestamp on a pod, the way the API server
// does when a pod is deleted with a grace period. The pod object stays around,
// Running and with its IP, until the kubelet finishes terminating it.
func markTerminating(t *testing.T, client *fake.Clientset, name string) {
	ctx := context.Background()
	pod, err := client.CoreV1().Pods(testNamespace).Get(ctx, name, metav1.GetOptions{})
	require.NoError(t, err)
	now := metav1.Now()
	pod.DeletionTimestamp = &now
	_, err = client.CoreV1().Pods(testNamespace).Update(ctx, pod, metav1.UpdateOptions{})
	require.NoError(t, err)
}
