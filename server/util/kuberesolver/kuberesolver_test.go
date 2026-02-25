package kuberesolver

import (
	"context"
	"net/url"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"
)

func TestParsePodTarget(t *testing.T) {
	tests := []struct {
		name      string
		endpoint  string
		want      podTarget
		wantError bool
	}{
		{
			name:     "full FQDN with port",
			endpoint: "metadata-server-0.headless.metadata-server-dev.svc.cluster.local:4772",
			want: podTarget{
				podName:     "metadata-server-0",
				serviceName: "headless",
				namespace:   "metadata-server-dev",
				port:        "4772",
			},
		},
		{
			name:     "full FQDN without port",
			endpoint: "metadata-server-0.headless.metadata-server-dev.svc.cluster.local",
			want: podTarget{
				podName:     "metadata-server-0",
				serviceName: "headless",
				namespace:   "metadata-server-dev",
				port:        "",
			},
		},
		{
			name:     "short FQDN with port",
			endpoint: "pod-0.svc.ns:8080",
			want: podTarget{
				podName:     "pod-0",
				serviceName: "svc",
				namespace:   "ns",
				port:        "8080",
			},
		},
		{
			name:     "short FQDN without port",
			endpoint: "pod-0.svc.ns",
			want: podTarget{
				podName:     "pod-0",
				serviceName: "svc",
				namespace:   "ns",
				port:        "",
			},
		},
		{
			name:      "too few parts",
			endpoint:  "pod-0.svc",
			wantError: true,
		},
		{
			name:      "single name",
			endpoint:  "pod-0",
			wantError: true,
		},
		{
			name:      "empty string",
			endpoint:  "",
			wantError: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parsePodTarget(tc.endpoint)
			if tc.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// fakeClientConn implements resolver.ClientConn for testing.
type fakeClientConn struct {
	states chan resolver.State
	errors chan error
}

func newFakeClientConn() *fakeClientConn {
	return &fakeClientConn{
		states: make(chan resolver.State, 10),
		errors: make(chan error, 10),
	}
}

func (f *fakeClientConn) UpdateState(state resolver.State) error {
	f.states <- state
	return nil
}

func (f *fakeClientConn) ReportError(err error) {
	f.errors <- err
}

func (f *fakeClientConn) NewAddress(_ []resolver.Address) {}
func (f *fakeClientConn) ParseServiceConfig(_ string) *serviceconfig.ParseResult {
	return nil
}
func (f *fakeClientConn) NewServiceConfig(_ string) {}

func makeTarget(rawURL string) resolver.Target {
	u, err := url.Parse(rawURL)
	if err != nil {
		panic(err)
	}
	return resolver.Target{URL: *u}
}

func TestResolveExistingPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "metadata-server-0",
			Namespace: "metadata-server-dev",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}
	client := fake.NewClientset(pod)

	cc := newFakeClientConn()
	builder := &kubeResolverBuilder{manager: NewPodWatcherManager(client)}
	r, err := builder.Build(
		makeTarget("k8s:///metadata-server-0.headless.metadata-server-dev.svc.cluster.local:4772"),
		cc,
		resolver.BuildOptions{},
	)
	require.NoError(t, err)
	defer r.Close()

	select {
	case state := <-cc.states:
		require.Len(t, state.Addresses, 1)
		require.Equal(t, "10.0.0.1:4772", state.Addresses[0].Addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for initial resolve")
	}
}

func TestResolvePodIPChanges(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "metadata-server-0",
			Namespace: "metadata-server-dev",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}
	client := fake.NewClientset(pod)

	cc := newFakeClientConn()
	builder := &kubeResolverBuilder{manager: NewPodWatcherManager(client)}
	r, err := builder.Build(
		makeTarget("k8s:///metadata-server-0.headless.metadata-server-dev.svc.cluster.local:4772"),
		cc,
		resolver.BuildOptions{},
	)
	require.NoError(t, err)
	defer r.Close()

	// Wait for initial resolve.
	select {
	case state := <-cc.states:
		require.Len(t, state.Addresses, 1)
		require.Equal(t, "10.0.0.1:4772", state.Addresses[0].Addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for initial resolve")
	}

	// Wait for the watch goroutine to establish its watch before
	// updating the pod. The fake client tracks watch actions.
	// N.B. This is needed for the test because the fake k8s client doesn't
	// track historical events. Real k8s would send updates that happened
	// before watch was called.
	require.Eventually(t, func() bool {
		for _, a := range client.Actions() {
			if a.GetVerb() == "watch" {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "timed out waiting for watch to be established")

	// Update the pod IP via the fake client.
	pod.Status.PodIP = "10.0.0.2"
	_, err = client.CoreV1().Pods("metadata-server-dev").Update(
		context.Background(), pod, metav1.UpdateOptions{},
	)
	require.NoError(t, err)

	// The watch should pick up the change.
	select {
	case state := <-cc.states:
		require.Len(t, state.Addresses, 1)
		require.Equal(t, "10.0.0.2:4772", state.Addresses[0].Addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for updated resolve")
	}
}

func TestResolveNonExistentPod(t *testing.T) {
	client := fake.NewClientset()

	cc := newFakeClientConn()
	builder := &kubeResolverBuilder{manager: NewPodWatcherManager(client)}
	r, err := builder.Build(
		makeTarget("k8s:///nonexistent-0.headless.ns.svc.cluster.local:4772"),
		cc,
		resolver.BuildOptions{},
	)
	require.NoError(t, err)
	defer r.Close()

	// Should get an error reported since pod doesn't exist.
	select {
	case err := <-cc.errors:
		require.Contains(t, err.Error(), "not found")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for error")
	}
}

func TestSharedWatcher(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-0",
			Namespace: "ns",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}
	client := fake.NewClientset(pod)

	builder := &kubeResolverBuilder{manager: NewPodWatcherManager(client)}

	// Build two resolvers for the same pod but different ports.
	cc1 := newFakeClientConn()
	r1, err := builder.Build(
		makeTarget("k8s:///pod-0.svc.ns.svc.cluster.local:8080"),
		cc1,
		resolver.BuildOptions{},
	)
	require.NoError(t, err)
	defer r1.Close()

	cc2 := newFakeClientConn()
	r2, err := builder.Build(
		makeTarget("k8s:///pod-0.svc.ns.svc.cluster.local:9090"),
		cc2,
		resolver.BuildOptions{},
	)
	require.NoError(t, err)
	defer r2.Close()

	// Both should resolve with their respective ports.
	for _, tc := range []struct {
		cc   *fakeClientConn
		port string
	}{
		{cc1, "8080"},
		{cc2, "9090"},
	} {
		select {
		case state := <-tc.cc.states:
			require.Len(t, state.Addresses, 1)
			require.Equal(t, "10.0.0.1:"+tc.port, state.Addresses[0].Addr)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "timed out waiting for resolve on port %s", tc.port)
		}
	}

	// Wait for the watch to be established before updating.
	require.Eventually(t, func() bool {
		for _, a := range client.Actions() {
			if a.GetVerb() == "watch" {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "timed out waiting for watch to be established")

	// Close one resolver; the other should still receive updates.
	r1.Close()

	pod.Status.PodIP = "10.0.0.2"
	_, err = client.CoreV1().Pods("ns").Update(
		context.Background(), pod, metav1.UpdateOptions{},
	)
	require.NoError(t, err)

	select {
	case state := <-cc2.states:
		require.Len(t, state.Addresses, 1)
		require.Equal(t, "10.0.0.2:9090", state.Addresses[0].Addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for update after closing r1")
	}
}

func TestWatchPodIPExistingPod(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-0",
			Namespace: "ns",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}
	client := fake.NewClientset(pod)
	m := NewPodWatcherManager(client)

	type result struct {
		addr string
		err  error
	}
	results := make(chan result, 10)
	cancel, err := m.WatchPodIP("pod-0.svc.ns.svc.cluster.local:7238", func(ipPort string, watchErr error) {
		results <- result{addr: ipPort, err: watchErr}
	})
	require.NoError(t, err)
	defer cancel()

	// Should get the initial IP.
	select {
	case r := <-results:
		require.NoError(t, r.err)
		require.Equal(t, "10.0.0.1:7238", r.addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for initial IP")
	}
}

func TestWatchPodIPUpdates(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-0",
			Namespace: "ns",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}
	client := fake.NewClientset(pod)
	m := NewPodWatcherManager(client)

	type result struct {
		addr string
		err  error
	}
	results := make(chan result, 10)
	cancel, err := m.WatchPodIP("pod-0.svc.ns.svc.cluster.local:7238", func(ipPort string, watchErr error) {
		results <- result{addr: ipPort, err: watchErr}
	})
	require.NoError(t, err)
	defer cancel()

	// Wait for initial resolve.
	select {
	case r := <-results:
		require.NoError(t, r.err)
		require.Equal(t, "10.0.0.1:7238", r.addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for initial IP")
	}

	// Wait for the watch to be established before updating.
	require.Eventually(t, func() bool {
		for _, a := range client.Actions() {
			if a.GetVerb() == "watch" {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond, "timed out waiting for watch")

	// Update the pod IP.
	pod.Status.PodIP = "10.0.0.2"
	_, err = client.CoreV1().Pods("ns").Update(
		context.Background(), pod, metav1.UpdateOptions{},
	)
	require.NoError(t, err)

	select {
	case r := <-results:
		require.NoError(t, r.err)
		require.Equal(t, "10.0.0.2:7238", r.addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for updated IP")
	}
}

func TestWatchPodIPNonExistentPod(t *testing.T) {
	client := fake.NewClientset()
	m := NewPodWatcherManager(client)

	type result struct {
		addr string
		err  error
	}
	results := make(chan result, 10)
	cancel, err := m.WatchPodIP("pod-0.svc.ns.svc.cluster.local:7238", func(ipPort string, watchErr error) {
		results <- result{addr: ipPort, err: watchErr}
	})
	require.NoError(t, err)
	defer cancel()

	// Should get an error since the pod doesn't exist.
	select {
	case r := <-results:
		require.Error(t, r.err)
		require.Contains(t, r.err.Error(), "not found")
		require.Empty(t, r.addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for error")
	}
}

func TestWatchPodIPCancel(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-0",
			Namespace: "ns",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}
	client := fake.NewClientset(pod)
	m := NewPodWatcherManager(client)

	type result struct {
		addr string
		err  error
	}
	results := make(chan result, 10)
	cancel, err := m.WatchPodIP("pod-0.svc.ns.svc.cluster.local:7238", func(ipPort string, watchErr error) {
		results <- result{addr: ipPort, err: watchErr}
	})
	require.NoError(t, err)

	// Wait for initial resolve.
	select {
	case r := <-results:
		require.NoError(t, r.err)
		require.Equal(t, "10.0.0.1:7238", r.addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for initial IP")
	}

	// Cancel the watch.
	cancel()

	// The watcher should be cleaned up from the manager.
	m.mu.Lock()
	require.Empty(t, m.watchers, "watcher should be removed after cancel")
	m.mu.Unlock()
}

func TestWatchPodIPInvalidTarget(t *testing.T) {
	client := fake.NewClientset()
	m := NewPodWatcherManager(client)

	_, err := m.WatchPodIP("not-a-pod-fqdn", func(string, error) {})
	require.Error(t, err)
}

func TestWatchPodIPSharedWatcher(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-0",
			Namespace: "ns",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}
	client := fake.NewClientset(pod)
	m := NewPodWatcherManager(client)

	type result struct {
		addr string
		err  error
	}

	// Two watchers for the same pod but different ports should share a
	// single underlying podWatcher.
	results1 := make(chan result, 10)
	cancel1, err := m.WatchPodIP("pod-0.svc.ns.svc.cluster.local:7238", func(ipPort string, watchErr error) {
		results1 <- result{addr: ipPort, err: watchErr}
	})
	require.NoError(t, err)
	defer cancel1()

	results2 := make(chan result, 10)
	cancel2, err := m.WatchPodIP("pod-0.svc.ns.svc.cluster.local:9090", func(ipPort string, watchErr error) {
		results2 <- result{addr: ipPort, err: watchErr}
	})
	require.NoError(t, err)
	defer cancel2()

	// Both should resolve with their respective ports.
	select {
	case r := <-results1:
		require.NoError(t, r.err)
		require.Equal(t, "10.0.0.1:7238", r.addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for first watcher")
	}

	select {
	case r := <-results2:
		require.NoError(t, r.err)
		require.Equal(t, "10.0.0.1:9090", r.addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for second watcher")
	}

	// Only one underlying watcher should exist.
	m.mu.Lock()
	require.Len(t, m.watchers, 1, "expected a single shared watcher")
	m.mu.Unlock()

	// Cancelling one should keep the watcher alive.
	cancel1()
	m.mu.Lock()
	require.Len(t, m.watchers, 1, "watcher should survive after one cancel")
	m.mu.Unlock()

	// Cancelling both should remove the watcher.
	cancel2()
	m.mu.Lock()
	require.Empty(t, m.watchers, "watcher should be removed after all cancels")
	m.mu.Unlock()
}

func TestWatchRecoveryFromResourceExpired(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-0",
			Namespace: "ns",
		},
		Status: corev1.PodStatus{
			PodIP: "10.0.0.1",
		},
	}
	client := fake.NewClientset(pod)

	// Intercept the first watch call to return a 410 Gone error,
	// simulating an expired resource version.
	var watchCount atomic.Int32
	client.PrependWatchReactor("pods", func(action k8stesting.Action) (bool, watch.Interface, error) {
		if watchCount.Add(1) == 1 {
			fw := watch.NewFake()
			go func() {
				fw.Error(&metav1.Status{
					Status:  metav1.StatusFailure,
					Code:    410,
					Reason:  metav1.StatusReasonExpired,
					Message: "too old resource version: 123 (456)",
				})
			}()
			return true, fw, nil
		}
		return false, nil, nil
	})

	cc := newFakeClientConn()
	builder := &kubeResolverBuilder{manager: NewPodWatcherManager(client)}
	r, err := builder.Build(
		makeTarget("k8s:///pod-0.svc.ns.svc.cluster.local:8080"),
		cc,
		resolver.BuildOptions{},
	)
	require.NoError(t, err)
	defer r.Close()

	// Wait for initial resolve.
	select {
	case state := <-cc.states:
		require.Len(t, state.Addresses, 1)
		require.Equal(t, "10.0.0.1:8080", state.Addresses[0].Addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for initial resolve")
	}

	// After the 410, the resolver should re-resolve and establish a new watch.
	require.Eventually(t, func() bool {
		return watchCount.Load() >= 2
	}, 5*time.Second, 10*time.Millisecond, "expected watch to be retried after 410")

	// Drain any re-resolve state updates.
	for len(cc.states) > 0 {
		<-cc.states
	}

	// Update the pod IP and verify the recovered watch picks it up.
	pod.Status.PodIP = "10.0.0.2"
	_, err = client.CoreV1().Pods("ns").Update(
		context.Background(), pod, metav1.UpdateOptions{},
	)
	require.NoError(t, err)

	select {
	case state := <-cc.states:
		require.Len(t, state.Addresses, 1)
		require.Equal(t, "10.0.0.2:8080", state.Addresses[0].Addr)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "timed out waiting for updated address after 410 recovery")
	}
}
