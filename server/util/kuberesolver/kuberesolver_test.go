package kuberesolver

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/serviceconfig"
	"k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	builder := &kubeResolverBuilder{client: client}
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
	builder := &kubeResolverBuilder{client: client}
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
	builder := &kubeResolverBuilder{client: client}
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

	builder := &kubeResolverBuilder{client: client}

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
