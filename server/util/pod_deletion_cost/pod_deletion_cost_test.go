package pod_deletion_cost

import (
	"math"
	"testing"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	corev1 "k8s.io/api/core/v1"
	k8smeta "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stesting "k8s.io/client-go/testing"
)

func newFakePod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: k8smeta.ObjectMeta{
			Name:      "executor-pod",
			Namespace: "executor-ns",
		},
	}
}

func getAnnotation(t *testing.T, client kubernetes.Interface) string {
	pod, err := client.CoreV1().Pods("executor-ns").Get(t.Context(), "executor-pod", k8smeta.GetOptions{})
	require.NoError(t, err)
	return pod.GetAnnotations()[AnnotationKey]
}

func TestUpdateSetsAnnotationToCost(t *testing.T) {
	ctx := t.Context()
	client := fake.NewClientset(newFakePod())
	cost := int32(0)
	u := NewUpdater(client, "executor-pod", "executor-ns", clockwork.NewFakeClock(), func() int32 { return cost })

	// With a cost of 0, the first update should explicitly set the deletion
	// cost annotation to 0, marking this pod as the cheapest to delete.
	require.NoError(t, u.Update(ctx))
	require.Equal(t, "0", getAnnotation(t, client))

	// Raise the cost; the annotation should be updated to match.
	cost = 90
	require.NoError(t, u.Update(ctx))
	require.Equal(t, "90", getAnnotation(t, client))

	// Drop the cost back to 0; the annotation should follow.
	cost = 0
	require.NoError(t, u.Update(ctx))
	require.Equal(t, "0", getAnnotation(t, client))
}

func TestUpdateSkipsPatchIfCostUnchanged(t *testing.T) {
	ctx := t.Context()
	client := fake.NewClientset(newFakePod())
	cost := int32(60)
	u := NewUpdater(client, "executor-pod", "executor-ns", clockwork.NewFakeClock(), func() int32 { return cost })

	countPatches := func() int {
		n := 0
		for _, action := range client.Actions() {
			if _, ok := action.(k8stesting.PatchAction); ok {
				n++
			}
		}
		return n
	}

	// The first update should patch the pod.
	require.NoError(t, u.Update(ctx))
	require.Equal(t, 1, countPatches())

	// The cost hasn't changed, so the next update should not issue another
	// patch.
	require.NoError(t, u.Update(ctx))
	require.Equal(t, 1, countPatches())

	// Once the cost changes, the next update should patch the pod again.
	cost = 61
	require.NoError(t, u.Update(ctx))
	require.Equal(t, 2, countPatches())
	require.Equal(t, "61", getAnnotation(t, client))
}

func TestUpdateAllowsNegativeCost(t *testing.T) {
	ctx := t.Context()
	client := fake.NewClientset(newFakePod())
	// Report a negative cost, which Kubernetes allows; it should be written
	// to the annotation as-is.
	cost := int32(math.MinInt32)
	u := NewUpdater(client, "executor-pod", "executor-ns", clockwork.NewFakeClock(), func() int32 { return cost })
	require.NoError(t, u.Update(ctx))
	require.Equal(t, "-2147483648", getAnnotation(t, client))
}
