// Package pod_deletion_cost keeps the
// controller.kubernetes.io/pod-deletion-cost annotation on the server's own
// pod up to date, so that on scale-down, Kubernetes prefers to delete the
// pods with the lowest deletion cost. For example, executors report the total
// execution progress of their running tasks as the cost, so that scale-down
// wastes as little work as possible.
package pod_deletion_cost

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"strconv"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/jonboulle/clockwork"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	k8smeta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	enabled = flag.Bool("pod_deletion_cost.enabled", false, "Whether to periodically update the controller.kubernetes.io/pod-deletion-cost annotation on this server's own pod. On scale-down, Kubernetes prefers to delete the pods with the lowest deletion cost. Requires the MY_POD_NAME and MY_NAMESPACE environment variables (typically set via the downward API) and RBAC permission to patch the pod.")

	// Note: Kubernetes docs discourage frequent updates of this annotation:
	// https://kubernetes.io/docs/concepts/workloads/controllers/replicaset/#pod-deletion-cost
	updateInterval = flag.Duration("pod_deletion_cost.update_interval", 30*time.Second, "How often to update the pod-deletion-cost annotation.")
)

// AnnotationKey is the annotation that the ReplicaSet controller reads when
// deciding which pods to delete first on scale-down. Pods with lower values
// are deleted first.
const AnnotationKey = "controller.kubernetes.io/pod-deletion-cost"

const patchTimeout = 10 * time.Second

// An Updater periodically patches the server's own pod, setting the
// pod-deletion-cost annotation to the value returned by the cost function.
type Updater struct {
	client    kubernetes.Interface
	clock     clockwork.Clock
	podName   string
	namespace string
	cost      func() int32

	lastCost    int32
	hasLastCost bool
}

func NewUpdater(client kubernetes.Interface, podName, namespace string, clock clockwork.Clock, cost func() int32) *Updater {
	return &Updater{
		client:    client,
		clock:     clock,
		podName:   podName,
		namespace: namespace,
		cost:      cost,
	}
}

// Start begins updating the pod-deletion-cost annotation in the background,
// if enabled. cost returns the current deletion cost for this pod.
func Start(ctx context.Context, podName, podNamespace string, cost func() int32) error {
	if !*enabled {
		return nil
	}
	if podName == "" {
		return fmt.Errorf("pod deletion cost updates require a pod name; make sure the MY_POD_NAME and MY_NAMESPACE environment variables are set")
	}
	config, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("get in-cluster k8s config: %w", err)
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("create k8s client: %w", err)
	}
	log.Infof("Starting pod deletion cost updates for pod %q in namespace %q", podName, podNamespace)
	u := NewUpdater(client, podName, podNamespace, clockwork.NewRealClock(), cost)
	go u.Run(ctx)
	return nil
}

// Run updates the annotation immediately and then on every update interval,
// until ctx is canceled.
func (u *Updater) Run(ctx context.Context) {
	ticker := u.clock.NewTicker(*updateInterval)
	defer ticker.Stop()
	for {
		if err := u.Update(ctx); err != nil {
			log.Warningf("Could not update pod deletion cost: %s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.Chan():
		}
	}
}

// Update patches the pod-deletion-cost annotation with the current cost. The
// patch is skipped if the cost is unchanged since the last update.
func (u *Updater) Update(ctx context.Context) error {
	cost := u.cost()
	if u.hasLastCost && cost == u.lastCost {
		return nil
	}
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{
				AnnotationKey: strconv.FormatInt(int64(cost), 10),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, patchTimeout)
	defer cancel()
	if _, err := u.client.CoreV1().Pods(u.namespace).Patch(ctx, u.podName, types.StrategicMergePatchType, patch, k8smeta.PatchOptions{}); err != nil {
		return fmt.Errorf("patch pod %s/%s: %w", u.namespace, u.podName, err)
	}
	u.lastCost = cost
	u.hasLastCost = true
	return nil
}
