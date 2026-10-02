// Package k8singest converts Kubernetes resources into internal resources and
// stores them for searching.
package k8singest

import (
	"fmt"
	"slices"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/summaries"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/cache"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Summarize takes a Kubernetes resource from the kubernetes client and turns
// it into an Entry.
// The input type is "any" because the input can arrive in two different formats.
// For types for which we index detailed information (e.g. Pods) it arrives as a
// *unstructured.Unstructured value which contains the full resource.
// For other types, it arrives is a *metav1.PartialObjectMetadata and for these
// we index the common object attributes.
func Summarize(res summaries.ResourceType, obj any) (*summaries.Entry, error) {
	m, ok := obj.(metav1.Object)
	if !ok {
		return nil, fmt.Errorf("cannot summarize %T", obj)
	}
	e := &summaries.Entry{
		Cluster:   res.Cluster,
		Group:     res.Group,
		Version:   res.Version,
		Resource:  res.Resource,
		Kind:      res.Kind,
		Namespace: m.GetNamespace(),
		Name:      m.GetName(),
		UID:       string(m.GetUID()),
		Created:   m.GetCreationTimestamp().Time,
		Labels:    m.GetLabels(),
	}
	for _, ref := range m.GetOwnerReferences() {
		if ref.Controller != nil && *ref.Controller {
			e.Owner = ref.Kind + "/" + ref.Name
			break
		}
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		enrich(e, u)
	}
	if m.GetDeletionTimestamp() != nil {
		e.Phase = "Terminating"
	}
	return e, nil
}

// enrich fills in kind-specific fields from the full object.
func enrich(e *summaries.Entry, u *unstructured.Unstructured) {
	switch e.Kind {
	case "Pod":
		enrichPod(e, u)
	case "Deployment", "StatefulSet", "ReplicaSet", "DaemonSet":
		enrichWorkload(e, u)
	case "Service":
		enrichService(e, u)
	case "Node":
		enrichNode(e, u)
	case "Job":
		enrichJob(e, u)
	case "CronJob":
		enrichCronJob(e, u)
	case "Namespace":
		e.Phase, _, _ = unstructured.NestedString(u.Object, "status", "phase")
	}
}

func enrichPod(e *summaries.Entry, u *unstructured.Unstructured) {
	e.Node, _, _ = unstructured.NestedString(u.Object, "spec", "nodeName")
	e.Hostname, _, _ = unstructured.NestedString(u.Object, "spec", "hostname")
	e.Subdomain, _, _ = unstructured.NestedString(u.Object, "spec", "subdomain")
	e.Phase, _, _ = unstructured.NestedString(u.Object, "status", "phase")
	if ip, _, _ := unstructured.NestedString(u.Object, "status", "podIP"); ip != "" {
		e.IPs = []string{ip}
	}

	// Sidecars (init containers with restartPolicy Always) run for the life of
	// the pod, so they count as regular containers, as in kubectl.
	sidecars := map[string]bool{}
	var containers []any
	initContainers, _, _ := unstructured.NestedSlice(u.Object, "spec", "initContainers")
	for _, c := range initContainers {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if rp, _, _ := unstructured.NestedString(cm, "restartPolicy"); rp == "Always" {
			name, _, _ := unstructured.NestedString(cm, "name")
			sidecars[name] = true
			containers = append(containers, cm)
		}
	}
	regular, _, _ := unstructured.NestedSlice(u.Object, "spec", "containers")
	containers = append(containers, regular...)
	for _, c := range containers {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		if name, _, _ := unstructured.NestedString(cm, "name"); name != "" {
			e.Containers = append(e.Containers, name)
		}
		if img, _, _ := unstructured.NestedString(cm, "image"); img != "" {
			e.Images = append(e.Images, img)
		}
		ports, _, _ := unstructured.NestedSlice(cm, "ports")
		for _, p := range ports {
			pm, ok := p.(map[string]any)
			if !ok {
				continue
			}
			port := summaries.Port{}
			port.Name, _, _ = unstructured.NestedString(pm, "name")
			if n, ok, _ := unstructured.NestedInt64(pm, "containerPort"); ok {
				port.Port = int32(n)
			}
			if proto, _, _ := unstructured.NestedString(pm, "protocol"); proto != "TCP" {
				port.Protocol = proto
			}
			if port.Port != 0 {
				e.Ports = append(e.Ports, port)
			}
		}
	}

	ready := 0
	initStatuses, _, _ := unstructured.NestedSlice(u.Object, "status", "initContainerStatuses")
	for _, s := range initStatuses {
		sm, ok := s.(map[string]any)
		if !ok {
			continue
		}
		name, _, _ := unstructured.NestedString(sm, "name")
		if r, _, _ := unstructured.NestedBool(sm, "ready"); r && sidecars[name] {
			ready++
		}
		// A finished init container's restarts are history, as in kubectl.
		if code, done, _ := unstructured.NestedInt64(sm, "state", "terminated", "exitCode"); done && code == 0 {
			continue
		}
		if n, _, _ := unstructured.NestedInt64(sm, "restartCount"); n > 0 {
			e.Restarts += n
		}
		if reason := waitingReason(sm); reason != "" {
			e.Phase = "Init:" + reason
		}
	}
	statuses, _, _ := unstructured.NestedSlice(u.Object, "status", "containerStatuses")
	for _, s := range statuses {
		sm, ok := s.(map[string]any)
		if !ok {
			continue
		}
		if r, _, _ := unstructured.NestedBool(sm, "ready"); r {
			ready++
		}
		if n, _, _ := unstructured.NestedInt64(sm, "restartCount"); n > 0 {
			e.Restarts += n
		}
		if reason := waitingReason(sm); reason != "" {
			e.Phase = reason
		}
	}
	if len(containers) > 0 {
		e.Ready = fmt.Sprintf("%d/%d", ready, len(containers))
	}
}

// waitingReason returns a container's waiting reason (CrashLoopBackOff,
// ImagePullBackOff, ...) when it says more than the pod phase does.
func waitingReason(status map[string]any) string {
	reason, _, _ := unstructured.NestedString(status, "state", "waiting", "reason")
	if reason == "ContainerCreating" || reason == "PodInitializing" {
		return ""
	}
	return reason
}

func enrichWorkload(e *summaries.Entry, u *unstructured.Unstructured) {
	var ready, desired int64
	if e.Kind == "DaemonSet" {
		ready, _, _ = unstructured.NestedInt64(u.Object, "status", "numberReady")
		desired, _, _ = unstructured.NestedInt64(u.Object, "status", "desiredNumberScheduled")
	} else {
		ready, _, _ = unstructured.NestedInt64(u.Object, "status", "readyReplicas")
		if n, ok, _ := unstructured.NestedInt64(u.Object, "spec", "replicas"); ok {
			desired = n
		} else {
			desired = 1 // the API default when spec.replicas is unset
		}
	}
	e.Ready = fmt.Sprintf("%d/%d", ready, desired)
	e.Selector, _, _ = unstructured.NestedStringMap(u.Object, "spec", "selector", "matchLabels")

	containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "template", "spec", "containers")
	for _, c := range containers {
		if cm, ok := c.(map[string]any); ok {
			if img, _, _ := unstructured.NestedString(cm, "image"); img != "" {
				e.Images = append(e.Images, img)
			}
		}
	}
}

func enrichService(e *summaries.Entry, u *unstructured.Unstructured) {
	e.Phase, _, _ = unstructured.NestedString(u.Object, "spec", "type")
	e.Selector, _, _ = unstructured.NestedStringMap(u.Object, "spec", "selector")
	// clusterIPs has both families of a dual-stack service; objects from
	// before it existed only have clusterIP.
	addrs, ok, _ := unstructured.NestedStringSlice(u.Object, "spec", "clusterIPs")
	if !ok {
		ip, _, _ := unstructured.NestedString(u.Object, "spec", "clusterIP")
		addrs = []string{ip}
	}
	external, _, _ := unstructured.NestedStringSlice(u.Object, "spec", "externalIPs")
	addrs = append(addrs, external...)
	ingress, _, _ := unstructured.NestedSlice(u.Object, "status", "loadBalancer", "ingress")
	for _, i := range ingress {
		if im, ok := i.(map[string]any); ok {
			ip, _, _ := unstructured.NestedString(im, "ip")
			host, _, _ := unstructured.NestedString(im, "hostname") // AWS load balancers
			addrs = append(addrs, ip, host)
		}
	}
	name, _, _ := unstructured.NestedString(u.Object, "spec", "externalName")
	for _, a := range append(addrs, name) {
		if a != "" && a != "None" { // "None" is a headless service
			e.IPs = append(e.IPs, a)
		}
	}
	ports, _, _ := unstructured.NestedSlice(u.Object, "spec", "ports")
	for _, p := range ports {
		pm, ok := p.(map[string]any)
		if !ok {
			continue
		}
		port := summaries.Port{}
		port.Name, _, _ = unstructured.NestedString(pm, "name")
		if n, ok, _ := unstructured.NestedInt64(pm, "port"); ok {
			port.Port = int32(n)
		}
		if n, ok, _ := unstructured.NestedInt64(pm, "nodePort"); ok {
			port.NodePort = int32(n)
		}
		if proto, _, _ := unstructured.NestedString(pm, "protocol"); proto != "TCP" {
			port.Protocol = proto
		}
		if port.Port != 0 {
			e.Ports = append(e.Ports, port)
		}
	}
}

func enrichNode(e *summaries.Entry, u *unstructured.Unstructured) {
	e.Phase = "NotReady"
	conditions, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conditions {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		typ, _, _ := unstructured.NestedString(cm, "type")
		st, _, _ := unstructured.NestedString(cm, "status")
		if typ == "Ready" && st == "True" {
			e.Phase = "Ready"
		}
	}
	if unschedulable, _, _ := unstructured.NestedBool(u.Object, "spec", "unschedulable"); unschedulable {
		e.Phase += ",SchedulingDisabled"
	}
	addrs, _, _ := unstructured.NestedSlice(u.Object, "status", "addresses")
	for _, a := range addrs {
		am, ok := a.(map[string]any)
		if !ok {
			continue
		}
		typ, _, _ := unstructured.NestedString(am, "type")
		addr, _, _ := unstructured.NestedString(am, "address")
		if addr != "" && (typ == "InternalIP" || typ == "ExternalIP") {
			e.IPs = append(e.IPs, addr)
		}
	}
	if v, _, _ := unstructured.NestedString(u.Object, "status", "nodeInfo", "kubeletVersion"); v != "" {
		e.SetExtra("kubelet", v)
	}
	var roles []string
	for k := range e.Labels {
		if role, ok := strings.CutPrefix(k, "node-role.kubernetes.io/"); ok && role != "" {
			roles = append(roles, role)
		}
	}
	if len(roles) > 0 {
		slices.Sort(roles)
		e.SetExtra("roles", strings.Join(roles, ","))
	}
}

func enrichJob(e *summaries.Entry, u *unstructured.Unstructured) {
	succeeded, _, _ := unstructured.NestedInt64(u.Object, "status", "succeeded")
	failed, _, _ := unstructured.NestedInt64(u.Object, "status", "failed")
	active, _, _ := unstructured.NestedInt64(u.Object, "status", "active")
	suspended, _, _ := unstructured.NestedBool(u.Object, "spec", "suspend")
	switch {
	case conditionTrue(u, "Complete"):
		e.Phase = "Complete"
	case conditionTrue(u, "Failed"):
		e.Phase = "Failed"
	case suspended:
		e.Phase = "Suspended"
	case active > 0:
		e.Phase = "Active"
	case failed > 0:
		e.Phase = "Retrying"
	}
	// Same as kubectl's COMPLETIONS column. Without spec.completions the job
	// is a work queue, done once any one pod succeeds.
	if n, ok, _ := unstructured.NestedInt64(u.Object, "spec", "completions"); ok {
		e.Ready = fmt.Sprintf("%d/%d", succeeded, n)
	} else if p, _, _ := unstructured.NestedInt64(u.Object, "spec", "parallelism"); p > 1 {
		e.Ready = fmt.Sprintf("%d/1 of %d", succeeded, p)
	} else {
		e.Ready = fmt.Sprintf("%d/1", succeeded)
	}
}

// conditionTrue reports whether status.conditions has the named condition
// with status True.
func conditionTrue(u *unstructured.Unstructured, name string) bool {
	conditions, _, _ := unstructured.NestedSlice(u.Object, "status", "conditions")
	for _, c := range conditions {
		cm, ok := c.(map[string]any)
		if !ok {
			continue
		}
		typ, _, _ := unstructured.NestedString(cm, "type")
		st, _, _ := unstructured.NestedString(cm, "status")
		if typ == name && st == "True" {
			return true
		}
	}
	return false
}

func enrichCronJob(e *summaries.Entry, u *unstructured.Unstructured) {
	if schedule, _, _ := unstructured.NestedString(u.Object, "spec", "schedule"); schedule != "" {
		e.SetExtra("schedule", schedule)
	}
	if suspended, _, _ := unstructured.NestedBool(u.Object, "spec", "suspend"); suspended {
		e.Phase = "Suspended"
	}
}

// Store is the write side of a summary Store, as a Reflector sees it.
// It takes objects from the kubernetes client, converts them into internal
// types and passes that to the underlying store.
type Store struct {
	res summaries.ResourceType
	s   *summaries.Store
}

var _ cache.ReflectorStore = (*Store)(nil)

func NewStore(s *summaries.Store) *Store {
	return &Store{res: s.ResourceType(), s: s}
}

func (a *Store) Add(obj any) error    { return a.put(obj) }
func (a *Store) Update(obj any) error { return a.put(obj) }

func (a *Store) put(obj any) error {
	e, err := Summarize(a.res, obj)
	if err != nil {
		return err
	}
	a.s.Put(e)
	return nil
}

func (a *Store) Delete(obj any) error {
	key, err := a.keyOf(obj)
	if err != nil {
		return err
	}
	a.s.Delete(key)
	return nil
}

// keyOf returns the store key for a Kubernetes object or for a tombstone from
// a missed delete, which carries only the key.
func (a *Store) keyOf(obj any) (string, error) {
	if d, ok := obj.(cache.DeletedFinalStateUnknown); ok {
		return d.Key, nil
	}
	e, err := Summarize(a.res, obj)
	if err != nil {
		return "", err
	}
	return e.Key(), nil
}

// Replace substitutes the full contents, as a Reflector does after each relist.
func (a *Store) Replace(objs []any, _ string) error {
	entries := make([]*summaries.Entry, 0, len(objs))
	for _, obj := range objs {
		e, err := Summarize(a.res, obj)
		if err != nil {
			return err
		}
		entries = append(entries, e)
	}
	a.s.Replace(entries)
	return nil
}

func (a *Store) Resync() error { return nil }
