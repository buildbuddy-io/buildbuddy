// Package k8singest converts Kubernetes resources into internal resources and
// stores them for searching.
package k8singest

import (
	"fmt"
	"sort"
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
	case "Deployment", "StatefulSet", "ReplicaSet":
		enrichWorkload(e, u, "readyReplicas", "replicas")
	case "DaemonSet":
		enrichWorkload(e, u, "numberReady", "desiredNumberScheduled")
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
	e.Phase, _, _ = unstructured.NestedString(u.Object, "status", "phase")
	if ip, _, _ := unstructured.NestedString(u.Object, "status", "podIP"); ip != "" {
		e.IPs = []string{ip}
	}

	containers, _, _ := unstructured.NestedSlice(u.Object, "spec", "containers")
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

	statuses, _, _ := unstructured.NestedSlice(u.Object, "status", "containerStatuses")
	ready := 0
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
		// A waiting reason (CrashLoopBackOff, ImagePullBackOff, ...) is more
		// informative than the generic phase, so let it win.
		if reason, _, _ := unstructured.NestedString(sm, "state", "waiting", "reason"); reason != "" && reason != "ContainerCreating" {
			e.Phase = reason
		}
	}
	if len(containers) > 0 {
		e.Ready = fmt.Sprintf("%d/%d", ready, len(containers))
	}
}

func enrichWorkload(e *summaries.Entry, u *unstructured.Unstructured, readyField, desiredField string) {
	ready, _, _ := unstructured.NestedInt64(u.Object, "status", readyField)
	var desired int64
	if e.Kind == "DaemonSet" {
		desired, _, _ = unstructured.NestedInt64(u.Object, "status", desiredField)
	} else if n, ok, _ := unstructured.NestedInt64(u.Object, "spec", desiredField); ok {
		desired = n
	} else {
		desired = 1 // the API default when spec.replicas is unset
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
	if ip, _, _ := unstructured.NestedString(u.Object, "spec", "clusterIP"); ip != "" && ip != "None" {
		e.IPs = append(e.IPs, ip)
	}
	ingress, _, _ := unstructured.NestedSlice(u.Object, "status", "loadBalancer", "ingress")
	for _, i := range ingress {
		if im, ok := i.(map[string]any); ok {
			if ip, _, _ := unstructured.NestedString(im, "ip"); ip != "" {
				e.IPs = append(e.IPs, ip)
			}
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
		sort.Strings(roles)
		e.SetExtra("roles", strings.Join(roles, ","))
	}
}

func enrichJob(e *summaries.Entry, u *unstructured.Unstructured) {
	succeeded, _, _ := unstructured.NestedInt64(u.Object, "status", "succeeded")
	failed, _, _ := unstructured.NestedInt64(u.Object, "status", "failed")
	active, _, _ := unstructured.NestedInt64(u.Object, "status", "active")
	switch {
	case conditionTrue(u, "Complete"):
		e.Phase = "Complete"
	case conditionTrue(u, "Failed"):
		e.Phase = "Failed"
	case active > 0:
		e.Phase = "Active"
	case failed > 0:
		e.Phase = "Retrying"
	}
	completions := int64(1)
	if n, ok, _ := unstructured.NestedInt64(u.Object, "spec", "completions"); ok {
		completions = n
	}
	e.Ready = fmt.Sprintf("%d/%d", succeeded, completions)
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

// Store is a wrapper over a summary Store.
// It takes objects from the kubernetes client, converts them into internal
// types and passes that to the underyling store.
type Store struct {
	res summaries.ResourceType
	s   *summaries.Store
}

var _ cache.Store = (*Store)(nil)

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

// keyOf returns the store key for anything cache.Store may be handed: a
// Kubernetes object, a tombstone from a missed delete (which carries only the
// key), or an entry that List returned.
func (a *Store) keyOf(obj any) (string, error) {
	switch o := obj.(type) {
	case cache.DeletedFinalStateUnknown:
		return o.Key, nil
	case *summaries.Entry:
		return o.Key(), nil
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

func (a *Store) List() []any {
	entries := a.s.Entries()
	out := make([]any, 0, len(entries))
	for _, e := range entries {
		out = append(out, e)
	}
	return out
}

func (a *Store) ListKeys() []string {
	entries := a.s.Entries()
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.Key())
	}
	return out
}

func (a *Store) Get(obj any) (any, bool, error) {
	key, err := a.keyOf(obj)
	if err != nil {
		return nil, false, err
	}
	return a.GetByKey(key)
}

func (a *Store) GetByKey(key string) (any, bool, error) {
	e, ok := a.s.Get(key)
	if !ok {
		return nil, false, nil
	}
	return e, true, nil
}
