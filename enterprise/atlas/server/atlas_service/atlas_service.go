// Package atlas_service implements the API used by the frontend.
package atlas_service

import (
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/cluster"
	"github.com/buildbuddy-io/buildbuddy/enterprise/atlas/server/summaries"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	atlaspb "github.com/buildbuddy-io/buildbuddy/proto/atlas"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	sigsyaml "sigs.k8s.io/yaml"
)

const (
	// maxRelatedPods caps the pod tables on detail pages
	maxRelatedPods = 250

	defaultLogTailLines = 200
	logChunkSize        = 32 * 1024
)

type AtlasService struct {
	c *cluster.Cluster
}

func New(c *cluster.Cluster) *AtlasService {
	return &AtlasService{c: c}
}

// cluster resolves a request's cluster name. Today there is one cluster, but
// requests and URLs keep naming so we can support multiple clusters in the
// future.
func (s *AtlasService) cluster(name string) (*cluster.Cluster, bool) {
	if name != s.c.Name() {
		return nil, false
	}
	return s.c, true
}

func (s *AtlasService) GetFilterValues(ctx context.Context, req *atlaspb.GetFilterValuesRequest) (*atlaspb.GetFilterValuesResponse, error) {
	values, err := s.c.Index().GetFilterValues(req.GetField(), req.GetKey(), req.GetPrefix(), int(req.GetLimit()))
	if err != nil {
		return nil, status.InvalidArgumentError(err.Error())
	}
	rsp := &atlaspb.GetFilterValuesResponse{}
	for _, v := range values {
		rsp.Values = append(rsp.Values, &atlaspb.FilterValue{Value: v.Value, Count: int32(v.Count)})
	}
	return rsp, nil
}

func (s *AtlasService) Search(ctx context.Context, req *atlaspb.SearchRequest) (*atlaspb.SearchResponse, error) {
	start := time.Now()
	res := s.c.Index().Search(req.GetQuery(), int(req.GetGroupLimit()))
	rsp := &atlaspb.SearchResponse{
		Total:    int32(res.Total),
		TookUsec: time.Since(start).Microseconds(),
	}
	for _, g := range res.Groups {
		group := &atlaspb.SearchGroup{Group: g.Group, Kind: g.Kind, Total: int32(g.Total)}
		for _, e := range g.Items {
			group.Results = append(group.Results, &atlaspb.SearchResult{
				Entry: entryProto(e),
				Links: portLinks(s.c, e, nil),
			})
		}
		rsp.Groups = append(rsp.Groups, group)
	}
	return rsp, nil
}

func (s *AtlasService) GetStatus(ctx context.Context, req *atlaspb.GetStatusRequest) (*atlaspb.GetStatusResponse, error) {
	st := s.c.Status()
	cs := &atlaspb.ClusterStatus{
		Name:           st.Name,
		SvcZone:        st.SvcZone,
		PodZone:        st.PodZone,
		DiscoveryError: st.DiscoveryError,
		LastDiscovery:  timestampOrNil(st.LastDiscovery),
		TotalObjects:   int32(st.TotalObjects),
	}
	for _, r := range st.Resources {
		cs.Resources = append(cs.Resources, &atlaspb.ResourceTypeStatus{
			Resource:  resourceProto(r.ResourceType),
			Count:     int32(r.Count),
			Synced:    r.Synced,
			Error:     r.Error,
			ErrorTime: timestampOrNil(r.ErrTime),
		})
	}
	return &atlaspb.GetStatusResponse{Clusters: []*atlaspb.ClusterStatus{cs}}, nil
}

func (s *AtlasService) GetObject(ctx context.Context, req *atlaspb.GetObjectRequest) (*atlaspb.GetObjectResponse, error) {
	if req.GetCluster() == "" || req.GetVersion() == "" || req.GetResource() == "" || req.GetName() == "" {
		return nil, status.InvalidArgumentError("cluster, version, resource and name are required")
	}
	c, ok := s.cluster(req.GetCluster())
	if !ok {
		return nil, status.NotFoundErrorf("unknown cluster %q", req.GetCluster())
	}
	res := summaries.ResourceType{
		Cluster:    req.GetCluster(),
		Group:      req.GetGroup(),
		Version:    req.GetVersion(),
		Resource:   req.GetResource(),
		Namespaced: req.GetNamespace() != "",
	}
	u, err := c.GetObject(ctx, res, req.GetNamespace(), req.GetName())
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, status.NotFoundErrorf("%s/%s not found in %s", res.Resource, req.GetName(), req.GetCluster())
		}
		return nil, status.UnavailableErrorf("fetching %s/%s from %s: %s", res.Resource, req.GetName(), req.GetCluster(), err)
	}
	yamlBytes, err := sigsyaml.Marshal(u.Object)
	if err != nil {
		yamlBytes = []byte(fmt.Sprintf("# rendering failed: %s", err))
	}

	rsp := &atlaspb.GetObjectResponse{Kind: u.GetKind(), Yaml: string(yamlBytes)}
	if entry, ok := s.c.Index().GetEntry(req.GetCluster(), req.GetGroup(), req.GetResource(), req.GetNamespace(), req.GetName()); ok {
		rel := s.relationsFor(entry)
		rsp.Entry = entryProto(entry)
		rsp.Relations = relationsProto(rel)
		rsp.Ports = portLinks(c, entry, rel)
	}
	if events, err := c.Events(ctx, u); err != nil {
		rsp.EventsError = err.Error()
	} else {
		for _, ev := range events {
			rsp.Events = append(rsp.Events, &atlaspb.Event{
				Type:     ev.Type,
				Reason:   ev.Reason,
				Message:  ev.Message,
				Count:    ev.Count,
				LastSeen: timestampOrNil(ev.LastSeen),
			})
		}
	}
	return rsp, nil
}

// mapLogFetchError maps a k8s API error into a user-facing error.
func mapLogFetchError(err error) error {
	var refusal *apierrors.StatusError
	switch {
	case apierrors.IsNotFound(err):
		return status.NotFoundErrorf("%s", err)
	case errors.As(err, &refusal):
		return status.FailedPreconditionErrorf("%s", err)
	}
	return status.UnavailableErrorf("streaming logs: %s", err)
}

func (s *AtlasService) StreamLogs(req *atlaspb.StreamLogsRequest, stream atlaspb.AtlasService_StreamLogsServer) error {
	if req.GetCluster() == "" || req.GetNamespace() == "" || req.GetName() == "" {
		return status.InvalidArgumentError("cluster, namespace and name are required")
	}
	c, ok := s.cluster(req.GetCluster())
	if !ok {
		return status.NotFoundErrorf("unknown cluster %q", req.GetCluster())
	}
	opts := cluster.LogOptions{
		Container: req.GetContainer(),
		TailLines: defaultLogTailLines,
		Follow:    req.GetFollow(),
		Previous:  req.GetPrevious(),
	}
	if req.GetTailLines() > 0 {
		opts.TailLines = req.GetTailLines()
	}
	logs, err := c.Logs(stream.Context(), req.GetNamespace(), req.GetName(), opts)
	if err != nil {
		return mapLogFetchError(err)
	}
	defer logs.Close()

	buf := make([]byte, logChunkSize)
	for {
		n, err := logs.Read(buf)
		if n > 0 {
			if err := stream.Send(&atlaspb.StreamLogsResponse{Data: bytes.Clone(buf[:n])}); err != nil {
				return err
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			if stream.Context().Err() != nil {
				return stream.Context().Err()
			}
			return status.UnavailableErrorf("reading logs: %s", err)
		}
	}
}

func timestampOrNil(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

func resourceProto(r summaries.ResourceType) *atlaspb.ResourceType {
	return &atlaspb.ResourceType{
		Cluster:    r.Cluster,
		Group:      r.Group,
		Version:    r.Version,
		Resource:   r.Resource,
		Kind:       r.Kind,
		Namespaced: r.Namespaced,
	}
}

func healthProto(h summaries.Health) atlaspb.Health {
	switch h {
	case summaries.HealthOK:
		return atlaspb.Health_HEALTH_OK
	case summaries.HealthWarn:
		return atlaspb.Health_HEALTH_WARN
	case summaries.HealthBad:
		return atlaspb.Health_HEALTH_BAD
	}
	return atlaspb.Health_HEALTH_UNKNOWN
}

func entryProto(e *summaries.Entry) *atlaspb.Entry {
	if e == nil {
		return nil
	}
	p := &atlaspb.Entry{
		Cluster:    e.Cluster,
		Group:      e.Group,
		Version:    e.Version,
		Resource:   e.Resource,
		Kind:       e.Kind,
		Namespace:  e.Namespace,
		Name:       e.Name,
		Uid:        e.UID,
		Created:    timestampOrNil(e.Created),
		Labels:     e.Labels,
		Owner:      e.Owner,
		Phase:      e.Phase,
		Health:     healthProto(e.Health),
		Ready:      e.Ready,
		Restarts:   e.Restarts,
		Node:       e.Node,
		Ips:        e.IPs,
		Images:     e.Images,
		Containers: e.Containers,
		Selector:   e.Selector,
		Extra:      e.Extra,
	}
	for _, port := range e.Ports {
		p.Ports = append(p.Ports, &atlaspb.Port{
			Name:     port.Name,
			Port:     port.Port,
			Protocol: port.Protocol,
			NodePort: port.NodePort,
		})
	}
	return p
}

func entriesProto(es []*summaries.Entry) []*atlaspb.Entry {
	out := make([]*atlaspb.Entry, 0, len(es))
	for _, e := range es {
		out = append(out, entryProto(e))
	}
	return out
}

type relations struct {
	Owners    []*summaries.Entry
	Node      *summaries.Entry
	Pods      []*summaries.Entry
	PodsTotal int
	Jobs      []*summaries.Entry
	Services  []*summaries.Entry
}

func relationsProto(rel *relations) *atlaspb.Relations {
	return &atlaspb.Relations{
		Owners:    entriesProto(rel.Owners),
		Node:      entryProto(rel.Node),
		Pods:      entriesProto(rel.Pods),
		PodsTotal: int32(rel.PodsTotal),
		Jobs:      entriesProto(rel.Jobs),
		Services:  entriesProto(rel.Services),
	}
}

// findByKind returns the indexed entry of the given kind, matching namespace
// too unless ns is empty.
func (s *AtlasService) findByKind(clusterName, kind, ns, name string) *summaries.Entry {
	var found *summaries.Entry
	s.c.Index().Scan(clusterName, kind, func(e *summaries.Entry) {
		if e.Name == name && (ns == "" || e.Namespace == ns) {
			found = e
		}
	})
	return found
}

// scanEntries collects entries of a kind matching the filter, sorted by name.
func (s *AtlasService) scanEntries(clusterName, kind string, filter func(*summaries.Entry) bool) []*summaries.Entry {
	var out []*summaries.Entry
	s.c.Index().Scan(clusterName, kind, func(e *summaries.Entry) {
		if filter(e) {
			out = append(out, e)
		}
	})
	slices.SortFunc(out, func(a, b *summaries.Entry) int {
		return cmp.Or(cmp.Compare(a.Namespace, b.Namespace), cmp.Compare(a.Name, b.Name))
	})
	return out
}

func ownerRef(e *summaries.Entry) string { return e.Kind + "/" + e.Name }

// podsOwnedBy returns pods whose controller is one of owners ("Kind/name").
func (s *AtlasService) podsOwnedBy(clusterName, ns string, owners ...string) []*summaries.Entry {
	set := map[string]bool{}
	for _, o := range owners {
		set[o] = true
	}
	return s.scanEntries(clusterName, "Pod", func(p *summaries.Entry) bool {
		return p.Namespace == ns && set[p.Owner]
	})
}

func (s *AtlasService) relationsFor(e *summaries.Entry) *relations {
	rel := &relations{}

	// Walk the ownership chain upward (Pod -> ReplicaSet -> Deployment).
	// 4 is an arbitrary cap to ensure the loop terminates. We don't expect any
	// proper resource to get there.
	cur := e
	for range 4 {
		if cur.Owner == "" {
			break
		}
		kind, name, ok := strings.Cut(cur.Owner, "/")
		if !ok {
			break
		}
		owner := s.findByKind(e.Cluster, kind, cur.Namespace, name)
		if owner == nil {
			break
		}
		rel.Owners = append(rel.Owners, owner)
		cur = owner
	}

	switch e.Kind {
	case "Pod":
		if e.Node != "" {
			rel.Node = s.findByKind(e.Cluster, "Node", "", e.Node)
		}
		rel.Services = s.scanEntries(e.Cluster, "Service", func(svc *summaries.Entry) bool {
			return svc.Namespace == e.Namespace && summaries.SelectorMatches(svc.Selector, e.Labels)
		})
	case "ReplicaSet", "StatefulSet", "DaemonSet", "Job":
		rel.Pods = s.podsOwnedBy(e.Cluster, e.Namespace, ownerRef(e))
	case "Deployment":
		replicaSets := s.scanEntries(e.Cluster, "ReplicaSet", func(rs *summaries.Entry) bool {
			return rs.Namespace == e.Namespace && rs.Owner == ownerRef(e)
		})
		owners := make([]string, len(replicaSets))
		for i, rs := range replicaSets {
			owners[i] = ownerRef(rs)
		}
		rel.Pods = s.podsOwnedBy(e.Cluster, e.Namespace, owners...)
	case "CronJob":
		rel.Jobs = s.scanEntries(e.Cluster, "Job", func(j *summaries.Entry) bool {
			return j.Namespace == e.Namespace && j.Owner == ownerRef(e)
		})
		owners := make([]string, len(rel.Jobs))
		for i, j := range rel.Jobs {
			owners[i] = ownerRef(j)
		}
		rel.Pods = s.podsOwnedBy(e.Cluster, e.Namespace, owners...)
	case "Service":
		rel.Pods = s.scanEntries(e.Cluster, "Pod", func(p *summaries.Entry) bool {
			return p.Namespace == e.Namespace && summaries.SelectorMatches(e.Selector, p.Labels)
		})
	case "Node":
		rel.Pods = s.scanEntries(e.Cluster, "Pod", func(p *summaries.Entry) bool {
			return p.Node == e.Name
		})
	}

	rel.PodsTotal = len(rel.Pods)
	if len(rel.Pods) > maxRelatedPods {
		rel.Pods = rel.Pods[:maxRelatedPods]
	}
	return rel
}

// hasHeadlessService reports whether the index contains the given headless service.
func hasHeadlessService(ix *summaries.Index, cluster, namespace, name string) bool {
	svc, ok := ix.GetEntry(cluster, "", "services", namespace, name)
	return ok && svc.Phase == "ClusterIP" && len(svc.IPs) == 0
}

// dashedIP renders an IP the way pod DNS records expect ("10-2-3-4").
func dashedIP(ip string) string {
	return strings.NewReplacer(".", "-", ":", "-").Replace(ip)
}

// portLinks renders the connectable ports of an object as tunnel DNS names.
//
// Pod ports use the dashed-IP pod record ("10-2-3-4.ns.<pod_zone>"), service
// ports the service record ("name.ns.<svc_zone>"). Both resolve only through
// a tunnel, which rewrites the zone suffix back to pod/svc.cluster.local and
// relays via that cluster's gateway.
func portLinks(c *cluster.Cluster, e *summaries.Entry, rel *relations) []*atlaspb.PortLink {
	svcZone, podZone := c.Zones()
	// Ports first, then the pages some of them are known to serve.
	var out, pages []*atlaspb.PortLink
	// Check if entry represents a BuildBuddy server or something (e.g. service)
	// in front of a BuildBuddy server.
	isBuildBuddyServer := isBuildBuddyImage(e) || (rel != nil && slices.ContainsFunc(rel.Pods, isBuildBuddyImage))

	addPort := func(p summaries.Port, host string, via atlaspb.PortLink_Via, viaName string) {
		l := &atlaspb.PortLink{
			Name:     p.Name,
			Port:     p.Port,
			Protocol: p.Protocol,
			Host:     host,
			Via:      via,
			ViaName:  viaName,
		}
		scheme := guessScheme(p)
		if host != "" {
			l.HostPort = fmt.Sprintf("%s:%d", host, p.Port)
			if scheme != "" {
				l.Url = fmt.Sprintf("%s://%s", scheme, l.HostPort)
			}
		}
		out = append(out, l)
		// A port known to serve debug pages also gets one link per page,
		// named by the page, over the port's scheme (plain http unless the
		// port says otherwise).
		if known := debugPages(p, isBuildBuddyServer); len(known) > 0 && host != "" {
			if scheme == "" {
				scheme = "http"
			}
			for _, pg := range known {
				pages = append(pages, &atlaspb.PortLink{
					Name:     pg.name,
					Port:     p.Port,
					Protocol: p.Protocol,
					Host:     host,
					HostPort: l.HostPort,
					Url:      scheme + "://" + l.HostPort + pg.path,
					Via:      via,
					ViaName:  viaName,
					Path:     pg.path,
				})
			}
		}
	}

	switch e.Kind {
	case "Pod":
		host := ""
		switch {
		case e.Hostname != "" && e.Subdomain != "" && svcZone != "" && e.Namespace != "" &&
			hasHeadlessService(c.Index(), e.Cluster, e.Namespace, e.Subdomain):
			// If a pod is part of a headless statefulset service, return the
			// stable pod name DNS instead of an unstable IP reference. The
			// record exists only while that headless service does.
			host = e.Hostname + "." + e.Subdomain + "." + e.Namespace + "." + svcZone
		case podZone != "" && len(e.IPs) > 0 && e.Namespace != "":
			host = dashedIP(e.IPs[0]) + "." + e.Namespace + "." + podZone
		}
		for _, p := range e.Ports {
			addPort(p, host, atlaspb.PortLink_POD, "")
		}
		if svcZone != "" && rel != nil {
			for _, svc := range rel.Services {
				for _, p := range svc.Ports {
					addPort(p, svc.Name+"."+svc.Namespace+"."+svcZone, atlaspb.PortLink_SERVICE, svc.Name)
				}
			}
		}
	case "Service":
		host := ""
		if svcZone != "" && e.Namespace != "" {
			host = e.Name + "." + e.Namespace + "." + svcZone
		}
		for _, p := range e.Ports {
			addPort(p, host, atlaspb.PortLink_SERVICE, "")
		}
	}
	return append(out, pages...)
}

type debugPage struct{ name, path string }

// buildBuddyDebugPages are the standard handlers on BuildBuddy servers.
// For now, we use a simple heuristic to display these links. In the future, it
// might make sense to drive this via k8s annotations.
var buildBuddyDebugPages = []debugPage{
	{"statusz", "/statusz"},
	{"metrics", "/metrics"},
	{"pprof", "/debug/pprof/"},
	{"flagz", "/flagz"},
	{"rpcz", "/rpcz"},
	{"channelz", "/channelz/"},
}

// debugPages lists the debug pages a port is known to serve.
// isBuildBuddyServer indicates if the port belongs to a BuildBuddy server.
func debugPages(p summaries.Port, isBuildBuddyServer bool) []debugPage {
	if !isTCPPort(p) {
		return nil
	}
	name := strings.ToLower(p.Name)
	if (name == "monitoring" || p.Port == 9090) && isBuildBuddyServer {
		return buildBuddyDebugPages
	}
	if strings.Contains(name, "metrics") || strings.HasPrefix(name, "prom") {
		return []debugPage{{"metrics", "/metrics"}}
	}
	return nil
}

func isTCPPort(p summaries.Port) bool {
	return p.Protocol == "" || p.Protocol == "TCP"
}

func isBuildBuddyImage(e *summaries.Entry) bool {
	for _, img := range e.Images {
		if strings.Contains(img, "buildbuddy") {
			return true
		}
	}
	return false
}

// guessScheme decides whether a port is worth a clickable browser link, from
// its name first and well-known numbers second. Everything else still gets a
// copyable host:port.
func guessScheme(p summaries.Port) string {
	if !isTCPPort(p) {
		return ""
	}
	name := strings.ToLower(p.Name)
	if name == "tls" || strings.HasPrefix(name, "https") {
		return "https"
	}
	for _, prefix := range []string{"http", "web", "ui", "admin", "metrics", "debug", "pprof", "prom", "dash"} {
		if strings.HasPrefix(name, prefix) {
			return "http"
		}
	}
	switch p.Port {
	case 443, 8443:
		return "https"
	case 80, 8080, 8081, 8000, 3000, 5000, 9090, 9091, 9100, 15672, 16686, 8888:
		return "http"
	}
	return ""
}
