// Package trafficstats classifies gRPC traffic by cloud provider and region
// using embedded IP range data, and records per-RPC byte counts as Prometheus
// metrics labeled with the caller's group ID, cloud provider, and region.
//
// It requires both a [stats.Handler] and post-auth interceptors because gRPC
// stats handlers and interceptors operate on separate context chains. The
// stats handler's [stats.Handler.TagRPC] creates the context used by
// [stats.Handler.HandleRPC], but interceptors modify a different copy of
// the context, so values set by interceptors (like authenticated claims and
// client IP) are not visible in HandleRPC.
//
// To bridge this gap, TagRPC stores a pointer to a mutable [rpcCounters]
// struct in the context. The interceptors, which must run after auth
// has populated claims and client IP, look up that same pointer and initialize
// its dimensions. HandleRPC increments the counters and exports metrics.
package trafficstats

import (
	"bytes"
	"context"
	"encoding/csv"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/alert"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"

	_ "embed"
)

const (
	unknownGroupID = "unknown"
	otherProvider  = "other"
	unknownRegion  = "unknown"
	cacheMaxSize   = 100_000
)

type destination struct {
	provider string
	region   string
}

type ipRange struct {
	prefix      netip.Prefix
	destination destination
}

type classifier struct {
	ipRanges          []ipRange
	cache             lru.LRU[destination]
	occasionallLogger log.Logger
}

type StatsHandler struct {
	classifier *classifier
}

type rpcCountersKey struct{}

type rpcCounters struct {
	groupID                   string
	dest                      destination
	ingressBytes, egressBytes int
}

var (
	//go:embed data/aws.csv
	awsRangesCSV []byte

	//go:embed data/azure.csv
	azureRangesCSV []byte

	//go:embed data/gcp.csv
	gcpRangesCSV []byte

	//go:embed data/github.csv
	githubRangesCSV []byte

	//go:embed data/macstadium.csv
	macstadiumRangesCSV []byte

	metalRangesCSV []byte = []byte(`Metal,us-sjc,23.176.168.0/24
Metal,us-sjc,216.226.68.0/22
`)
	classifierOnce = sync.OnceValues(newClassifier)
)

// NewServerHandler returns the gRPC stats.Handler for traffic classification.
// It's UnaryInterceptor and StreamInterceptor methods must be installed in the
// gRPC server after the auth and clientip interceptors, so that it can access
// the claims and client IP for traffic classification.
func NewServerHandler() (*StatsHandler, error) {
	classifier, err := classifierOnce()
	if err != nil {
		return nil, err
	}
	return &StatsHandler{classifier: classifier}, nil
}

func (h *StatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*StatsHandler) HandleConn(context.Context, stats.ConnStats) {}

func (h *StatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	return context.WithValue(ctx, rpcCountersKey{}, &rpcCounters{groupID: "unset", dest: destination{provider: "unset", region: "unset"}})
}

func (h *StatsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	if s.IsClient() {
		return
	}
	var ingress, egress int
	var end bool
	switch st := s.(type) {
	case *stats.InPayload:
		ingress = st.WireLength
	case *stats.OutPayload:
		egress = st.WireLength
	case *stats.End:
		end = true
	}
	if egress > 0 || ingress > 0 || end {
		c, ok := ctx.Value(rpcCountersKey{}).(*rpcCounters)
		if !ok {
			alert.CtxUnexpectedEvent(ctx, "trafficstats_no_counters_in_handleprc", "They should be set in TagRPC.")
			return
		}
		c.ingressBytes += ingress
		c.egressBytes += egress
		if end {
			// Exporting metrics has to happen at the end of the RPC to make
			// that the interceptor populated the dimensions. For unary RPCs,
			// intercetors run after InPayload.
			if c.groupID == "unset" || c.dest.provider == "unset" {
				alert.CtxUnexpectedEvent(ctx, "trafficstats_unset_dimensions_at_end", "Maybe you forgot to install the interceptor? %v", c)
			}
			if c.ingressBytes > 0 {
				metrics.GRPCServerIngressBytes.WithLabelValues(c.groupID, c.dest.provider, c.dest.region).Add(float64(c.ingressBytes))
			}
			if c.egressBytes > 0 {
				metrics.GRPCServerEgressBytes.WithLabelValues(c.groupID, c.dest.provider, c.dest.region).Add(float64(c.egressBytes))
			}
		}
	}
}

// UnaryInterceptor populates the rpcCounters with metric labels derived
// from claims and client IP, which are available after auth interceptors.
func (h *StatsHandler) UnaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	h.initCounters(ctx)
	return handler(ctx, req)
}

// StreamInterceptor populates the rpcCounters with metric labels derived
// from claims and client IP, which are available after auth interceptors.
func (h *StatsHandler) StreamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	h.initCounters(ss.Context())
	return handler(srv, ss)
}

func (h *StatsHandler) initCounters(ctx context.Context) {
	c, ok := ctx.Value(rpcCountersKey{}).(*rpcCounters)
	if !ok {
		alert.CtxUnexpectedEvent(ctx, "trafficstats_no_counters_in_interceptor", "Maybe you forgot to install the StatsHandler?")
		return
	}
	c.groupID = unknownGroupID
	if cl, err := claims.ClaimsFromContext(ctx); err == nil && cl.GetGroupID() != "" {
		c.groupID = cl.GetGroupID()
	}
	ip := clientip.Get(ctx)
	if ip == "" {
		if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
			ip = p.Addr.String()
		}
	}
	c.dest = h.classifier.classify(ip)
}

func newClassifier() (*classifier, error) {
	var ranges []ipRange
	for _, csv := range []struct {
		name string
		data []byte
	}{
		{"GitHub", githubRangesCSV}, // GitHub needs to be before Azure because some GitHub IPs are subsets of Azure IPs.
		{"AWS", awsRangesCSV},
		{"Azure", azureRangesCSV},
		{"GCP", gcpRangesCSV},
		{"MacStadium", macstadiumRangesCSV},
		{"Metal", metalRangesCSV},
	} {
		entries, err := parseRangeEntries(csv.data)
		if err != nil {
			return nil, status.WrapErrorf(err, "parse %s egress ranges", csv.name)
		}
		slices.SortFunc(entries, func(a, b ipRange) int {
			return b.prefix.Bits() - a.prefix.Bits()
		})
		ranges = append(ranges, entries...)
	}
	cache, err := lru.New(&lru.Config[destination]{
		MaxSize:    cacheMaxSize,
		SizeFn:     func(destination) int64 { return 1 },
		ThreadSafe: true,
	})
	if err != nil {
		return nil, status.WrapError(err, "create egress classifier cache")
	}
	return &classifier{ipRanges: ranges, cache: cache, occasionallLogger: log.NamedSubLogger("trafficstats").EveryDuration(time.Minute)}, nil
}

func parseRangeEntries(csvBytes []byte) ([]ipRange, error) {
	r := csv.NewReader(bytes.NewReader(csvBytes))
	records, err := r.ReadAll()
	if err != nil {
		return nil, status.WrapError(err, "read egress range CSV")
	}
	if len(records) == 0 {
		return nil, nil
	}

	entries := make([]ipRange, 0, len(records))
	for i, record := range records {
		if len(record) != 3 {
			return nil, status.InternalErrorf("invalid egress CSV record at row %d: got %d columns, want 3", i+1, len(record))
		}
		provider := strings.ToLower(strings.TrimSpace(record[0]))
		region := strings.TrimSpace(record[1])
		prefix, err := netip.ParsePrefix(strings.TrimSpace(record[2]))
		if err != nil {
			return nil, status.WrapErrorf(err, "parse prefix at row %d", i+1)
		}
		if provider == "" {
			provider = "empty provider"
		}
		entries = append(entries, ipRange{
			prefix: prefix.Masked(),
			destination: destination{
				provider: provider,
				region:   region,
			},
		})
	}
	return entries, nil
}

func (c *classifier) classify(ipStr string) destination {
	ip, err := netip.ParseAddr(ipStr)
	if err != nil {
		// Try host:port format.
		if addrPort, err := netip.ParseAddrPort(ipStr); err == nil {
			ip = addrPort.Addr()
		} else {
			return destination{provider: otherProvider, region: unknownRegion}
		}
	}
	ip = ip.Unmap()
	if ip.IsLoopback() {
		return destination{provider: "loopback", region: ""}
	}
	if ip.IsPrivate() {
		return destination{provider: "internal", region: ""}
	}
	cacheKey := ip.String()
	value, ok := c.cache.Get(cacheKey)
	if ok {
		return value
	}
	for _, entry := range c.ipRanges {
		if entry.prefix.Contains(ip) {
			c.cache.Add(cacheKey, entry.destination)
			return entry.destination
		}
	}
	c.occasionallLogger.Infof("traffic classifier didn't find IP: %s", ip)
	unknown := destination{provider: otherProvider, region: unknownRegion}
	c.cache.Add(cacheKey, unknown)
	return unknown
}
