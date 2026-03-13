package trafficstats

import (
	"bytes"
	"context"
	"encoding/csv"
	"net/netip"
	"slices"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus"
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
	ipRanges []ipRange
	cache    lru.LRU[destination]
}

type statsHandler struct {
	classifier *classifier
}

type rpcCountersKey struct{}

type rpcCounters struct {
	egress  prometheus.Counter
	ingress prometheus.Counter
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

// NewServerHandler returns a gRPC server stats handler that classifies
// bytes by destination cloud provider and region using the embedded IP ranges.
func NewServerHandler() (stats.Handler, error) {
	classifier, err := classifierOnce()
	if err != nil {
		return nil, err
	}
	return &statsHandler{classifier: classifier}, nil
}

func (h *statsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	return ctx
}

func (*statsHandler) HandleConn(context.Context, stats.ConnStats) {}

func (h *statsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	groupID := unknownGroupID
	if c, err := claims.ClaimsFromContext(ctx); err == nil && c.GetGroupID() != "" {
		groupID = c.GetGroupID()
	}
	ip := clientip.Get(ctx)
	if ip == "" {
		if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
			ip = p.Addr.String()
		}
	}
	dest := h.classifier.classify(ip)
	return context.WithValue(ctx, rpcCountersKey{}, &rpcCounters{
		egress:  metrics.GRPCServerEgressBytes.WithLabelValues(groupID, dest.provider, dest.region),
		ingress: metrics.GRPCServerIngressBytes.WithLabelValues(groupID, dest.provider, dest.region),
	})
}

func (h *statsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	switch st := s.(type) {
	case *stats.InPayload:
		if !st.IsClient() && st.WireLength > 0 {
			if c, ok := ctx.Value(rpcCountersKey{}).(*rpcCounters); ok {
				c.ingress.Add(float64(st.WireLength))
			}
		}
	case *stats.OutPayload:
		if !st.IsClient() && st.WireLength > 0 {
			if c, ok := ctx.Value(rpcCountersKey{}).(*rpcCounters); ok {
				c.egress.Add(float64(st.WireLength))
			}
		}
	}
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
	return &classifier{ipRanges: ranges, cache: cache}, nil
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
	unknown := destination{provider: otherProvider, region: unknownRegion}
	c.cache.Add(cacheKey, unknown)
	return unknown
}
