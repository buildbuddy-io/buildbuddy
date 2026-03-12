package egress

import (
	"bytes"
	"context"
	"encoding/csv"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/lru"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
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
	mu       sync.Mutex
	cache    *lru.LRU[destination]
}

type statsHandler struct {
	classifier *classifier
}

// Context keys for storing peer information in context.
type connDestinationKey struct{}
type rpcMetricLabelsKey struct{}

type rpcMetricLabels struct {
	groupID  string
	provider string
	region   string
}

var (
	//go:embed aws/aws.csv
	awsRangesCSV []byte

	//go:embed azure/azure.csv
	azureRangesCSV []byte

	//go:embed gcp/gcp.csv
	gcpRangesCSV []byte

	//go:embed github/github.csv
	githubRangesCSV []byte

	//go:embed macstadium/macstadium.csv
	macstadiumRangesCSV []byte

	metalRangesCSV []byte = []byte(`Metal,us-sjc,23.176.168.0/24
Metal,us-sjc,216.226.68.0/22
`)
	classifierOnce = sync.OnceValues(newClassifier)
)

// NewStatsHandler returns a gRPC server stats handler that classifies response
// bytes by destination cloud provider and region using the embedded IP ranges.
func NewStatsHandler() (stats.Handler, error) {
	classifier, err := classifierOnce()
	if err != nil {
		return nil, err
	}
	return &statsHandler{classifier: classifier}, nil
}

func (h *statsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	if info == nil {
		return ctx
	}
	return context.WithValue(ctx, connDestinationKey{}, h.classifier.classify(info.RemoteAddr))
}

func (*statsHandler) HandleConn(context.Context, stats.ConnStats) {}

func (h *statsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	labels := rpcMetricLabels{
		groupID:  unknownGroupID,
		provider: otherProvider,
		region:   unknownRegion,
	}
	if c, err := claims.ClaimsFromContext(ctx); err == nil && c.GetGroupID() != "" {
		labels.groupID = c.GetGroupID()
	}
	if destination, ok := ctx.Value(connDestinationKey{}).(destination); ok {
		labels.provider = destination.provider
		labels.region = destination.region
	}
	return context.WithValue(ctx, rpcMetricLabelsKey{}, labels)
}

func (h *statsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	switch st := s.(type) {
	case *stats.OutPayload:
		if st.IsClient() || st.WireLength <= 0 {
			return
		}
		h.record(ctx, st.WireLength)
	}
}

func (h *statsHandler) record(ctx context.Context, wireLength int) {
	labels := rpcMetricLabels{
		groupID:  unknownGroupID,
		provider: otherProvider,
		region:   unknownRegion,
	}
	if v, ok := ctx.Value(rpcMetricLabelsKey{}).(rpcMetricLabels); ok {
		labels = v
	}
	metrics.GRPCEgressBytes.WithLabelValues(labels.groupID, labels.provider, labels.region).Add(float64(wireLength))
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
	cache, err := lru.NewLRU(&lru.Config[destination]{
		MaxSize: cacheMaxSize,
		SizeFn:  func(destination) int64 { return 1 },
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

func (c *classifier) classify(addr net.Addr) destination {
	ip, ok := addrFromNetAddr(addr)
	if !ok {
		return destination{provider: otherProvider, region: unknownRegion}
	}
	cacheKey := ip.String()
	c.mu.Lock()
	value, ok := c.cache.Get(cacheKey)
	c.mu.Unlock()
	if ok {
		return value
	}
	for _, entry := range c.ipRanges {
		if entry.prefix.Contains(ip) {
			c.mu.Lock()
			c.cache.Add(cacheKey, entry.destination)
			c.mu.Unlock()
			return entry.destination
		}
	}
	unknown := destination{provider: otherProvider, region: unknownRegion}
	c.mu.Lock()
	c.cache.Add(cacheKey, unknown)
	c.mu.Unlock()
	return unknown
}

func addrFromNetAddr(addr net.Addr) (netip.Addr, bool) {
	if addr == nil {
		return netip.Addr{}, false
	}
	switch a := addr.(type) {
	case *net.TCPAddr:
		if ip, ok := netip.AddrFromSlice(a.IP); ok {
			return ip.Unmap(), true
		}
	case *net.UDPAddr:
		if ip, ok := netip.AddrFromSlice(a.IP); ok {
			return ip.Unmap(), true
		}
	}
	if addrPort, err := netip.ParseAddrPort(addr.String()); err == nil {
		return addrPort.Addr().Unmap(), true
	}
	if ip, err := netip.ParseAddr(addr.String()); err == nil {
		return ip.Unmap(), true
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		return netip.Addr{}, false
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return netip.Addr{}, false
	}
	return ip.Unmap(), true
}
