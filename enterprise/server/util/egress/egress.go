package egress

import (
	"bytes"
	"context"
	"encoding/csv"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"google.golang.org/grpc/stats"

	_ "embed"
)

const (
	otherProvider  = "other"
	unknownGroupID = "unknown"
	unknownRegion  = "unknown"
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
	cache    sync.Map
}

type grpcStatsHandler struct {
	classifier *classifier
}

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

	//go:embed gcp/gcp.csv
	gcpRangesCSV []byte

	classifierOnce sync.Once
	cachedMatcher  *classifier
	cachedErr      error
)

// NewStatsHandler returns a gRPC server stats handler that classifies response
// bytes by destination cloud provider and region using the embedded IP ranges.
func NewStatsHandler() (stats.Handler, error) {
	classifierOnce.Do(func() {
		cachedMatcher, cachedErr = newClassifier()
	})
	if cachedErr != nil {
		return nil, cachedErr
	}
	return &grpcStatsHandler{classifier: cachedMatcher}, nil
}

func (h *grpcStatsHandler) TagConn(ctx context.Context, info *stats.ConnTagInfo) context.Context {
	if info == nil {
		return ctx
	}
	return context.WithValue(ctx, connDestinationKey{}, h.classifier.classify(info.RemoteAddr))
}

func (*grpcStatsHandler) HandleConn(context.Context, stats.ConnStats) {}

func (h *grpcStatsHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
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

func (h *grpcStatsHandler) HandleRPC(ctx context.Context, s stats.RPCStats) {
	switch st := s.(type) {
	case *stats.OutPayload:
		if st.IsClient() || st.WireLength <= 0 {
			return
		}
		h.record(ctx, st.WireLength)
	case *stats.OutTrailer:
		if st.IsClient() || st.WireLength <= 0 {
			return
		}
		h.record(ctx, st.WireLength)
	}
}

func (h *grpcStatsHandler) record(ctx context.Context, wireLength int) {
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
	ranges, err := parseRangeEntries(awsRangesCSV)
	if err != nil {
		return nil, status.WrapError(err, "parse AWS egress ranges")
	}
	gcpRanges, err := parseRangeEntries(gcpRangesCSV)
	if err != nil {
		return nil, status.WrapError(err, "parse GCP egress ranges")
	}
	ranges = append(ranges, gcpRanges...)
	sort.Slice(ranges, func(i, j int) bool {
		return ranges[i].prefix.Bits() > ranges[j].prefix.Bits()
	})
	return &classifier{ipRanges: ranges}, nil
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
	start := 0

	entries := make([]ipRange, 0, len(records)-start)
	for i, record := range records[start:] {
		if len(record) != 3 {
			return nil, status.InternalErrorf("invalid egress CSV record at row %d: got %d columns, want 3", i+start+1, len(record))
		}
		provider := strings.ToLower(strings.TrimSpace(record[0]))
		region := strings.TrimSpace(record[1])
		prefix, err := netip.ParsePrefix(strings.TrimSpace(record[2]))
		if err != nil {
			return nil, status.WrapErrorf(err, "parse prefix at row %d", i+start+1)
		}
		if provider == "" {
			provider = otherProvider
		}
		if region == "" {
			region = unknownRegion
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
	if value, ok := c.cache.Load(cacheKey); ok {
		return value.(destination)
	}
	for _, entry := range c.ipRanges {
		if entry.prefix.Contains(ip) {
			c.cache.Store(cacheKey, entry.destination)
			return entry.destination
		}
	}
	unknown := destination{provider: otherProvider, region: unknownRegion}
	c.cache.Store(cacheKey, unknown)
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
