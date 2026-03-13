package trafficstats

import (
	"context"
	"fmt"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/stats"
)

func TestStatsHandler_RecordsEgressByDestination(t *testing.T) {
	handler, err := NewServerHandler()
	if err != nil {
		t.Fatalf("NewStatsHandler() returned error: %v", err)
	}

	testCases := []struct {
		name     string
		ip       string
		groupID  string
		provider string
		region   string
	}{
		{
			name:     "aws",
			ip:       "3.4.12.4",
			groupID:  "GR123",
			provider: "aws",
			region:   "eu-west-1",
		},
		{
			name:     "gcp",
			ip:       "34.80.0.1",
			groupID:  "GR456",
			provider: "gcp",
			region:   "asia-east1",
		},
		{
			name:     "azure",
			ip:       "4.198.32.1",
			groupID:  "GR234",
			provider: "azure",
			region:   "australiacentral2",
		},
		{
			name:     "github",
			ip:       "185.199.108.1",
			groupID:  "GR345",
			provider: "github",
			region:   "",
		},
		{
			name:     "macstadium",
			ip:       "208.52.145.1",
			groupID:  "GR567",
			provider: "macstadium",
			region:   "atlanta",
		},
		{
			name:     "metal",
			ip:       "23.176.168.1",
			groupID:  "GR678",
			provider: "metal",
			region:   "us-sjc",
		},
		{
			name:     "github_over_azure",
			ip:       "4.148.0.1", // In both GitHub 4.148.0.0/16 and Azure 4.148.0.0/16; GitHub should win.
			groupID:  "GR890",
			provider: "github",
			region:   "",
		},
		{
			name:     "ipv4_mapped_ipv6",
			ip:       "::ffff:3.4.12.4", // IPv4-mapped IPv6 should classify the same as the IPv4 address.
			groupID:  "GR012",
			provider: "aws",
			region:   "eu-west-1",
		},
		{
			name:     "private",
			ip:       "10.0.1.5",
			groupID:  "GR901",
			provider: "internal",
			region:   "",
		},
		{
			name:     "other",
			ip:       "203.0.113.10",
			groupID:  "GR789",
			provider: "other",
			region:   "unknown",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics.GRPCServerEgressBytes.Reset()

			ctx := peer.NewContext(context.Background(), &peer.Peer{
				Addr: &net.TCPAddr{IP: net.ParseIP(tc.ip), Port: 1985},
			})
			ctx = claims.AuthContext(ctx, &claims.Claims{GroupID: tc.groupID})
			ctx = handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/Test"})
			handler.HandleRPC(ctx, &stats.OutPayload{WireLength: 123})

			labels := prometheus.Labels{
				metrics.GroupID:                  tc.groupID,
				metrics.DestinationProviderLabel: tc.provider,
				metrics.DestinationRegionLabel:   tc.region,
			}
			if got := testmetrics.CounterValueForLabels(t, metrics.GRPCServerEgressBytes, labels); got != 123 {
				t.Fatalf("metric value = %v, want 123", got)
			}
		})
	}
}

func TestStatsHandler_RecordsIngressBySource(t *testing.T) {
	metrics.GRPCServerIngressBytes.Reset()

	handler, err := NewServerHandler()
	if err != nil {
		t.Fatalf("NewStatsHandler() returned error: %v", err)
	}

	ctx := context.WithValue(context.Background(), clientip.ContextKey, "3.4.12.4")
	ctx = claims.AuthContext(ctx, &claims.Claims{GroupID: "GR123"})
	ctx = handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/Test"})
	handler.HandleRPC(ctx, &stats.InPayload{WireLength: 456})

	labels := prometheus.Labels{
		metrics.GroupID:                  "GR123",
		metrics.DestinationProviderLabel: "aws",
		metrics.DestinationRegionLabel:   "eu-west-1",
	}
	if got := testmetrics.CounterValueForLabels(t, metrics.GRPCServerIngressBytes, labels); got != 456 {
		t.Fatalf("metric value = %v, want 456", got)
	}
}

func TestStatsHandler_IgnoresClientSidePayloads(t *testing.T) {
	metrics.GRPCServerEgressBytes.Reset()

	handler, err := NewServerHandler()
	if err != nil {
		t.Fatalf("NewStatsHandler() returned error: %v", err)
	}

	ctx := handler.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/ClientPayload"})
	handler.HandleRPC(ctx, &stats.OutPayload{Client: true, WireLength: 55})

	labels := prometheus.Labels{
		metrics.GroupID:                  unknownGroupID,
		metrics.DestinationProviderLabel: "other",
		metrics.DestinationRegionLabel:   "unknown",
	}
	if got := testmetrics.CounterValueForLabels(t, metrics.GRPCServerEgressBytes, labels); got != 0 {
		t.Fatalf("metric value = %v, want 0", got)
	}
}

func TestStatsHandler_NoPeerInfo(t *testing.T) {
	metrics.GRPCServerEgressBytes.Reset()

	handler, err := NewServerHandler()
	if err != nil {
		t.Fatalf("NewStatsHandler() returned error: %v", err)
	}

	// No peer info or clientip in context.
	ctx := handler.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/Test"})
	handler.HandleRPC(ctx, &stats.OutPayload{WireLength: 50})

	labels := prometheus.Labels{
		metrics.GroupID:                  unknownGroupID,
		metrics.DestinationProviderLabel: "other",
		metrics.DestinationRegionLabel:   "unknown",
	}
	if got := testmetrics.CounterValueForLabels(t, metrics.GRPCServerEgressBytes, labels); got != 50 {
		t.Fatalf("metric value = %v, want 50", got)
	}
}

func TestStatsHandler_NoClaims(t *testing.T) {
	metrics.GRPCServerEgressBytes.Reset()

	handler, err := NewServerHandler()
	if err != nil {
		t.Fatalf("NewStatsHandler() returned error: %v", err)
	}

	ctx := context.WithValue(context.Background(), clientip.ContextKey, "3.4.12.4")
	// No claims added to context.
	ctx = handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/Test"})
	handler.HandleRPC(ctx, &stats.OutPayload{WireLength: 75})

	labels := prometheus.Labels{
		metrics.GroupID:                  unknownGroupID,
		metrics.DestinationProviderLabel: "aws",
		metrics.DestinationRegionLabel:   "eu-west-1",
	}
	if got := testmetrics.CounterValueForLabels(t, metrics.GRPCServerEgressBytes, labels); got != 75 {
		t.Fatalf("metric value = %v, want 75", got)
	}
}

func BenchmarkClassifierClassify(b *testing.B) {
	b.Run("cached_hit", func(b *testing.B) {
		classifier := newBenchmarkClassifier(b)
		classifier.classify("3.4.12.4")

		b.ReportAllocs()
		for b.Loop() {
			classifier.classify("3.4.12.4")
		}
	})

	b.Run("varying_ips", func(b *testing.B) {
		classifier := newBenchmarkClassifier(b)

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			classifier.classify(benchmarkIP(i))
			i++
		}
	})
}

func newBenchmarkClassifier(b *testing.B) *classifier {
	b.Helper()
	classifier, err := newClassifier()
	if err != nil {
		b.Fatalf("newClassifier() returned error: %v", err)
	}
	return classifier
}

func benchmarkStatsHandler(b *testing.B) *statsHandler {
	return &statsHandler{classifier: newBenchmarkClassifier(b)}
}

func BenchmarkStatsHandler(b *testing.B) {
	rpcInfo := &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/Test"}
	payload := &stats.OutPayload{WireLength: 123}
	claimsVal := &claims.Claims{GroupID: "GR123"}
	ctx := claims.AuthContext(context.Background(), claimsVal)

	b.Run("cached_hit", func(b *testing.B) {
		handler := benchmarkStatsHandler(b)
		ctx := context.WithValue(ctx, clientip.ContextKey, "3.4.12.4")

		b.ReportAllocs()
		for b.Loop() {
			rpcCtx := handler.TagRPC(ctx, rpcInfo)
			handler.HandleRPC(rpcCtx, payload)
		}
	})
	b.Run("cached_hit_multiple_payloads", func(b *testing.B) {
		handler := benchmarkStatsHandler(b)
		ctx := context.WithValue(ctx, clientip.ContextKey, "3.4.12.4")

		b.ReportAllocs()
		for b.Loop() {
			rpcCtx := handler.TagRPC(ctx, rpcInfo)
			for range 10 {
				handler.HandleRPC(rpcCtx, payload)
			}
		}
	})
	b.Run("varying_ips", func(b *testing.B) {
		handler := benchmarkStatsHandler(b)
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			ctx := context.WithValue(ctx, clientip.ContextKey, benchmarkIP(i))
			i++
			rpcCtx := handler.TagRPC(ctx, rpcInfo)
			handler.HandleRPC(rpcCtx, payload)
		}
	})
}

func benchmarkIP(i int) string {
	return fmt.Sprintf("100.64.%d.%d", (i>>8)&0xff, i&0xff)
}

