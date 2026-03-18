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

func TestStatsHandler_RecordsTrafficByDestination(t *testing.T) {
	handler := newTestHandler(t)

	testCases := []struct {
		name     string
		ip       string
		groupID  string
		provider string
		region   string
		// useClientIP sets the IP via clientip.ContextKey instead of peer info.
		useClientIP bool
	}{
		{
			name:     "aws",
			ip:       "3.4.12.4",
			groupID:  "GR123",
			provider: "aws",
			region:   "eu-west-1",
		},
		{
			name:        "aws_clientip",
			ip:          "3.4.12.4",
			groupID:     "GR123",
			provider:    "aws",
			region:      "eu-west-1",
			useClientIP: true,
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
			name:        "github",
			ip:          "185.199.108.1",
			groupID:     "GR345",
			provider:    "github",
			region:      "",
			useClientIP: true,
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
			name:     "loopback",
			ip:       "127.0.0.1",
			groupID:  "GR902",
			provider: "loopback",
			region:   "",
		},
		{
			name:        "private",
			ip:          "10.0.1.5",
			groupID:     "GR901",
			provider:    "internal",
			region:      "",
			useClientIP: true,
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
			metrics.GRPCServerIngressBytes.Reset()

			// TagRPC runs before interceptors, so claims and clientip
			// are not yet in the context. Peer info is available.
			ctx := context.Background()
			if !tc.useClientIP {
				ctx = peer.NewContext(ctx, &peer.Peer{
					Addr: &net.TCPAddr{IP: net.ParseIP(tc.ip), Port: 1985},
				})
			}
			ctx = handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/Test"})
			// Interceptors add claims and clientip after TagRPC.
			ctx = claims.AuthContext(ctx, &claims.Claims{GroupID: tc.groupID})
			if tc.useClientIP {
				ctx = context.WithValue(ctx, clientip.ContextKey, tc.ip)
			}
			// Simulate the post-auth interceptor populating counters.
			handler.initCounters(ctx)
			handler.HandleRPC(ctx, &stats.OutPayload{WireLength: 100})
			handler.HandleRPC(ctx, &stats.InPayload{WireLength: 200})

			labels := prometheus.Labels{
				metrics.GroupID:                  tc.groupID,
				metrics.DestinationProviderLabel: tc.provider,
				metrics.DestinationRegionLabel:   tc.region,
			}
			if got := testmetrics.CounterValueForLabels(t, metrics.GRPCServerEgressBytes, labels); got != 100 {
				t.Fatalf("egress metric value = %v, want 100", got)
			}
			if got := testmetrics.CounterValueForLabels(t, metrics.GRPCServerIngressBytes, labels); got != 200 {
				t.Fatalf("ingress metric value = %v, want 200", got)
			}
		})
	}
}

func TestStatsHandler_IgnoresClientSidePayloads(t *testing.T) {
	metrics.GRPCServerEgressBytes.Reset()

	handler := newTestHandler(t)

	ctx := context.WithValue(context.Background(), clientip.ContextKey, "3.4.12.4")
	ctx = handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/ClientPayload"})
	handler.initCounters(ctx)
	handler.HandleRPC(ctx, &stats.OutPayload{Client: true, WireLength: 55})

	labels := prometheus.Labels{
		metrics.GroupID:                  unknownGroupID,
		metrics.DestinationProviderLabel: "aws",
		metrics.DestinationRegionLabel:   "eu-west-1",
	}
	if got := testmetrics.CounterValueForLabels(t, metrics.GRPCServerEgressBytes, labels); got != 0 {
		t.Fatalf("metric value = %v, want 0", got)
	}
}

func TestStatsHandler_NoPeerInfo(t *testing.T) {
	metrics.GRPCServerEgressBytes.Reset()

	handler := newTestHandler(t)

	// No peer info or clientip in context.
	ctx := handler.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/Test"})
	handler.initCounters(ctx)
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

	handler := newTestHandler(t)

	// No claims added to context.
	ctx := handler.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/Test"})
	ctx = context.WithValue(ctx, clientip.ContextKey, "3.4.12.4")
	handler.initCounters(ctx)
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
		classifier := newTestClassifier(b)
		classifier.classify("3.4.12.4")

		b.ReportAllocs()
		for b.Loop() {
			classifier.classify("3.4.12.4")
		}
	})

	b.Run("varying_ips", func(b *testing.B) {
		classifier := newTestClassifier(b)

		b.ReportAllocs()
		i := 0
		for b.Loop() {
			classifier.classify(benchmarkIP(i))
			i++
		}
	})
}

func newTestClassifier(t testing.TB) *classifier {
	t.Helper()
	classifier, err := newClassifier()
	if err != nil {
		t.Fatalf("newClassifier() returned error: %v", err)
	}
	return classifier
}

func newTestHandler(t testing.TB) *StatsHandler {
	t.Helper()
	return &StatsHandler{classifier: newTestClassifier(t)}
}

func benchmarkIP(i int) string {
	return fmt.Sprintf("100.64.%d.%d", (i>>8)&0xff, i&0xff)
}

func BenchmarkStatsHandler(b *testing.B) {
	rpcInfo := &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/Test"}
	payload := &stats.OutPayload{WireLength: 123}
	claimsVal := &claims.Claims{GroupID: "GR123"}
	ctx := claims.AuthContext(context.Background(), claimsVal)

	b.Run("cached_hit", func(b *testing.B) {
		handler := newTestHandler(b)
		ctx := context.WithValue(ctx, clientip.ContextKey, "3.4.12.4")

		b.ReportAllocs()
		for b.Loop() {
			rpcCtx := handler.TagRPC(ctx, rpcInfo)
			handler.initCounters(rpcCtx)
			handler.HandleRPC(rpcCtx, payload)
		}
	})
	b.Run("cached_hit_multiple_payloads", func(b *testing.B) {
		handler := newTestHandler(b)
		ctx := context.WithValue(ctx, clientip.ContextKey, "3.4.12.4")

		b.ReportAllocs()
		for b.Loop() {
			rpcCtx := handler.TagRPC(ctx, rpcInfo)
			handler.initCounters(rpcCtx)
			for range 10 {
				handler.HandleRPC(rpcCtx, payload)
			}
		}
	})
	b.Run("varying_ips", func(b *testing.B) {
		handler := newTestHandler(b)
		b.ReportAllocs()
		i := 0
		for b.Loop() {
			ctx := context.WithValue(ctx, clientip.ContextKey, benchmarkIP(i))
			i++
			rpcCtx := handler.TagRPC(ctx, rpcInfo)
			handler.initCounters(rpcCtx)
			handler.HandleRPC(rpcCtx, payload)
		}
	})
}
