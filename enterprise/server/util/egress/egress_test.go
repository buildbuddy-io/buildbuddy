package egress

import (
	"context"
	"net"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testmetrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/claims"
	"github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/stats"
)

func TestStatsHandler_RecordsEgressByDestination(t *testing.T) {
	handler, err := NewStatsHandler()
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
			name:     "other",
			ip:       "203.0.113.10",
			groupID:  "GR789",
			provider: "other",
			region:   "unknown",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			metrics.GRPCEgressBytes.Reset()

			ctx := handler.TagConn(context.Background(), &stats.ConnTagInfo{
				RemoteAddr: &net.TCPAddr{IP: net.ParseIP(tc.ip), Port: 1985},
			})
			ctx = claims.AuthContext(ctx, &claims.Claims{GroupID: tc.groupID})
			ctx = handler.TagRPC(ctx, &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/Test"})
			handler.HandleRPC(ctx, &stats.OutPayload{WireLength: 123})

			labels := prometheus.Labels{
				metrics.GroupID:                        tc.groupID,
				metrics.EgressDestinationProviderLabel: tc.provider,
				metrics.EgressDestinationRegionLabel:   tc.region,
			}
			if got := testmetrics.CounterValueForLabels(t, metrics.GRPCEgressBytes, labels); got != 123 {
				t.Fatalf("metric value = %v, want 123", got)
			}
		})
	}
}

func TestStatsHandler_IgnoresClientSidePayloads(t *testing.T) {
	metrics.GRPCEgressBytes.Reset()

	handler, err := NewStatsHandler()
	if err != nil {
		t.Fatalf("NewStatsHandler() returned error: %v", err)
	}

	ctx := handler.TagRPC(context.Background(), &stats.RPCTagInfo{FullMethodName: "/buildbuddy.service/ClientPayload"})
	handler.HandleRPC(ctx, &stats.OutPayload{Client: true, WireLength: 55})

	labels := prometheus.Labels{
		metrics.GroupID:                        unknownGroupID,
		metrics.EgressDestinationProviderLabel: "other",
		metrics.EgressDestinationRegionLabel:   "unknown",
	}
	if got := testmetrics.CounterValueForLabels(t, metrics.GRPCEgressBytes, labels); got != 0 {
		t.Fatalf("metric value = %v, want 0", got)
	}
}
