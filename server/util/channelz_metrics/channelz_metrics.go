// Package channelz_metrics periodically reads HTTP/2 flow-control state from the
// local gRPC channelz service and exports it as Prometheus metrics. This turns
// the per-connection flow-control window (which is otherwise only visible by
// hand via the channelz web UI) into an alertable signal: a connection whose
// remote (send) window sits at 0 is stalled behind the peer's flow control and
// head-of-line-blocks every stream multiplexed on it.
package channelz_metrics

import (
	"context"
	"fmt"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/flag"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	channelzpb "google.golang.org/grpc/channelz/grpc_channelz_v1"
)

const (
	directionRemote = "remote"
	directionLocal  = "local"

	// Upper bound on how long a single sampling pass may take.
	sampleTimeout = 30 * time.Second
)

var (
	enabled = flag.Bool("grpc_client.channelz_flow_control_metrics", false, "If true, periodically sample HTTP/2 flow-control state from the local channelz service and export it as Prometheus metrics. Requires the channelz service to be registered on the internal gRPC server.")

	interval = flag.Duration("grpc_client.channelz_flow_control_metrics_interval", 15*time.Second, "How often to sample channelz flow-control state.")
)

// connState is the subset of channelz socket data we export.
type connState struct {
	target       string
	remoteWindow *int64 // window for data we send; nil if channelz didn't report it
	localWindow  *int64 // window for data we receive; nil if channelz didn't report it
	openStreams  int64
}

// Start begins periodically exporting channelz-derived flow-control metrics, if
// the feature is enabled. It samples the local internal gRPC server, which must
// have the channelz service registered. Sampling stops when the health checker
// shuts down. It is a no-op (returning nil) when the feature is disabled.
func Start(env environment.Env) error {
	if !*enabled {
		return nil
	}
	target := fmt.Sprintf("grpc://localhost:%d", grpc_server.InternalGRPCPort())
	conn, err := grpc_client.DialInternalWithoutPooling(env, target)
	if err != nil {
		return status.InternalErrorf("channelz flow-control metrics: dial internal gRPC server: %s", err)
	}
	client := channelzpb.NewChannelzClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	env.GetHealthChecker().RegisterShutdownFunction(func(_ context.Context) error {
		cancel()
		return conn.Close()
	})
	go run(ctx, client)
	return nil
}

func run(ctx context.Context, client channelzpb.ChannelzClient) {
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sctx, cancel := context.WithTimeout(ctx, sampleTimeout)
			if err := sample(sctx, client); err != nil {
				log.Warningf("channelz flow-control metrics sample failed: %s", err)
			}
			cancel()
		}
	}
}

// sample walks all client channels via channelz and refreshes the flow-control
// metrics with a fresh snapshot.
func sample(ctx context.Context, client channelzpb.ChannelzClient) error {
	conns, err := collect(ctx, client)
	if err != nil {
		return err
	}
	// Per-connection counters are instantaneous state, so reset them before
	// repopulating: a target (or connection) that went away should stop being
	// reported rather than linger at a stale value. The histograms accumulate a
	// distribution over time and are intentionally not reset.
	metrics.GRPCClientConnectionCount.Reset()
	metrics.GRPCClientFlowControlBlockedConnections.Reset()

	type agg struct{ total, blocked int }
	byTarget := map[string]*agg{}
	for _, c := range conns {
		a := byTarget[c.target]
		if a == nil {
			a = &agg{}
			byTarget[c.target] = a
		}
		a.total++
		if c.remoteWindow != nil {
			metrics.GRPCClientFlowControlWindowBytes.WithLabelValues(c.target, directionRemote).Observe(float64(*c.remoteWindow))
			if *c.remoteWindow == 0 {
				a.blocked++
			}
		}
		if c.localWindow != nil {
			metrics.GRPCClientFlowControlWindowBytes.WithLabelValues(c.target, directionLocal).Observe(float64(*c.localWindow))
		}
		metrics.GRPCClientConnectionOpenStreams.WithLabelValues(c.target).Observe(float64(c.openStreams))
	}
	for target, a := range byTarget {
		metrics.GRPCClientConnectionCount.WithLabelValues(target).Set(float64(a.total))
		metrics.GRPCClientFlowControlBlockedConnections.WithLabelValues(target).Set(float64(a.blocked))
	}
	return nil
}

// collect walks every client channel -> subchannel -> socket via channelz and
// returns the flow-control state of each connection. Subchannels, child
// channels, and sockets can disappear between calls as connections churn; those
// are skipped rather than failing the whole pass.
func collect(ctx context.Context, client channelzpb.ChannelzClient) ([]connState, error) {
	var out []connState
	var startID int64
	for {
		resp, err := client.GetTopChannels(ctx, &channelzpb.GetTopChannelsRequest{StartChannelId: startID})
		if err != nil {
			return nil, err
		}
		for _, ch := range resp.GetChannel() {
			collectChannel(ctx, client, ch, ch.GetData().GetTarget(), &out)
			if id := ch.GetRef().GetChannelId(); id >= startID {
				startID = id + 1
			}
		}
		if resp.GetEnd() {
			return out, nil
		}
	}
}

// collectChannel gathers connections from a channel's subchannels and,
// recursively, its nested child channels. Hierarchical LB policies (e.g. xDS)
// place sockets under child channels rather than directly under the top
// channel, so we must descend into ChannelRef as well as SubchannelRef. Child
// channels are labeled with the top-level channel's target, since they may not
// carry one of their own.
func collectChannel(ctx context.Context, client channelzpb.ChannelzClient, ch *channelzpb.Channel, target string, out *[]connState) {
	for _, subRef := range ch.GetSubchannelRef() {
		*out = append(*out, collectSubchannel(ctx, client, target, subRef.GetSubchannelId())...)
	}
	for _, childRef := range ch.GetChannelRef() {
		resp, err := client.GetChannel(ctx, &channelzpb.GetChannelRequest{ChannelId: childRef.GetChannelId()})
		if err != nil {
			continue
		}
		collectChannel(ctx, client, resp.GetChannel(), target, out)
	}
}

func collectSubchannel(ctx context.Context, client channelzpb.ChannelzClient, target string, subchannelID int64) []connState {
	resp, err := client.GetSubchannel(ctx, &channelzpb.GetSubchannelRequest{SubchannelId: subchannelID})
	if err != nil {
		return nil
	}
	var out []connState
	for _, sockRef := range resp.GetSubchannel().GetSocketRef() {
		sockResp, err := client.GetSocket(ctx, &channelzpb.GetSocketRequest{SocketId: sockRef.GetSocketId()})
		if err != nil {
			continue
		}
		d := sockResp.GetSocket().GetData()
		if d == nil {
			continue
		}
		c := connState{
			target:      target,
			openStreams: d.GetStreamsStarted() - d.GetStreamsSucceeded() - d.GetStreamsFailed(),
		}
		// The window fields are wrapper messages: a nil wrapper means channelz
		// didn't report the window (unknown), which is distinct from a reported
		// value of 0 (send-blocked).
		if w := d.GetRemoteFlowControlWindow(); w != nil {
			v := w.GetValue()
			c.remoteWindow = &v
		}
		if w := d.GetLocalFlowControlWindow(); w != nil {
			v := w.GetValue()
			c.localWindow = &v
		}
		out = append(out, c)
	}
	return out
}
