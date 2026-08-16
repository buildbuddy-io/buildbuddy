package channelz_metrics

import (
	"context"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/metrics"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"google.golang.org/grpc"
	channelzpb "google.golang.org/grpc/channelz/grpc_channelz_v1"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// fakeChannelz is a canned channelz server used to drive collect() without a
// real gRPC connection.
type fakeChannelz struct {
	channels     []*channelzpb.Channel // top-level channels, returned by GetTopChannels
	channelsByID map[int64]*channelzpb.Channel
	subchannels  map[int64]*channelzpb.Subchannel
	sockets      map[int64]*channelzpb.Socket
	missing      map[int64]bool // socket ids that "vanished" and return an error
	pageSize     int
}

func (f *fakeChannelz) GetTopChannels(ctx context.Context, in *channelzpb.GetTopChannelsRequest, opts ...grpc.CallOption) (*channelzpb.GetTopChannelsResponse, error) {
	var page []*channelzpb.Channel
	for _, ch := range f.channels {
		if ch.GetRef().GetChannelId() < in.GetStartChannelId() {
			continue
		}
		page = append(page, ch)
		if f.pageSize > 0 && len(page) >= f.pageSize {
			break
		}
	}
	end := true
	if len(page) > 0 {
		last := page[len(page)-1].GetRef().GetChannelId()
		for _, ch := range f.channels {
			if ch.GetRef().GetChannelId() > last {
				end = false
				break
			}
		}
	}
	return &channelzpb.GetTopChannelsResponse{Channel: page, End: end}, nil
}

func (f *fakeChannelz) GetSubchannel(ctx context.Context, in *channelzpb.GetSubchannelRequest, opts ...grpc.CallOption) (*channelzpb.GetSubchannelResponse, error) {
	sc, ok := f.subchannels[in.GetSubchannelId()]
	if !ok {
		return nil, status.NotFoundError("no such subchannel")
	}
	return &channelzpb.GetSubchannelResponse{Subchannel: sc}, nil
}

func (f *fakeChannelz) GetSocket(ctx context.Context, in *channelzpb.GetSocketRequest, opts ...grpc.CallOption) (*channelzpb.GetSocketResponse, error) {
	if f.missing[in.GetSocketId()] {
		return nil, status.NotFoundError("socket vanished")
	}
	s, ok := f.sockets[in.GetSocketId()]
	if !ok {
		return nil, status.NotFoundError("no such socket")
	}
	return &channelzpb.GetSocketResponse{Socket: s}, nil
}

// Unused interface methods.
func (f *fakeChannelz) GetServers(ctx context.Context, in *channelzpb.GetServersRequest, opts ...grpc.CallOption) (*channelzpb.GetServersResponse, error) {
	return nil, nil
}
func (f *fakeChannelz) GetServer(ctx context.Context, in *channelzpb.GetServerRequest, opts ...grpc.CallOption) (*channelzpb.GetServerResponse, error) {
	return nil, nil
}
func (f *fakeChannelz) GetServerSockets(ctx context.Context, in *channelzpb.GetServerSocketsRequest, opts ...grpc.CallOption) (*channelzpb.GetServerSocketsResponse, error) {
	return nil, nil
}
func (f *fakeChannelz) GetChannel(ctx context.Context, in *channelzpb.GetChannelRequest, opts ...grpc.CallOption) (*channelzpb.GetChannelResponse, error) {
	ch, ok := f.channelsByID[in.GetChannelId()]
	if !ok {
		return nil, status.NotFoundError("no such channel")
	}
	return &channelzpb.GetChannelResponse{Channel: ch}, nil
}

func i64(v int64) *int64 { return &v }

func socket(id int64, remote, local *int64, started, succeeded, failed int64) *channelzpb.Socket {
	d := &channelzpb.SocketData{StreamsStarted: started, StreamsSucceeded: succeeded, StreamsFailed: failed}
	if remote != nil {
		d.RemoteFlowControlWindow = wrapperspb.Int64(*remote)
	}
	if local != nil {
		d.LocalFlowControlWindow = wrapperspb.Int64(*local)
	}
	return &channelzpb.Socket{Ref: &channelzpb.SocketRef{SocketId: id}, Data: d}
}

// addSubchannel registers a subchannel owning a single socket.
func (f *fakeChannelz) addSubchannel(subID, socketID int64, sock *channelzpb.Socket) {
	f.subchannels[subID] = &channelzpb.Subchannel{
		Ref:       &channelzpb.SubchannelRef{SubchannelId: subID},
		SocketRef: []*channelzpb.SocketRef{{SocketId: socketID}},
	}
	if sock != nil {
		f.sockets[socketID] = sock
	}
}

// addChannel builds a top-level channel with a single subchannel pointing at a
// single socket, and registers the subchannel/socket in the fake.
func (f *fakeChannelz) addChannel(channelID, subID, socketID int64, target string, sock *channelzpb.Socket) {
	ch := &channelzpb.Channel{
		Ref:           &channelzpb.ChannelRef{ChannelId: channelID},
		Data:          &channelzpb.ChannelData{Target: target},
		SubchannelRef: []*channelzpb.SubchannelRef{{SubchannelId: subID}},
	}
	f.channels = append(f.channels, ch)
	f.channelsByID[channelID] = ch
	f.addSubchannel(subID, socketID, sock)
}

// addNestedChannel builds a top-level channel whose socket lives under a child
// channel (as with hierarchical LB policies like xDS): the top channel has no
// direct subchannel, only a ChannelRef to the child, which owns the subchannel
// and socket. The child carries no target of its own.
func (f *fakeChannelz) addNestedChannel(topID, childID, subID, socketID int64, target string, sock *channelzpb.Socket) {
	child := &channelzpb.Channel{
		Ref:           &channelzpb.ChannelRef{ChannelId: childID},
		Data:          &channelzpb.ChannelData{},
		SubchannelRef: []*channelzpb.SubchannelRef{{SubchannelId: subID}},
	}
	f.channelsByID[childID] = child
	top := &channelzpb.Channel{
		Ref:        &channelzpb.ChannelRef{ChannelId: topID},
		Data:       &channelzpb.ChannelData{Target: target},
		ChannelRef: []*channelzpb.ChannelRef{{ChannelId: childID}},
	}
	f.channels = append(f.channels, top)
	f.channelsByID[topID] = top
	f.addSubchannel(subID, socketID, sock)
}

func newFake() *fakeChannelz {
	return &fakeChannelz{
		channelsByID: map[int64]*channelzpb.Channel{},
		subchannels:  map[int64]*channelzpb.Subchannel{},
		sockets:      map[int64]*channelzpb.Socket{},
		missing:      map[int64]bool{},
	}
}

func TestCollect(t *testing.T) {
	f := newFake()
	f.pageSize = 2 // force pagination across the three "remote" channels
	const remote = "grpc://remote:443"
	const other = "grpc://other:443"
	// Blocked: remote window 0, 100 open streams.
	f.addChannel(1, 11, 101, remote, socket(101, i64(0), i64(1000), 105, 5, 0))
	// Healthy: remote window 500000, 1 open stream.
	f.addChannel(2, 12, 102, remote, socket(102, i64(500_000), i64(2000), 10, 8, 1))
	// Unknown remote window (nil wrapper), 0 open streams.
	f.addChannel(3, 13, 103, remote, socket(103, nil, i64(3000), 3, 3, 0))
	// Different target.
	f.addChannel(4, 14, 104, other, socket(104, i64(12345), i64(4000), 2, 2, 0))
	// Socket that vanished mid-walk: should be skipped, not fail the pass.
	f.addChannel(5, 15, 105, remote, nil)
	f.missing[105] = true

	conns, err := collect(context.Background(), f)
	require.NoError(t, err)

	byTarget := map[string][]connState{}
	for _, c := range conns {
		byTarget[c.target] = append(byTarget[c.target], c)
	}
	require.Len(t, byTarget[remote], 3, "vanished socket should be skipped")
	require.Len(t, byTarget[other], 1)

	// Verify the blocked connection's fields.
	var blocked *connState
	for i := range conns {
		if conns[i].openStreams == 100 {
			blocked = &conns[i]
		}
	}
	require.NotNil(t, blocked)
	require.Equal(t, remote, blocked.target)
	require.NotNil(t, blocked.remoteWindow)
	require.Equal(t, int64(0), *blocked.remoteWindow)

	// The unknown-window connection (socket 103) reports a nil remote window,
	// distinct from a reported 0, and 0 open streams.
	var unknown *connState
	for i := range conns {
		if conns[i].target == remote && conns[i].remoteWindow == nil {
			unknown = &conns[i]
		}
	}
	require.NotNil(t, unknown)
	require.Equal(t, int64(0), unknown.openStreams)
}

func TestCollect_NestedChildChannels(t *testing.T) {
	f := newFake()
	const target = "xds:///remote:443"
	// The top channel (1) has no direct subchannel; its socket lives under child
	// channel 2, reachable only via ChannelRef + GetChannel.
	f.addNestedChannel(1, 2, 21, 201, target, socket(201, i64(0), i64(1000), 100, 0, 0))

	conns, err := collect(context.Background(), f)
	require.NoError(t, err)
	require.Len(t, conns, 1, "socket under a child channel should be found")
	require.Equal(t, target, conns[0].target, "child-channel socket should inherit the top channel's target")
	require.NotNil(t, conns[0].remoteWindow)
	require.Equal(t, int64(0), *conns[0].remoteWindow)
}

func TestSampleAggregates(t *testing.T) {
	f := newFake()
	const target = "grpc://sample-test:443"
	f.addChannel(1, 11, 101, target, socket(101, i64(0), i64(1000), 100, 0, 0))     // blocked
	f.addChannel(2, 12, 102, target, socket(102, i64(0), i64(1000), 50, 0, 0))      // blocked
	f.addChannel(3, 13, 103, target, socket(103, i64(500_000), i64(1000), 5, 4, 0)) // healthy

	require.NoError(t, sample(context.Background(), f))

	require.Equal(t, 3.0, testutil.ToFloat64(metrics.GRPCClientConnectionCount.WithLabelValues(target)))
	require.Equal(t, 2.0, testutil.ToFloat64(metrics.GRPCClientFlowControlBlockedConnections.WithLabelValues(target)))
}
