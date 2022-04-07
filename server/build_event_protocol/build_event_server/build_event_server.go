package build_event_server

import (
	"context"
	"io"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_asset/fetch_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_asset/push_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
	rapb "github.com/buildbuddy-io/buildbuddy/proto/remote_asset"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type BuildEventProtocolServer struct {
	env environment.Env
}

func Register(env environment.Env, grpcServer *grpc.Server) error {
	// Register to handle build event protocol messages.
	buildEventServer, err := NewBuildEventProtocolServer(env)
	if err != nil {
		return status.InternalErrorf("Error initializing BuildEventProtocolServer: %s", err)
	}
	pepb.RegisterPublishBuildEventServer(grpcServer, buildEventServer)

	enableCache := env.GetCache() != nil
	// OPTIONAL CACHE API -- only enable if configured.
	if enableCache {
		// Register to handle content addressable storage (CAS) messages.
		casServer, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
		if err != nil {
			return status.InternalErrorf("Error initializing ContentAddressableStorageServer: %s", err)
		}
		repb.RegisterContentAddressableStorageServer(grpcServer, casServer)

		// Register to handle bytestream (upload and download) messages.
		byteStreamServer, err := byte_stream_server.NewByteStreamServer(env)
		if err != nil {
			return status.InternalErrorf("Error initializing ByteStreamServer: %s", err)
		}
		bspb.RegisterByteStreamServer(grpcServer, byteStreamServer)

		// Register to handle action cache (upload and download) messages.
		actionCacheServer, err := action_cache_server.NewActionCacheServer(env)
		if err != nil {
			return status.InternalErrorf("Error initializing ActionCacheServer: %s", err)
		}
		repb.RegisterActionCacheServer(grpcServer, actionCacheServer)

		pushServer := push_server.NewPushServer(env)
		rapb.RegisterPushServer(grpcServer, pushServer)

		fetchServer, err := fetch_server.NewFetchServer(env)
		if err != nil {
			return status.InternalErrorf("Error initializing FetchServer: %s", err)
		}
		rapb.RegisterFetchServer(grpcServer, fetchServer)

	}
	enableRemoteExec := false
	if rexec := env.GetRemoteExecutionService(); rexec != nil {
		enableRemoteExec = true
		repb.RegisterExecutionServer(grpcServer, rexec)
	}
	if scheduler := env.GetSchedulerService(); scheduler != nil {
		scpb.RegisterSchedulerServer(grpcServer, scheduler)
	}
	// Register to handle GetCapabilities messages, which tell the client
	// that this server supports CAS functionality.
	capabilitiesServer := capabilities_server.NewCapabilitiesServer(
		/*supportCAS=*/ enableCache,
		/*supportRemoteExec=*/ enableRemoteExec,
		/*supportZstd=*/ env.GetConfigurator().GetCacheZstdTranscodingEnabled(),
	)
	repb.RegisterCapabilitiesServer(grpcServer, capabilitiesServer)
	return nil
}

func NewBuildEventProtocolServer(env environment.Env) (*BuildEventProtocolServer, error) {
	return &BuildEventProtocolServer{
		env: env,
	}, nil
}

func (s *BuildEventProtocolServer) PublishLifecycleEvent(ctx context.Context, req *pepb.PublishLifecycleEventRequest) (*empty.Empty, error) {
	for _, c := range s.env.GetBuildEventProxyClients() {
		client := c
		go func() {
			client.PublishLifecycleEvent(ctx, req)
		}()
	}
	return &empty.Empty{}, nil
}

// Handles Streaming BuildToolEvent
// From the bazel client: (Read more in BuildEventServiceUploader.java.)
// {@link BuildEventServiceUploaderCommands#OPEN_STREAM} is the first event and opens a
// bidi streaming RPC for sending build events and receiving ACKs.
// {@link BuildEventServiceUploaderCommands#SEND_REGULAR_BUILD_EVENT} sends a build event to
// the server. Sending of the Nth build event does
// does not wait for the ACK of the N-1th build event to have been received.
// {@link BuildEventServiceUploaderCommands#SEND_LAST_BUILD_EVENT} sends the last build event
// and half closes the RPC.
// {@link BuildEventServiceUploaderCommands#ACK_RECEIVED} is executed for every ACK from
// the server and checks that the ACKs are in the correct order.
// {@link BuildEventServiceUploaderCommands#STREAM_COMPLETE} checks that all build events
// have been sent and all ACKs have been received. If not it invokes a retry logic that may
// decide to re-send every build event for which an ACK has not been received. If so, it
// adds an OPEN_STREAM event.
func (s *BuildEventProtocolServer) PublishBuildToolEventStream(stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	ctx := stream.Context()
	// Semantically, the protocol requires we ack events in order.
	acks := make([]int, 0)
	var streamID *bepb.StreamId
	var channel interfaces.BuildEventChannel

	forwardingStreams := make([]pepb.PublishBuildEvent_PublishBuildToolEventStreamClient, 0)
	for _, client := range s.env.GetBuildEventProxyClients() {
		stream, err := client.PublishBuildToolEventStream(ctx, grpc.WaitForReady(false))
		if err != nil {
			log.Warningf("Unable to proxy stream: %s", err)
			continue
		}
		defer stream.CloseSend()
		forwardingStreams = append(forwardingStreams, stream)
	}

	disconnectWithErr := func(e error) error {
		if channel != nil && streamID != nil {
			log.Warningf("Disconnecting invocation %q: %s", streamID.InvocationId, e)
			if err := channel.FinalizeInvocation(streamID.InvocationId); err != nil {
				log.Warningf("Error finalizing invocation %q during disconnect: %s", streamID.InvocationId, err)
			}
		}
		return e
	}

	for {
		in, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Warningf("Error receiving build event stream %+v: %s", streamID, err)
			return disconnectWithErr(err)
		}
		if streamID == nil {
			streamID = in.OrderedBuildEvent.StreamId
			channel = s.env.GetBuildEventHandler().OpenChannel(ctx, streamID.InvocationId)
			defer channel.Close()
		}

		if err := channel.HandleEvent(in); err != nil {
			if status.IsAlreadyExistsError(err) {
				log.Warningf("AlreadyExistsError handling event; this means the invocation already exists and may not be retried: %s", err)
				return err
			}
			log.Warningf("Error handling event; this means a broken build command: %s", err)
			return disconnectWithErr(err)
		}
		for _, stream := range forwardingStreams {
			// Intentionally ignore errors here -- proxying is best effort.
			stream.Send(in)
		}

		acks = append(acks, int(in.OrderedBuildEvent.SequenceNumber))
	}

	// Check that we have received all acks! If we haven't bail out since we
	// don't want to ack *anything*. This forces the client to retransmit
	// everything all at once, which means we don't need to worry about
	// cross-server consistency of messages in an invocation.
	sort.Sort(sort.IntSlice(acks))
	for i, ack := range acks {
		if ack != i+1 {
			log.Warningf("Missing ack: saw %d and wanted %d. Bailing!", ack, i+1)
			return io.EOF
		}
	}

	if channel != nil {
		if err := channel.FinalizeInvocation(streamID.GetInvocationId()); err != nil {
			log.Warningf("Error finalizing invocation %q: %s", streamID.GetInvocationId(), err)
			return err
		}
	}

	// Finally, ack everything.
	for _, ack := range acks {
		rsp := &pepb.PublishBuildToolEventStreamResponse{
			StreamId:       streamID,
			SequenceNumber: int64(ack),
		}
		if err := stream.Send(rsp); err != nil {
			log.Warningf("Error sending ack stream for invocation %q: %s", streamID.InvocationId, err)
			return err
		}
	}
	return nil
}
