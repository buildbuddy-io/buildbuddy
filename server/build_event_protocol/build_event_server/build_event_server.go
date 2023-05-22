package build_event_server

import (
	"context"
	"io"
	"sort"

	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	bepb "github.com/buildbuddy-io/buildbuddy/proto/build_events"
	pepb "github.com/buildbuddy-io/buildbuddy/proto/publish_build_event"
)

type BuildEventProtocolServer struct {
	env environment.Env
	// If true, wait until orwarding clients acknowledge.
	synchronous bool
}

func Register(env environment.Env) error {
	// Register to handle build event protocol messages.
	buildEventServer, err := NewBuildEventProtocolServer(env, false)
	if err != nil {
		return status.InternalErrorf("Error initializing BuildEventProtocolServer: %s", err)
	}
	env.SetBuildEventServer(buildEventServer)
	return nil
}

func NewBuildEventProtocolServer(env environment.Env, synchronous bool) (*BuildEventProtocolServer, error) {
	return &BuildEventProtocolServer{
		env:         env,
		synchronous: synchronous,
	}, nil
}

func (s *BuildEventProtocolServer) PublishLifecycleEvent(ctx context.Context, req *pepb.PublishLifecycleEventRequest) (*emptypb.Empty, error) {
	eg, ctx := errgroup.WithContext(ctx)
	for _, c := range s.env.GetBuildEventProxyClients() {
		client := c
		eg.Go(func() error {
			client.PublishLifecycleEvent(ctx, req)
			return nil
		})
	}
	if s.synchronous {
		eg.Wait()
	}
	return &emptypb.Empty{}, nil
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

	eg, ctx := errgroup.WithContext(ctx)
	if s.synchronous {
		// wait for acknowledges from the forwarding stream clients. Note: Wait()
		// needs to be run *after* fwdStream.CloseSend()
		defer eg.Wait()
	}
	forwardingStreams := make([]pepb.PublishBuildEvent_PublishBuildToolEventStreamClient, 0)
	for _, client := range s.env.GetBuildEventProxyClients() {
		fwdStream, err := client.PublishBuildToolEventStream(ctx, grpc.WaitForReady(false))
		if err != nil {
			log.CtxWarningf(ctx, "Unable to proxy stream: %s", err)
			continue
		}
		eg.Go(func() error {
			for {
				_, err := fwdStream.Recv()
				if err == io.EOF {
					break
				}
				if err != nil {
					log.Warningf("Got error while getting response from proxy: %s", err)
					break
				}
			}
			return nil
		})
		defer fwdStream.CloseSend()
		forwardingStreams = append(forwardingStreams, fwdStream)
	}

	disconnectWithErr := func(e error) error {
		if channel != nil && streamID != nil {
			log.CtxWarningf(ctx, "Disconnecting invocation %q: %s", streamID.InvocationId, e)
			if err := channel.FinalizeInvocation(streamID.InvocationId); err != nil {
				log.CtxWarningf(ctx, "Error finalizing invocation %q during disconnect: %s", streamID.InvocationId, err)
			}
		}
		return e
	}

	errCh := make(chan error)
	inCh := make(chan *pepb.PublishBuildToolEventStreamRequest)

	// Listen on request stream in the background
	go func() {
		for {
			in, err := stream.Recv()
			if err != nil {
				errCh <- err
				return
			}
			inCh <- in
		}
	}()

	var channelDone <-chan struct{}
	for {
		select {
		case <-channelDone:
			return disconnectWithErr(status.FromContextError(channel.Context()))
		case err := <-errCh:
			if err == io.EOF {
				return postProcessStream(ctx, channel, streamID, acks, stream)
			}
			log.CtxWarningf(ctx, "Error receiving build event stream %+v: %s", streamID, err)
			return disconnectWithErr(err)
		case in := <-inCh:
			if streamID == nil {
				streamID = in.OrderedBuildEvent.StreamId
				ctx = log.EnrichContext(ctx, log.InvocationIDKey, streamID.InvocationId)
				channel = s.env.GetBuildEventHandler().OpenChannel(ctx, streamID.InvocationId)
				channelDone = channel.Context().Done()
				defer channel.Close()
			}

			if err := channel.HandleEvent(in); err != nil {
				if status.IsAlreadyExistsError(err) {
					log.CtxWarningf(ctx, "AlreadyExistsError handling event; this means the invocation already exists and may not be retried: %s", err)
					return err
				}
				log.CtxWarningf(ctx, "Error handling event; this means a broken build command: %s", err)
				return disconnectWithErr(err)
			}
			for _, stream := range forwardingStreams {
				// Intentionally ignore errors here -- proxying is best effort.
				stream.Send(in)
			}
			acks = append(acks, int(in.OrderedBuildEvent.SequenceNumber))
		}
	}
}

func postProcessStream(ctx context.Context, channel interfaces.BuildEventChannel, streamID *bepb.StreamId, acks []int, stream pepb.PublishBuildEvent_PublishBuildToolEventStreamServer) error {
	if channel != nil && channel.GetNumDroppedEvents() > 0 {
		log.CtxWarningf(ctx, "We got over 100 build events before an event with options for invocation %s. Dropped the %d earliest event(s).",
			streamID.InvocationId, channel.GetNumDroppedEvents())
	}

	// Check that we have received all acks! If we haven't bail out since we
	// don't want to ack *anything*. This forces the client to retransmit
	// everything all at once, which means we don't need to worry about
	// cross-server consistency of messages in an invocation.
	sort.Sort(sort.IntSlice(acks))

	expectedSeqNo := channel.GetInitialSequenceNumber()
	for _, ack := range acks {
		if ack != int(expectedSeqNo) {
			log.CtxWarningf(ctx, "Missing ack: saw %d and wanted %d. Bailing!", ack, expectedSeqNo)
			return status.UnknownErrorf("event sequence number mismatch: received %d, wanted %d", ack, expectedSeqNo)
		}
		expectedSeqNo++
	}

	if channel != nil {
		if err := channel.FinalizeInvocation(streamID.GetInvocationId()); err != nil {
			log.CtxWarningf(ctx, "Error finalizing invocation %q: %s", streamID.GetInvocationId(), err)
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
			log.CtxWarningf(ctx, "Error sending ack stream for invocation %q: %s", streamID.InvocationId, err)
			return err
		}
	}
	return nil
}
