package executor_handle

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/util/grpc_client"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/tracing"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/peer"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	tpb "github.com/buildbuddy-io/buildbuddy/proto/trace"
)

const (
	// EnqueueTaskReservationTimeout specifies how long to wait before giving up on a probe.
	EnqueueTaskReservationTimeout = 100 * time.Millisecond
)

type executorID string

type ExecutorHandle interface {
	// TODO: Remove and use ID sent as part of the registration instead.
	ID() executorID
	GroupID() string
	RecvRegistration() (*scpb.ExecutionNode, error)
	SupportsTaskStreaming() bool
	EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error)
}

func executorIDFromContext(ctx context.Context) (executorID, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return "", status.FailedPreconditionError("peer info not in context")
	}
	return executorID(p.Addr.String()), nil
}

type registrationOnlyExecutorHandle struct {
	stream scpb.Scheduler_RegisterNodeServer

	id      executorID
	groupID string

	mu       sync.Mutex
	nodeAddr string
}

func NewRegistrationOnlyExecutorHandle(stream scpb.Scheduler_RegisterNodeServer, groupID string) (*registrationOnlyExecutorHandle, error) {
	id, err := executorIDFromContext(stream.Context())
	if err != nil {
		return nil, err
	}
	return &registrationOnlyExecutorHandle{stream: stream, id: id, groupID: groupID}, nil
}

func (h *registrationOnlyExecutorHandle) SupportsTaskStreaming() bool {
	return false
}

func (h *registrationOnlyExecutorHandle) ID() executorID {
	return h.id
}

func (h *registrationOnlyExecutorHandle) GroupID() string {
	return h.groupID
}

func (h *registrationOnlyExecutorHandle) RecvRegistration() (*scpb.ExecutionNode, error) {
	req, err := h.stream.Recv()
	if err != nil {
		return nil, err
	}
	node := &scpb.ExecutionNode{
		Host:                  req.GetNodeAddress().GetHost(),
		Port:                  req.GetNodeAddress().GetPort(),
		AssignableMemoryBytes: req.GetAssignableMemoryBytes(),
		AssignableMilliCpu:    req.GetAssignableMilliCpu(),
		Os:                    req.GetOs(),
		Arch:                  req.GetArch(),
		Pool:                  req.GetPool(),
		ExecutorId:            req.GetExecutorId(),
	}

	h.mu.Lock()
	h.nodeAddr = fmt.Sprintf("grpc://%s:%d", req.GetNodeAddress().GetHost(), req.GetNodeAddress().GetPort())
	h.mu.Unlock()

	return node, nil
}

func (h *registrationOnlyExecutorHandle) EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error) {
	h.mu.Lock()
	nodeAddr := h.nodeAddr
	h.mu.Unlock()

	if nodeAddr == "" {
		return nil, status.FailedPreconditionErrorf("nodeAddr is not set")
	}

	conn, err := grpc_client.DialTargetWithOptions(nodeAddr, true, grpc.WithTimeout(EnqueueTaskReservationTimeout), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	client := scpb.NewQueueExecutorClient(conn)
	return client.EnqueueTaskReservation(ctx, req)
}

// enqueueTaskReservationRequest represents a request to be sent via a work stream and a channel for the reply once one
// is received via the stream.
type enqueueTaskReservationRequest struct {
	proto    *scpb.EnqueueTaskReservationRequest
	response chan<- *scpb.EnqueueTaskReservationResponse
}

type registrationAndTasksExecutorHandle struct {
	stream  scpb.Scheduler_RegisterAndStreamWorkServer
	id      executorID
	groupID string

	mu       sync.RWMutex
	requests chan enqueueTaskReservationRequest
	replies  map[string]chan<- *scpb.EnqueueTaskReservationResponse
}

func NewRegistrationAndTasksExecutorHandle(stream scpb.Scheduler_RegisterAndStreamWorkServer, groupID string) (*registrationAndTasksExecutorHandle, error) {
	id, err := executorIDFromContext(stream.Context())
	if err != nil {
		return nil, err
	}
	h := &registrationAndTasksExecutorHandle{
		stream:   stream,
		id:       id,
		groupID:  groupID,
		requests: make(chan enqueueTaskReservationRequest, 10),
		replies:  make(map[string]chan<- *scpb.EnqueueTaskReservationResponse),
	}
	h.startTaskReservationStreamer()
	return h, nil
}

func (h *registrationAndTasksExecutorHandle) SupportsTaskStreaming() bool {
	return true
}

func (h *registrationAndTasksExecutorHandle) ID() executorID {
	return h.id
}

func (h *registrationAndTasksExecutorHandle) GroupID() string {
	return h.groupID
}

func (h *registrationAndTasksExecutorHandle) RecvRegistration() (*scpb.ExecutionNode, error) {
	for {
		req, err := h.stream.Recv()
		if err != nil {
			return nil, err
		}

		// Only registration requests are returned to the caller since only those messages are common to both of the
		// registration APIs and are handled in the same manner.
		// The rest are processed internally.
		// This can be simplified after the executors are on the new API and we don't need to support the old
		// RegisterNode API.

		if req.GetRegisterExecutorRequest() != nil {
			return req.GetRegisterExecutorRequest().GetNode(), nil
		} else if req.GetEnqueueTaskReservationResponse() != nil {
			h.handleTaskReservationResponse(req.GetEnqueueTaskReservationResponse())
		} else {
			log.Warningf("Invalid message from executor:\n%q", proto.MarshalTextString(req))
			return nil, status.InternalErrorf("message from executor did not contain any data")
		}
	}
}

func (h *registrationAndTasksExecutorHandle) handleTaskReservationResponse(response *scpb.EnqueueTaskReservationResponse) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ch := h.replies[response.GetTaskId()]
	if ch == nil {
		log.Warningf("Got task reservation response for unknown task %q", response.GetTaskId())
		return
	}

	// Reply channel is buffered so it's okay to write while holding lock.
	ch <- response
	close(ch)
	delete(h.replies, response.GetTaskId())
}

func (h *registrationAndTasksExecutorHandle) EnqueueTaskReservation(ctx context.Context, req *scpb.EnqueueTaskReservationRequest) (*scpb.EnqueueTaskReservationResponse, error) {
	req, ok := proto.Clone(req).(*scpb.EnqueueTaskReservationRequest)
	if !ok {
		log.Errorf("could not clone reservation request")
		return nil, status.InternalError("could not clone reservation request")
	}
	tracing.InjectProtoTraceMetadata(ctx, req.GetTraceMetadata(), func(m *tpb.Metadata) { req.TraceMetadata = m })

	timeout := time.NewTimer(EnqueueTaskReservationTimeout)
	rspCh := make(chan *scpb.EnqueueTaskReservationResponse, 1)
	select {
	case h.requests <- enqueueTaskReservationRequest{proto: req, response: rspCh}:
	case <-ctx.Done():
		return nil, status.CanceledErrorf("could not enqueue task reservation %q", req.GetTaskId())
	case <-timeout.C:
		log.Warningf("Could not enqueue task reservation %q on to work stream within timeout", req.GetTaskId())
		return nil, status.DeadlineExceededErrorf("could not enqueue task reservation %q on to stream", req.GetTaskId())
	}
	if !timeout.Stop() {
		<-timeout.C
	}

	select {
	case <-ctx.Done():
		return nil, status.CanceledErrorf("could not enqueue task reservation %q", req.GetTaskId())
	case rsp := <-rspCh:
		return rsp, nil
	}
}

func (h *registrationAndTasksExecutorHandle) startTaskReservationStreamer() {
	go func() {
		for {
			select {
			case req := <-h.requests:
				msg := scpb.RegisterAndStreamWorkResponse{EnqueueTaskReservationRequest: req.proto}
				h.mu.Lock()
				h.replies[req.proto.GetTaskId()] = req.response
				h.mu.Unlock()
				if err := h.stream.Send(&msg); err != nil {
					log.Warningf("Error sending task reservation response: %s", err)
					return
				}
			case <-h.stream.Context().Done():
				return
			}
		}
	}()
}
