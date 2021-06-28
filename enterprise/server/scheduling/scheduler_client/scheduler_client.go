package scheduler_client

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/auth"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/resources"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/version"
	"github.com/golang/protobuf/proto"
	"google.golang.org/grpc/metadata"

	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

const (
	schedulerCheckInInterval         = 5 * time.Second
	registrationFailureRetryInterval = 1 * time.Second
)

// Options provide overrides for executor registration properties.
type Options struct {
	// TESTING ONLY: overrides the hostname reported when registering executor
	HostnameOverride string
	// TESTING ONLY: overrides the port reported when registering executor
	PortOverride int32
	// TESTING ONLY: overrides the name reported when registering executor
	NodeNameOverride string
	// TESTING ONLY: overrides the API key sent by the client
	APIKeyOverride string
}

func makeExecutionNode(executorID string, options *Options) (*scpb.ExecutionNode, error) {
	hostname := options.HostnameOverride
	if hostname == "" {
		resHostname, err := resources.GetMyHostname()
		if err != nil {
			return nil, status.InternalErrorf("could not determine local hostname: %s", err)
		}
		hostname = resHostname
	}
	port := options.PortOverride
	if port == 0 {
		resPort, err := resources.GetMyPort()
		if err != nil {
			return nil, status.InternalErrorf("could not determine local port: %s", err)
		}
		port = resPort
	}
	nodeName := options.NodeNameOverride
	if nodeName == "" {
		nodeName = resources.GetNodeName()
	}
	return &scpb.ExecutionNode{
		Host:                  hostname,
		Port:                  port,
		AssignableMemoryBytes: resources.GetAllocatedRAMBytes(),
		AssignableMilliCpu:    resources.GetAllocatedCPUMillis(),
		Os:                    resources.GetOS(),
		Arch:                  resources.GetArch(),
		Pool:                  resources.GetPoolName(),
		Version:               version.AppVersion(),
		ExecutorId:            executorID,
	}, nil
}

func sleepWithContext(ctx context.Context, delay time.Duration) (done bool) {
	select {
	case <-ctx.Done():
		return true
	case <-time.After(delay):
		return false
	}
}

type Registration struct {
	schedulerClient     scpb.SchedulerClient
	queueExecutorServer scpb.QueueExecutorServer
	node                *scpb.ExecutionNode
	apiKey              string
	shutdownSignal      chan struct{}

	mu        sync.Mutex
	connected bool
}

func (r *Registration) getConnected() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.connected
}

func (r *Registration) setConnected(connected bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.connected = connected
}

func (r *Registration) Check(ctx context.Context) error {
	if r.getConnected() {
		return nil
	}
	return errors.New("not registered to scheduler yet")
}

func (r *Registration) processWorkStream(ctx context.Context, stream scpb.Scheduler_RegisterAndStreamWorkClient, schedulerMsgs chan *scpb.RegisterAndStreamWorkResponse) (bool, error) {
	registrationMsg := &scpb.RegisterAndStreamWorkRequest{
		RegisterExecutorRequest: &scpb.RegisterExecutorRequest{Node: r.node},
	}

	idleTimer := time.NewTimer(schedulerCheckInInterval)
	defer idleTimer.Stop()

	select {
	case <-ctx.Done():
		log.Debugf("Context cancelled, cancelling node registration.")
		return true, nil
	case <-r.shutdownSignal:
		log.Info("Executor shutting down, cancelling node registration.")
		return true, nil
	case msg, ok := <-schedulerMsgs:
		if !ok {
			return false, status.UnavailableError("could not receive message from scheduler")
		}

		if msg.EnqueueTaskReservationRequest == nil {
			return false, status.FailedPreconditionErrorf("message from scheduler did not contain a task reservation request:\n%s", proto.MarshalTextString(msg))
		}

		rsp, err := r.queueExecutorServer.EnqueueTaskReservation(ctx, msg.GetEnqueueTaskReservationRequest())
		if err != nil {
			log.Warningf("Task reservation enqueue failed: %s", err)
			return false, status.UnavailableErrorf("could not enqueue task reservation: %s", err)
		}
		rsp.TaskId = msg.GetEnqueueTaskReservationRequest().GetTaskId()
		rspMsg := &scpb.RegisterAndStreamWorkRequest{EnqueueTaskReservationResponse: rsp}
		if err := stream.Send(rspMsg); err != nil {
			return false, status.UnavailableErrorf("could not send task reservation response: %s", err)
		}
	case <-idleTimer.C:
		if err := stream.Send(registrationMsg); err != nil {
			return false, status.UnavailableErrorf("could not send idle registration message: %s", err)
		}
	}
	return false, nil
}

// maintainRegistrationAndStreamWork maintains registration with a scheduler server using the newer
// RegisterAndStreamWork API which supports both registration and task reservations.
func (r *Registration) maintainRegistrationAndStreamWork(ctx context.Context) {
	registrationMsg := &scpb.RegisterAndStreamWorkRequest{
		RegisterExecutorRequest: &scpb.RegisterExecutorRequest{Node: r.node},
	}

	defer r.setConnected(false)

	for {
		stream, err := r.schedulerClient.RegisterAndStreamWork(ctx)
		if err != nil {
			if done := sleepWithContext(ctx, registrationFailureRetryInterval); done {
				log.Debugf("Context cancelled, cancelling node registration.")
				return
			}
			continue
		}
		if err := stream.Send(registrationMsg); err != nil {
			log.Errorf("error registering node with scheduler: %s, will retry...", err)
			continue
		}

		r.setConnected(true)

		schedulerMsgs := make(chan *scpb.RegisterAndStreamWorkResponse)
		go func() {
			for {
				msg, err := stream.Recv()
				if err != nil {
					log.Warningf("Could not read from stream: %s", err)
					close(schedulerMsgs)
					break
				}
				select {
				case schedulerMsgs <- msg:
				case <-stream.Context().Done():
					return
				}
			}
		}()

		for {
			done, err := r.processWorkStream(ctx, stream, schedulerMsgs)
			if err != nil {
				_ = stream.CloseSend()
				log.Warningf("Error maintaining registration with scheduler, will retry: %s", err)
				break
			}
			if done {
				_ = stream.CloseSend()
				return
			}
		}
		r.setConnected(false)
		if done := sleepWithContext(ctx, registrationFailureRetryInterval); done {
			log.Debugf("Context cancelled, cancelling node registration.")
			return
		}
	}
}

// Start registers the executor with the scheduler and maintains that registration until the context is cancelled.
func (r *Registration) Start(ctx context.Context) {
	if r.apiKey != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, auth.APIKeyHeader, r.apiKey)
	}

	go func() {
		r.maintainRegistrationAndStreamWork(ctx)
	}()
}

// NewRegistration creates a handle to maintain registration with a scheduler server.
// The registration is not initiated until Start is called on the returned handle.
func NewRegistration(env environment.Env, queueExecutorServer scpb.QueueExecutorServer, executorID string, options *Options) (*Registration, error) {
	node, err := makeExecutionNode(executorID, options)
	if err != nil {
		return nil, status.InternalErrorf("Error determining node properties: %s", err)
	}
	apiKey := env.GetConfigurator().GetExecutorConfig().APIKey
	if options.APIKeyOverride != "" {
		apiKey = options.APIKeyOverride
	}

	shutdownSignal := make(chan struct{})
	env.GetHealthChecker().RegisterShutdownFunction(func(ctx context.Context) error {
		close(shutdownSignal)
		return nil
	})

	registration := &Registration{
		schedulerClient:     env.GetSchedulerClient(),
		queueExecutorServer: queueExecutorServer,
		node:                node,
		apiKey:              apiKey,
		shutdownSignal:      shutdownSignal,
	}
	env.GetHealthChecker().AddHealthCheck("registered_to_scheduler", registration)
	return registration, nil
}
