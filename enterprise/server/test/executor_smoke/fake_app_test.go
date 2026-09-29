package executor_smoke_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"sort"
	"sync"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/server/backends/memory_cache"
	"github.com/buildbuddy-io/buildbuddy/server/nullauth"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/action_cache_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/byte_stream_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/capabilities_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/content_addressable_storage_server"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/hit_tracker"
	"github.com/buildbuddy-io/buildbuddy/server/util/healthcheck"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	gstatus "google.golang.org/grpc/status"
)

// fakeApp is a minimal, portable stand-in for the BuildBuddy app, containing
// just enough of the app for an executor to register, lease tasks, read and
// write the cache, and publish execution results.
//
// The cache services are the real server implementations, backed by an
// in-memory cache. The scheduler and execution services are small fakes that
// dispatch tasks directly to connected executors, so that no Redis or database
// is needed and the harness can run on any OS.
type fakeApp struct {
	env      *real_environment.RealEnv
	server   *grpc.Server
	listener net.Listener

	mu            sync.Mutex
	executors     []*connectedExecutor
	nextExecutor  int
	registered    chan struct{}
	tasks         map[string]*fakeTask
	unimplemented map[string]int

	scpb.UnimplementedSchedulerServer
	repb.UnimplementedExecutionServer
}

type connectedExecutor struct {
	node *scpb.ExecutionNode

	mu     sync.Mutex
	stream scpb.Scheduler_RegisterAndStreamWorkServer
}

func (e *connectedExecutor) send(rsp *scpb.RegisterAndStreamWorkResponse) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.stream.Send(rsp)
}

// fakeTask tracks a single execution from reservation to completion.
type fakeTask struct {
	id         string
	serialized []byte

	mu          sync.Mutex
	leaseCount  int
	finalized   bool
	reEnqueued  *gstatus.Status
	stages      []repb.ExecutionStage_Value
	response    *repb.ExecuteResponse
	done        chan struct{}
	closeOnce   sync.Once
	executorIDs []string
}

func (t *fakeTask) finish() {
	t.closeOnce.Do(func() { close(t.done) })
}

func startFakeApp() (*fakeApp, error) {
	hc := healthcheck.NewHealthChecker("executor-smoke-app")
	env := real_environment.NewRealEnv(hc)
	c, err := memory_cache.NewMemoryCache(4 << 30)
	if err != nil {
		return nil, err
	}
	env.SetCache(c)
	// Allows anonymous access, so that neither the executor nor the harness
	// needs credentials.
	env.SetAuthenticator(&nullauth.NullAuthenticator{})
	hit_tracker.Register(env)

	app := &fakeApp{
		env:           env,
		registered:    make(chan struct{}),
		tasks:         map[string]*fakeTask{},
		unimplemented: map[string]int{},
	}

	app.server = grpc.NewServer(
		grpc.MaxRecvMsgSize(64<<20),
		grpc.MaxSendMsgSize(64<<20),
		grpc.ChainUnaryInterceptor(app.unaryInterceptor),
		grpc.ChainStreamInterceptor(app.streamInterceptor),
	)
	bs, err := byte_stream_server.NewByteStreamServer(env)
	if err != nil {
		return nil, err
	}
	bspb.RegisterByteStreamServer(app.server, bs)
	cas, err := content_addressable_storage_server.NewContentAddressableStorageServer(env)
	if err != nil {
		return nil, err
	}
	repb.RegisterContentAddressableStorageServer(app.server, cas)
	ac, err := action_cache_server.NewActionCacheServer(env)
	if err != nil {
		return nil, err
	}
	repb.RegisterActionCacheServer(app.server, ac)
	repb.RegisterCapabilitiesServer(app.server, capabilities_server.NewCapabilitiesServer(env, true /*=supportCAS*/, true /*=supportRemoteExec*/, true /*=supportZstd*/))
	scpb.RegisterSchedulerServer(app.server, app)
	repb.RegisterExecutionServer(app.server, app)

	app.listener, err = net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	go app.server.Serve(app.listener)
	return app, nil
}

// Context returns a context for accessing the fake app's cache directly.
func (a *fakeApp) Context(ctx context.Context) (context.Context, error) {
	return prefix.AttachUserPrefixToContext(ctx, a.env.GetAuthenticator())
}

func (a *fakeApp) Target() string {
	return "grpc://" + a.listener.Addr().String()
}

func (a *fakeApp) Stop() {
	a.server.Stop()
}

// Unimplemented returns the RPCs that the executor called but which the fake
// app does not implement, along with call counts. A non-empty result may mean
// that the executor depends on an app API that this harness should fake.
func (a *fakeApp) Unimplemented() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for m, n := range a.unimplemented {
		out = append(out, fmt.Sprintf("%s (%d calls)", m, n))
	}
	sort.Strings(out)
	return out
}

func (a *fakeApp) recordErr(method string, err error) {
	if gstatus.Code(err) == codes.Unimplemented {
		a.mu.Lock()
		a.unimplemented[method]++
		a.mu.Unlock()
	}
}

func (a *fakeApp) unaryInterceptor(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	rsp, err := handler(ctx, req)
	a.recordErr(info.FullMethod, err)
	return rsp, err
}

func (a *fakeApp) streamInterceptor(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, ss)
	a.recordErr(info.FullMethod, err)
	return err
}

// WaitForExecutor waits for an executor to register and returns its node info.
func (a *fakeApp) WaitForExecutor(ctx context.Context) (*scpb.ExecutionNode, error) {
	select {
	case <-a.registered:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.executors[0].node, nil
}

func (a *fakeApp) RegisterAndStreamWork(stream scpb.Scheduler_RegisterAndStreamWorkServer) error {
	var ex *connectedExecutor
	defer func() {
		if ex == nil {
			return
		}
		a.mu.Lock()
		defer a.mu.Unlock()
		for i, e := range a.executors {
			if e == ex {
				a.executors = append(a.executors[:i], a.executors[i+1:]...)
				break
			}
		}
	}()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		switch {
		case req.GetRegisterExecutorRequest() != nil:
			node := req.GetRegisterExecutorRequest().GetNode()
			if ex != nil {
				ex.node = node
				continue
			}
			ex = &connectedExecutor{node: node, stream: stream}
			a.mu.Lock()
			a.executors = append(a.executors, ex)
			if len(a.executors) == 1 {
				select {
				case <-a.registered:
				default:
					close(a.registered)
				}
			}
			a.mu.Unlock()
		case req.GetAskForMoreWorkRequest() != nil:
			// All work is pushed eagerly, so there is never more to hand out.
			if ex != nil {
				err := ex.send(&scpb.RegisterAndStreamWorkResponse{
					AskForMoreWorkResponse: &scpb.AskForMoreWorkResponse{},
				})
				if err != nil {
					return err
				}
			}
		}
	}
}

// Schedule enqueues a task reservation on a connected executor. The returned
// task completes when the executor publishes a final operation, or fails to
// run the task.
func (a *fakeApp) Schedule(ctx context.Context, execTask *repb.ExecutionTask, size *scpb.TaskSize) (*fakeTask, error) {
	serialized, err := proto.Marshal(execTask)
	if err != nil {
		return nil, err
	}
	t := &fakeTask{
		id:         execTask.GetExecutionId(),
		serialized: serialized,
		done:       make(chan struct{}),
	}
	a.mu.Lock()
	a.tasks[t.id] = t
	if len(a.executors) == 0 {
		a.mu.Unlock()
		return nil, status.UnavailableError("no executors registered")
	}
	ex := a.executors[a.nextExecutor%len(a.executors)]
	a.nextExecutor++
	a.mu.Unlock()

	err = ex.send(&scpb.RegisterAndStreamWorkResponse{
		EnqueueTaskReservationRequest: &scpb.EnqueueTaskReservationRequest{
			TaskId:   t.id,
			TaskSize: size,
			SchedulingMetadata: &scpb.SchedulingMetadata{
				TaskSize:    size,
				TaskGroupId: "GR_SMOKE",
			},
		},
	})
	if err != nil {
		return nil, err
	}
	return t, nil
}

func (a *fakeApp) task(id string) (*fakeTask, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	t, ok := a.tasks[id]
	if !ok {
		return nil, status.NotFoundErrorf("task %q not found", id)
	}
	return t, nil
}

func (a *fakeApp) LeaseTask(stream scpb.Scheduler_LeaseTaskServer) error {
	var t *fakeTask
	leaseID := uuid.NewString()
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		rsp := &scpb.LeaseTaskResponse{
			LeaseDurationSeconds: 10,
			LeaseId:              leaseID,
			SupportsReconnect:    true,
		}
		if t == nil {
			t, err = a.task(req.GetTaskId())
			if err != nil {
				return err
			}
			t.mu.Lock()
			if t.leaseCount > 0 {
				t.mu.Unlock()
				return status.NotFoundErrorf("task %q is already leased", t.id)
			}
			t.leaseCount++
			t.executorIDs = append(t.executorIDs, req.GetExecutorId())
			t.mu.Unlock()
			rsp.SerializedTask = t.serialized
		}
		if req.GetFinalize() || req.GetReEnqueue() || req.GetRelease() {
			t.mu.Lock()
			t.finalized = req.GetFinalize()
			if req.GetReEnqueue() {
				t.reEnqueued = gstatus.FromProto(req.GetReEnqueueReason())
				if t.reEnqueued == nil {
					t.reEnqueued = gstatus.New(codes.Unknown, "re-enqueued with no reason")
				}
			}
			t.mu.Unlock()
			rsp.ClosedCleanly = true
			if err := stream.Send(rsp); err != nil {
				return err
			}
			// A re-enqueued task will not be retried by this fake, so
			// consider it done.
			if req.GetReEnqueue() {
				t.finish()
			}
			return nil
		}
		if err := stream.Send(rsp); err != nil {
			return err
		}
	}
}

func (a *fakeApp) PublishOperation(stream repb.Execution_PublishOperationServer) error {
	for {
		op, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&repb.PublishOperationResponse{})
		}
		if err != nil {
			return err
		}
		t, err := a.task(op.GetName())
		if err != nil {
			return err
		}
		t.mu.Lock()
		t.stages = append(t.stages, operation.ExtractStage(op))
		if op.GetDone() {
			t.response = operation.ExtractExecuteResponse(op)
		}
		t.mu.Unlock()
		if op.GetDone() {
			t.finish()
		}
	}
}

// Wait waits for the task to complete and returns the final ExecuteResponse.
func (t *fakeTask) Wait(ctx context.Context) (*repb.ExecuteResponse, error) {
	select {
	case <-t.done:
	case <-ctx.Done():
		return nil, fmt.Errorf("waiting for task %s: %w", t.id, ctx.Err())
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.response == nil && t.reEnqueued != nil {
		return nil, fmt.Errorf("executor re-enqueued task: %s", t.reEnqueued.Err())
	}
	return t.response, nil
}

func (t *fakeTask) Stages() []repb.ExecutionStage_Value {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]repb.ExecutionStage_Value(nil), t.stages...)
}

// WaitFinalized waits until the executor has closed the task lease.
func (t *fakeTask) WaitFinalized(ctx context.Context) error {
	for {
		t.mu.Lock()
		f := t.finalized
		t.mu.Unlock()
		if f {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}
