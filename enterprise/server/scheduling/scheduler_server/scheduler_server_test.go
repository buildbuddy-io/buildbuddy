package scheduler_server

import (
	"context"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/platform"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testauth"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/durationpb"

	ctxpb "github.com/buildbuddy-io/buildbuddy/proto/context"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
)

const (
	defaultOS   = "linux"
	defaultArch = "amd64"
)

type fakeTaskRouter struct {
	preferredExecutors []string
}

type fakeRankedNode struct {
	node      interfaces.ExecutionNode
	preferred bool
}

func (n fakeRankedNode) GetExecutionNode() interfaces.ExecutionNode {
	return n.node
}

func (n fakeRankedNode) IsPreferred() bool {
	return n.preferred
}

func (f *fakeTaskRouter) RankNodes(ctx context.Context, action *repb.Action, cmd *repb.Command, remoteInstanceName string, nodes []interfaces.ExecutionNode) []interfaces.RankedExecutionNode {
	rankedNodes := make([]interfaces.RankedExecutionNode, len(nodes))
	for i, node := range nodes {
		preferred := false
		for _, preferredNodeID := range f.preferredExecutors {
			if node.GetExecutorId() == preferredNodeID {
				preferred = true
				break
			}
		}
		rankedNodes[i] = fakeRankedNode{node: node, preferred: preferred}
	}

	// Return the preferred nodes first, then non-preferred nodes, both sections
	// sorted deterministically by executor ID.
	sort.Slice(rankedNodes, func(i, j int) bool {
		if rankedNodes[i].IsPreferred() && !rankedNodes[j].IsPreferred() {
			return true
		} else if !rankedNodes[i].IsPreferred() && rankedNodes[j].IsPreferred() {
			return false
		}
		return nodes[i].GetExecutorId() < nodes[j].GetExecutorId()
	})
	return rankedNodes
}

func (f *fakeTaskRouter) MarkSucceeded(ctx context.Context, action *repb.Action, cmd *repb.Command, remoteInstanceName, executorInstanceID string) {
}

func (f *fakeTaskRouter) MarkFailed(ctx context.Context, action *repb.Action, cmd *repb.Command, remoteInstanceName, executorInstanceID string) {
}

type schedulerOpts struct {
	options            Options
	userOwnedEnabled   bool
	groupOwnedEnabled  bool
	preferredExecutors []string
}

func getEnv(t *testing.T, opts *schedulerOpts, user string) (*testenv.TestEnv, context.Context) {
	redisTarget := testredis.Start(t).Target
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{
		RedisTarget: redisTarget,
	})

	flags.Set(t, "remote_execution.default_pool_name", "defaultPoolName")
	flags.Set(t, "remote_execution.shared_executor_pool_group_id", "sharedGroupID")

	err := execution_server.Register(env)
	require.NoError(t, err)
	env.SetTaskRouter(&fakeTaskRouter{opts.preferredExecutors})
	s, err := NewSchedulerServerWithOptions(env, &opts.options)
	require.NoError(t, err)
	env.SetSchedulerService(s)

	server, runFunc, lis := testenv.RegisterLocalGRPCServer(t, env)
	scpb.RegisterSchedulerServer(server, env.GetSchedulerService())
	go runFunc()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clientConn, err := testenv.LocalGRPCConn(ctx, lis)
	require.NoError(t, err)
	sc := scpb.NewSchedulerClient(clientConn)
	env.SetSchedulerClient(sc)

	testUsers := make(map[string]interfaces.UserInfo, 0)
	testUsers["user1"] = &testauth.TestUser{UserID: "user1", GroupID: "group1", UseGroupOwnedExecutors: opts.groupOwnedEnabled}

	ta := testauth.NewTestAuthenticator(testUsers)
	env.SetAuthenticator(ta)
	s.enableUserOwnedExecutors = opts.userOwnedEnabled

	if user != "" {
		authenticatedCtx, err := ta.WithAuthenticatedUser(context.Background(), user)
		require.NoError(t, err)
		ctx = authenticatedCtx
	}
	return env, ctx
}

func getScheduleServer(t *testing.T, userOwnedEnabled, groupOwnedEnabled bool, user string) (*SchedulerServer, context.Context) {
	env, ctx := getEnv(t, &schedulerOpts{userOwnedEnabled: userOwnedEnabled, groupOwnedEnabled: groupOwnedEnabled}, user)
	return env.GetSchedulerService().(*SchedulerServer), ctx
}

func TestSchedulerServerGetPoolInfoUserOwnedDisabled(t *testing.T) {
	s, ctx := getScheduleServer(t, false, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "amd64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestSchedulerServerGetPoolInfoNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "amd64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestSchedulerServerGetPoolInfoWithOS(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "user1")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "amd64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)

	p, err = s.GetPoolInfo(ctx, "darwin", "arm64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)

	s.forceUserOwnedDarwinExecutors = true
	p, err = s.GetPoolInfo(ctx, "linux", "amd64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)

	p, err = s.GetPoolInfo(ctx, "darwin", "arm64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)
}

func TestSchedulerServerGetPoolInfoWithRequestedPoolWithAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, true, "user1")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "amd64", "my-pool", "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "my-pool", p.Name)
}

func TestSchedulerServerGetPoolInfoWithRequestedPoolWithNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "amd64", "my-pool", "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "my-pool", p.Name)
}

func TestSchedulerServerGetPoolInfoDarwin(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "amd64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestSchedulerServerGetPoolInfoDarwinNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	s.forceUserOwnedDarwinExecutors = true
	_, err := s.GetPoolInfo(ctx, "darwin", "arm64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.Error(t, err)
}

func TestSchedulerServerGetPoolInfoSelfHostedNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	_, err := s.GetPoolInfo(ctx, "linux", "amd64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeSelfHosted)
	require.Error(t, err)
}

func TestSchedulerServerGetPoolInfoSelfHosted(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "user1")
	p, err := s.GetPoolInfo(ctx, "linux", "amd64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeSelfHosted)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	// Linux workflows should respect useSelfHosted bool.
	p, err = s.GetPoolInfo(ctx, "linux", "amd64", "workflows", "WF1234" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "workflows", p.Name)

	p, err = s.GetPoolInfo(ctx, "linux", "amd64", "workflows", "WF1234" /*=workflowID*/, interfaces.PoolTypeSelfHosted)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "workflows", p.Name)
}

func TestSchedulerServerGetPoolInfoSelfHostedByDefault(t *testing.T) {
	s, ctx := getScheduleServer(t, true, true, "user1")
	p, err := s.GetPoolInfo(ctx, "linux", "amd64", "", "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	p, err = s.GetPoolInfo(ctx, "linux", "amd64", "", "" /*=workflowID*/, interfaces.PoolTypeSelfHosted)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	// Explicitly set use-self-hosted-executors=false
	p, err = s.GetPoolInfo(ctx, "linux", "amd64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeShared)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)

	// Linux workflows should respect useSelfHosted bool.
	p, err = s.GetPoolInfo(ctx, "linux", "amd64", "workflows", "WF1234" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "workflows", p.Name)

	p, err = s.GetPoolInfo(ctx, "linux", "amd64", "workflows", "WF1234" /*=workflowID*/, interfaces.PoolTypeSelfHosted)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "workflows", p.Name)
}

func TestSchedulerServerGetPoolInfoWithPoolOverride(t *testing.T) {
	tmp := testfs.MakeTempDir(t)
	overridePool := "experimental-linux-amd64-pool"
	configFile := testfs.WriteFile(t, tmp, "config.flagd.json", `{
	"$schema": "https://flagd.dev/schema/v0/flags.json",
	"flags": {
		"remote_execution.pool_override": {
			"state": "ENABLED",
			"defaultVariant": "default",
			"variants": {
				"experimentalPool": {
					"pool": "`+overridePool+`"
				},
				"default": {}
			},
			"targeting": {
				"if": [
					{
						"and": [
							{ "==": [{ "var": "os" }, "linux"] },
							{ "==": [{ "var": "arch" }, "amd64"] }
						]
					},
					"experimentalPool"
				]
			}
		}
	}
}`)
	provider := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(configFile))
	openfeature.SetProviderAndWait(provider)
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)

	env, ctx := getEnv(t, &schedulerOpts{userOwnedEnabled: true}, "user1")
	env.SetExperimentFlagProvider(fp)
	s := env.GetSchedulerService()

	p, err := s.GetPoolInfo(ctx, "linux", "arm64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, &interfaces.PoolInfo{
		GroupID:  "sharedGroupID",
		Name:     "defaultPoolName",
		IsShared: true,
	}, p)

	p, err = s.GetPoolInfo(ctx, "darwin", "amd64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, &interfaces.PoolInfo{
		GroupID:  "sharedGroupID",
		Name:     "defaultPoolName",
		IsShared: true,
	}, p)

	p, err = s.GetPoolInfo(ctx, "linux", "amd64", "" /*=pool*/, "" /*=workflowID*/, interfaces.PoolTypeDefault)
	require.NoError(t, err)
	require.Equal(t, &interfaces.PoolInfo{
		GroupID:  "sharedGroupID",
		Name:     overridePool,
		IsShared: true,
	}, p)
}

func TestSchedulerServerPersistentVolumes(t *testing.T) {
	tmp := testfs.MakeTempDir(t)
	configFile := testfs.WriteFile(t, tmp, "config.flagd.json", `{
	"$schema": "https://flagd.dev/schema/v0/flags.json",
	"flags": {
		"remote_execution.persistent_volumes": {
			"state": "ENABLED",
			"defaultVariant": "default",
			"variants": {
				"tmp-cache": "cache:/tmp/.cache",
				"default": ""
			},
			"targeting": {
				"fractional": [
					["tmp-cache", 50],
					["default", 0]
				]
			}
		}
	}
}`)
	provider := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(configFile))
	openfeature.SetProviderAndWait(provider)
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)

	env, ctx := getEnv(t, &schedulerOpts{}, "")
	env.SetExperimentFlagProvider(fp)
	fe := newFakeExecutor(ctx, t, env.GetSchedulerClient())
	fe.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{})

	fe.WaitForTask(taskID)
	lease := fe.Claim(taskID)
	defer lease.Finalize()

	require.Equal(t, []string{"remote_execution.persistent_volumes:tmp-cache"}, lease.task.GetExperiments())
	require.Empty(t, cmp.Diff([]*repb.Platform_Property{
		{Name: "persistent-volumes", Value: "cache:/tmp/.cache"},
	}, lease.task.GetPlatformOverrides().GetProperties(), protocmp.Transform()), nil)
}

type task struct {
	delay time.Duration
}

type Result[T any] struct {
	Value T
	Err   error
}

type schedulerRequest struct {
	request *scpb.RegisterAndStreamWorkRequest
	reply   chan error
}

type fakeExecutor struct {
	t               *testing.T
	schedulerClient scpb.SchedulerClient

	id   string
	node *scpb.ExecutionNode

	ctx       context.Context
	stop      context.CancelFunc
	unhealthy atomic.Bool
	wg        sync.WaitGroup

	mu    sync.Mutex
	tasks map[string]task

	send chan *scpb.RegisterAndStreamWorkRequest
	// Channel for inspecting replies from the scheduler within test cases. This
	// is a buffered channel - sends are non-blocking to avoid blocking the
	// scheduler goroutine. Tests that aren't interested in asserting on the
	// scheduler's replies don't need to receive from this channel.
	schedulerMessages chan *scpb.RegisterAndStreamWorkResponse
}

func newFakeExecutor(ctx context.Context, t *testing.T, schedulerClient scpb.SchedulerClient) *fakeExecutor {
	id, err := uuid.NewRandom()
	require.NoError(t, err)
	return newFakeExecutorWithId(ctx, t, id.String(), schedulerClient)
}

func newFakeExecutorWithId(ctx context.Context, t *testing.T, id string, schedulerClient scpb.SchedulerClient) *fakeExecutor {
	node := &scpb.ExecutionNode{
		ExecutorId:            id,
		Os:                    defaultOS,
		Arch:                  defaultArch,
		Host:                  "foo",
		AssignableMemoryBytes: 64_000_000_000,
		AssignableMilliCpu:    32_000,
	}
	ctx = log.EnrichContext(ctx, "executor_id", id)
	ctx, cancel := context.WithCancel(ctx)
	fe := &fakeExecutor{
		t:                 t,
		schedulerClient:   schedulerClient,
		id:                id,
		ctx:               ctx,
		tasks:             make(map[string]task),
		node:              node,
		send:              make(chan *scpb.RegisterAndStreamWorkRequest),
		schedulerMessages: make(chan *scpb.RegisterAndStreamWorkResponse, 128),
	}
	t.Cleanup(func() {
		cancel()
		fe.wait()
	})
	return fe
}

func (e *fakeExecutor) markUnhealthy() {
	e.unhealthy.Store(true)
}

// Send sends a request to the scheduler.
func (e *fakeExecutor) Send(req *scpb.RegisterAndStreamWorkRequest) {
	// Send via channel to the goroutine managing the stream, since it's not
	// safe to send on the stream from multiple goroutines.
	e.send <- req
}

func (e *fakeExecutor) Register() {
	ctx := e.ctx
	stream, err := e.schedulerClient.RegisterAndStreamWork(e.ctx)
	require.NoError(e.t, err)
	err = stream.Send(&scpb.RegisterAndStreamWorkRequest{
		RegisterExecutorRequest: &scpb.RegisterExecutorRequest{
			Node: e.node,
		},
	})
	require.NoError(e.t, err)

	recvChan := make(chan Result[*scpb.RegisterAndStreamWorkResponse], 1)
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			msg, err := stream.Recv()
			recvChan <- Result[*scpb.RegisterAndStreamWorkResponse]{Value: msg, Err: err}
			if err != nil {
				return
			}
		}
	}()
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-recvChan:
				rsp, err := msg.Value, msg.Err
				if status.IsUnavailableError(err) {
					return
				}
				require.NoError(e.t, err)
				log.CtxInfof(ctx, "Received scheduler message: %+v", rsp)
				if e.unhealthy.Load() {
					log.CtxInfof(ctx, "Executor %s got task %q but is unhealthy -- ignoring so it times out", e.id, rsp.GetEnqueueTaskReservationRequest().GetTaskId())
				} else {
					err = stream.Send(&scpb.RegisterAndStreamWorkRequest{
						EnqueueTaskReservationResponse: &scpb.EnqueueTaskReservationResponse{
							TaskId: rsp.GetEnqueueTaskReservationRequest().GetTaskId(),
						},
					})
					require.NoError(e.t, err)
					e.mu.Lock()
					log.CtxInfof(ctx, "Executor %s got task %q with scheduling delay %s", e.id, rsp.GetEnqueueTaskReservationRequest().GetTaskId(), rsp.GetEnqueueTaskReservationRequest().GetDelay())
					taskID := rsp.GetEnqueueTaskReservationRequest().GetTaskId()
					e.tasks[taskID] = task{delay: rsp.GetEnqueueTaskReservationRequest().GetDelay().AsDuration()}
					e.mu.Unlock()
					// Best effort: notify the test of every scheduler reply.
					select {
					case e.schedulerMessages <- rsp:
					default:
					}
				}
			case req := <-e.send:
				err := stream.Send(req)
				require.NoError(e.t, err)
			}
		}
	}()

	// Give the executor a moment to register with the scheduler.
	// TODO: explicitly wait for a scheduler reply.
	time.Sleep(100 * time.Millisecond)
}

func (e *fakeExecutor) wait() {
	e.wg.Wait()
}

func (e *fakeExecutor) WaitForTask(taskID string) {
	e.WaitForTaskWithDelay(taskID, 0*time.Second)
}

func (e *fakeExecutor) WaitForTaskWithDelay(taskID string, delay time.Duration) {
	for i := 0; i < 5; i++ {
		e.mu.Lock()
		task, ok := e.tasks[taskID]
		e.mu.Unlock()
		if ok {
			if task.delay == delay {
				return
			} else {
				require.FailNowf(e.t, "executor received task with unexpected delay", "executor %s task %q expected delay: %s actual delay: %s", e.id, taskID, delay, task.delay)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.FailNowf(e.t, "executor did not receive task", "task %q", taskID)
}

func (e *fakeExecutor) EnsureTaskNotReceived(taskID string) {
	// Allow some time for re-enqueuing, etc. No easy way around this,
	// unfortunately.
	time.Sleep(100 * time.Millisecond)
	e.mu.Lock()
	_, ok := e.tasks[taskID]
	e.mu.Unlock()
	if ok {
		require.FailNowf(e.t, "executor received task but was not expecting it", "task %q", taskID)
	}
}

func (e *fakeExecutor) ResetTasks() {
	e.mu.Lock()
	e.tasks = make(map[string]task)
	e.mu.Unlock()
}

type taskLease struct {
	t       *testing.T
	stream  scpb.Scheduler_LeaseTaskClient
	leaseID string
	taskID  string
	task    *repb.ExecutionTask
}

func (tl *taskLease) Renew() error {
	err := tl.stream.Send(&scpb.LeaseTaskRequest{
		TaskId: tl.taskID,
	})
	if err != nil {
		return err
	}
	_, err = tl.stream.Recv()
	if err != nil {
		return err
	}
	return nil
}

func (tl *taskLease) Finalize() error {
	err := tl.stream.Send(&scpb.LeaseTaskRequest{
		TaskId:   tl.taskID,
		Finalize: true,
	})
	if err != nil {
		return err
	}
	_, err = tl.stream.Recv()
	if err != nil {
		return err
	}
	return nil
}

func (e *fakeExecutor) Claim(taskID string) *taskLease {
	stream, err := e.schedulerClient.LeaseTask(e.ctx)
	require.NoError(e.t, err)
	err = stream.Send(&scpb.LeaseTaskRequest{
		TaskId: taskID,
	})
	require.NoError(e.t, err)
	rsp, err := stream.Recv()
	require.NoError(e.t, err)
	require.NotZero(e.t, rsp.GetLeaseDurationSeconds())

	var task *repb.ExecutionTask
	if len(rsp.GetSerializedTask()) > 0 {
		task = &repb.ExecutionTask{}
		err = proto.Unmarshal(rsp.GetSerializedTask(), task)
		require.NoError(e.t, err)
	}

	lease := &taskLease{
		t:       e.t,
		stream:  stream,
		taskID:  taskID,
		task:    task,
		leaseID: rsp.GetLeaseId(),
	}
	return lease
}

func scheduleTask(ctx context.Context, t *testing.T, env environment.Env, props map[string]string) string {
	id, err := uuid.NewRandom()
	require.NoError(t, err)
	taskID := id.String()

	task := &repb.ExecutionTask{
		ExecutionId: taskID,
		Command: &repb.Command{
			Platform: &repb.Platform{
				Properties: []*repb.Platform_Property{},
			},
		},
	}
	for k, v := range props {
		task.Command.Platform.Properties = append(task.Command.Platform.Properties, &repb.Platform_Property{Name: k, Value: v})
	}
	size := tasksize.Override(tasksize.Default(task), tasksize.Requested(task))
	taskBytes, err := proto.Marshal(task)
	require.NoError(t, err)
	_, err = env.GetSchedulerService().ScheduleTask(ctx, &scpb.ScheduleTaskRequest{
		TaskId: taskID,
		Metadata: &scpb.SchedulingMetadata{
			Os:       defaultOS,
			Arch:     defaultArch,
			TaskSize: size,
		},
		SerializedTask: taskBytes,
	})
	require.NoError(t, err)
	return taskID
}

func enqueueTaskReservation(ctx context.Context, t *testing.T, env environment.Env, delay time.Duration) string {
	id, err := uuid.NewRandom()
	require.NoError(t, err)
	taskID := id.String()

	require.NoError(t, err)
	_, err = env.GetSchedulerService().EnqueueTaskReservation(ctx, &scpb.EnqueueTaskReservationRequest{
		TaskId: taskID,
		TaskSize: &scpb.TaskSize{
			EstimatedMemoryBytes:   100,
			EstimatedMilliCpu:      100,
			EstimatedFreeDiskBytes: 100,
		},
		SchedulingMetadata: &scpb.SchedulingMetadata{
			Os:   defaultOS,
			Arch: defaultArch,
			TaskSize: &scpb.TaskSize{
				EstimatedMemoryBytes:   100,
				EstimatedMilliCpu:      100,
				EstimatedFreeDiskBytes: 100,
			},
		},
		Delay: durationpb.New(delay),
	})
	require.NoError(t, err)
	return taskID
}

func TestExecutorReEnqueue_NoLeaseID(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{}, "user1")

	fe := newFakeExecutor(ctx, t, env.GetSchedulerClient())
	fe.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{})
	fe.WaitForTask(taskID)
	fe.Claim(taskID)

	fe.ResetTasks()
	_, err := env.GetSchedulerClient().ReEnqueueTask(ctx, &scpb.ReEnqueueTaskRequest{
		TaskId: taskID,
		Reason: "for fun",
	})
	require.NoError(t, err)
	// On a successful re-enqueue the executor should receive the task again.
	fe.WaitForTask(taskID)
}

func TestExecutorReEnqueue_MatchingLeaseID(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{}, "user1")

	fe := newFakeExecutor(ctx, t, env.GetSchedulerClient())
	fe.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{})
	fe.WaitForTask(taskID)
	lease := fe.Claim(taskID)

	fe.ResetTasks()
	_, err := env.GetSchedulerClient().ReEnqueueTask(ctx, &scpb.ReEnqueueTaskRequest{
		TaskId:  taskID,
		Reason:  "for fun",
		LeaseId: lease.leaseID,
	})
	require.NoError(t, err)
	// On a successful re-enqueue the executor should receive the task again.
	fe.WaitForTask(taskID)
}

func TestExecutorReEnqueue_NonMatchingLeaseID(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{}, "user1")

	fe := newFakeExecutor(ctx, t, env.GetSchedulerClient())
	fe.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{})
	fe.WaitForTask(taskID)
	fe.Claim(taskID)

	_, err := env.GetSchedulerClient().ReEnqueueTask(ctx, &scpb.ReEnqueueTaskRequest{
		TaskId:  taskID,
		Reason:  "for fun",
		LeaseId: "bad lease ID",
	})
	require.True(t, status.IsPermissionDeniedError(err))
}

func TestExecutorReEnqueue_RetriesDisabled(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{}, "user1")

	fe := newFakeExecutor(ctx, t, env.GetSchedulerClient())
	fe.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{platform.RetryPropertyName: "false"})
	fe.WaitForTask(taskID)
	lease := fe.Claim(taskID)
	fe.ResetTasks()

	_, err := env.GetSchedulerClient().ReEnqueueTask(ctx, &scpb.ReEnqueueTaskRequest{
		TaskId:  taskID,
		Reason:  "for fun",
		LeaseId: lease.leaseID,
	})
	require.NoError(t, err)

	// Ensure the task was never re-enqueued
	fe.EnsureTaskNotReceived(taskID)
}

func TestLeaseExpiration(t *testing.T) {
	fakeClock := clockwork.NewFakeClock()
	env, ctx := getEnv(t, &schedulerOpts{options: Options{
		Clock:            fakeClock,
		LeaseDuration:    10 * time.Second,
		LeaseGracePeriod: 10 * time.Second,
	}}, "user1")

	fe := newFakeExecutor(ctx, t, env.GetSchedulerClient())
	fe.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{})
	fe.WaitForTask(taskID)
	lease := fe.Claim(taskID)

	// Reset known tasks so that we can tell if the task gets re-enqueued.
	fe.ResetTasks()
	// Lease should expire after 20 seconds so there should be no expiration
	// right now.
	fakeClock.Advance(19 * time.Second)
	fe.EnsureTaskNotReceived(taskID)

	// Renew task lease to avoid expiration.
	err := lease.Renew()
	require.NoError(t, err)
	// Get close to expiration, but lease should not expire yet.
	fakeClock.Advance(20 * time.Second)
	fe.EnsureTaskNotReceived(taskID)

	// Move past the grace period. Task should be re-enqueued.
	fakeClock.Advance(2 * time.Second)
	fe.WaitForTask(taskID)

	// Lease renewal should fail as the stream should be broken.
	err = lease.Renew()
	require.ErrorIs(t, io.EOF, err)
}

func TestSchedulingDelay_NoDelay(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{}, "user1")

	fe1 := newFakeExecutorWithId(ctx, t, "1", env.GetSchedulerClient())
	fe2 := newFakeExecutorWithId(ctx, t, "2", env.GetSchedulerClient())
	fe1.Register()
	fe2.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{})

	fe1.WaitForTaskWithDelay(taskID, 0*time.Second)
	fe2.WaitForTaskWithDelay(taskID, 0*time.Second)
}

func TestSchedulingDelay_DelayTooSmall(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{}, "user1")

	fe1 := newFakeExecutorWithId(ctx, t, "1", env.GetSchedulerClient())
	fe2 := newFakeExecutorWithId(ctx, t, "2", env.GetSchedulerClient())
	fe1.Register()
	fe2.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{"runner-recycling-max-wait": "-1s"})

	fe1.WaitForTaskWithDelay(taskID, 0*time.Second)
	fe2.WaitForTaskWithDelay(taskID, 0*time.Second)
}

func TestSchedulingDelay_NoPreferredExecutors(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{}, "user1")

	fe1 := newFakeExecutorWithId(ctx, t, "1", env.GetSchedulerClient())
	fe2 := newFakeExecutorWithId(ctx, t, "2", env.GetSchedulerClient())
	fe1.Register()
	fe2.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{"runner-recycling-max-wait": "5s"})

	fe1.WaitForTaskWithDelay(taskID, 0*time.Second)
	fe2.WaitForTaskWithDelay(taskID, 0*time.Second)
}

func TestSchedulingDelay_DelayTooLarge(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{preferredExecutors: []string{"2"}}, "user1")

	fe1 := newFakeExecutorWithId(ctx, t, "1", env.GetSchedulerClient())
	fe2 := newFakeExecutorWithId(ctx, t, "2", env.GetSchedulerClient())
	fe1.Register()
	fe2.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{"runner-recycling-max-wait": "1h"})

	fe1.WaitForTaskWithDelay(taskID, 5*time.Second)
	fe2.WaitForTaskWithDelay(taskID, 0*time.Second)
}

func TestSchedulingDelay_OnePreferredExecutor(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{preferredExecutors: []string{"2"}}, "user1")

	fe1 := newFakeExecutorWithId(ctx, t, "1", env.GetSchedulerClient())
	fe2 := newFakeExecutorWithId(ctx, t, "2", env.GetSchedulerClient())
	fe1.Register()
	fe2.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{"runner-recycling-max-wait": "5s"})

	fe1.WaitForTaskWithDelay(taskID, 5*time.Second)
	fe2.WaitForTaskWithDelay(taskID, 0*time.Second)
}

func TestSchedulingDelay_PreferredExecutorUnhealthy(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{preferredExecutors: []string{"2"}}, "user1")

	fe1 := newFakeExecutorWithId(ctx, t, "1", env.GetSchedulerClient())
	fe2 := newFakeExecutorWithId(ctx, t, "2", env.GetSchedulerClient())
	fe1.Register()
	fe2.Register()
	fe2.markUnhealthy()

	taskID := scheduleTask(ctx, t, env, map[string]string{"runner-recycling-max-wait": "5s"})

	fe2.EnsureTaskNotReceived(taskID)
	fe1.WaitForTaskWithDelay(taskID, 0*time.Second)
}

func TestEnqueueTaskReservation_DoesntOverwriteDelay(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{}, "user1")

	fe1 := newFakeExecutorWithId(ctx, t, "1", env.GetSchedulerClient())
	fe1.Register()

	taskID := enqueueTaskReservation(ctx, t, env, 3*time.Second)

	fe1.WaitForTaskWithDelay(taskID, 3*time.Second)
}

func TestEnqueueTaskReservation_Exists(t *testing.T) {
	env, ctx := getEnv(t, &schedulerOpts{}, "user1")

	fe := newFakeExecutor(ctx, t, env.GetSchedulerClient())
	fe.Register()

	taskID := scheduleTask(ctx, t, env, map[string]string{platform.RetryPropertyName: "false"})
	fe.WaitForTask(taskID)
	lease := fe.Claim(taskID)

	resp, err := env.GetSchedulerClient().TaskExists(ctx, &scpb.TaskExistsRequest{TaskId: taskID})

	require.Nil(t, err)
	require.True(t, resp.GetExists())

	lease.Finalize()

	resp, err = env.GetSchedulerClient().TaskExists(ctx, &scpb.TaskExistsRequest{TaskId: taskID})

	require.Nil(t, err)
	require.False(t, resp.GetExists())
}

func TestAskForMoreWork_OnlyEnqueuesTasksThatFitOnNode(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env, ctx := getEnv(t, &schedulerOpts{options: Options{Clock: clock}}, "user1")

	// Register two nodes with different capacities.
	largeExecutor := newFakeExecutorWithId(ctx, t, "large", env.GetSchedulerClient())
	largeExecutor.node.AssignableMilliCpu = 32_000
	largeExecutor.Register()

	smallExecutor := newFakeExecutorWithId(ctx, t, "small", env.GetSchedulerClient())
	smallExecutor.node.AssignableMilliCpu = 1000
	smallExecutor.Register()

	var rsp *scpb.RegisterAndStreamWorkResponse

	// Schedule a task that only fits on largeExecutor.
	taskID := scheduleTask(ctx, t, env, map[string]string{"EstimatedCPU": "8000m"})
	// Ensure the task was enqueued on largeExecutor, but don't have
	// largeExecutor claim the task, so that it's eligible to be enqueued
	// as part of AskForMoreWork.
	rsp = <-largeExecutor.schedulerMessages
	require.Equal(t, taskID, rsp.GetEnqueueTaskReservationRequest().GetTaskId())

	// Now have largeExecutor ask for more work. The scheduler should enqueue
	// the unclaimed task.
	largeExecutor.Send(&scpb.RegisterAndStreamWorkRequest{
		AskForMoreWorkRequest: &scpb.AskForMoreWorkRequest{},
	})
	// Make sure we get an EnqueueTaskReservationRequest. The scheduler doesn't
	// know what tasks are currently enqueued on largeExecutor, so it's fair
	// to expect the task to be enqueued again.
	// Note: we don't expect an AskForMoreWorkResponse here - the scheduler only
	// sends a response to increase the client backoff after enqueuing 0 tasks.
	rsp = <-largeExecutor.schedulerMessages
	require.Equal(t, taskID, rsp.GetEnqueueTaskReservationRequest().GetTaskId())

	// Have smallExecutor ask for more work now. The scheduler should not
	// schedule any work on smallExecutor because the task doesn't fit. It
	// should only reply with an AskForMoreWorkResponse to increase the client
	// backoff.
	smallExecutor.Send(&scpb.RegisterAndStreamWorkRequest{
		AskForMoreWorkRequest: &scpb.AskForMoreWorkRequest{},
	})
	rsp = <-smallExecutor.schedulerMessages
	require.Greater(t, rsp.GetAskForMoreWorkResponse().GetDelay().AsDuration(), time.Duration(0))
}

func TestAskForMoreWork_RespectRequestedExecutorID(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env, ctx := getEnv(t, &schedulerOpts{options: Options{Clock: clock}}, "user1")

	// Register two nodes.
	executor1 := newFakeExecutorWithId(ctx, t, "n1", env.GetSchedulerClient())
	executor1.node.AssignableMilliCpu = 1000
	executor1.Register()

	executor2 := newFakeExecutorWithId(ctx, t, "n2", env.GetSchedulerClient())
	executor2.node.AssignableMilliCpu = 1000
	executor2.Register()

	var rsp *scpb.RegisterAndStreamWorkResponse

	// Schedule a task on executor1.
	taskID := scheduleTask(ctx, t, env, map[string]string{"debug-executor-id": "n1"})
	// Ensure the task was enqueued on executor1, but don't have
	// executor1 claim the task, so that it's eligible to be enqueued
	// as part of AskForMoreWork.
	rsp = <-executor1.schedulerMessages
	require.Equal(t, taskID, rsp.GetEnqueueTaskReservationRequest().GetTaskId())

	// Have executor2 ask for more work now. The scheduler should not
	// schedule any work on executor2 because the task specifically requested
	// executor1. It should only reply with an AskForMoreWorkResponse to increase
	// the client backoff.
	executor2.Send(&scpb.RegisterAndStreamWorkRequest{
		AskForMoreWorkRequest: &scpb.AskForMoreWorkRequest{},
	})
	rsp = <-executor2.schedulerMessages
	require.Greater(t, rsp.GetAskForMoreWorkResponse().GetDelay().AsDuration(), time.Duration(0))
}

func TestGetExecutionNodes(t *testing.T) {
	clock := clockwork.NewFakeClock()
	env, ctx := getEnv(t, &schedulerOpts{options: Options{Clock: clock}}, "user1")
	enterprise_testauth.Configure(t, env)

	// Register several nodes with different IDs
	for _, hostID := range []string{"z", "m", "a"} {
		executor := newFakeExecutorWithId(ctx, t, hostID, env.GetSchedulerClient())
		executor.Register()
	}

	u := enterprise_testauth.CreateRandomUser(t, env, "org1.invalid")
	g := u.Groups[0].Group
	groupID := g.GroupID

	auther := env.GetAuthenticator().(*testauth.TestAuthenticator)
	authCtx, err := auther.WithAuthenticatedUser(ctx, u.UserID)
	require.NoError(t, err)

	rsp, err := env.GetSchedulerService().GetExecutionNodes(authCtx, &scpb.GetExecutionNodesRequest{
		RequestContext: &ctxpb.RequestContext{
			GroupId: groupID,
		},
	})
	require.NoError(t, err)

	// Ensure that executors are sorted by host id.
	for i, executor := range rsp.GetExecutor() {
		if i > 0 {
			last := rsp.GetExecutor()[i-1]
			require.GreaterOrEqual(t, executor.GetNode().GetHost(), last.GetNode().GetHost())
		}
	}
}
