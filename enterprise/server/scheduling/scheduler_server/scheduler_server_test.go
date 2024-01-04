package scheduler_server

import (
	"context"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/enterprise_testenv"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/log"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
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

func (f *fakeTaskRouter) RankNodes(ctx context.Context, cmd *repb.Command, remoteInstanceName string, nodes []interfaces.ExecutionNode) []interfaces.RankedExecutionNode {
	rankedNodes := make([]interfaces.RankedExecutionNode, len(nodes))
	for i, node := range nodes {
		preferred := false
		for _, preferredNodeID := range f.preferredExecutors {
			if node.GetExecutorID() == preferredNodeID {
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
		return nodes[i].GetExecutorID() < nodes[j].GetExecutorID()
	})
	return rankedNodes
}

func (f *fakeTaskRouter) MarkComplete(ctx context.Context, cmd *repb.Command, remoteInstanceName, executorInstanceID string) {
}

type schedulerOpts struct {
	userOwnedEnabled   bool
	groupOwnedEnabled  bool
	clock              clockwork.Clock
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
	s, err := NewSchedulerServerWithOptions(env, &Options{Clock: opts.clock})
	require.NoError(t, err)
	env.SetSchedulerService(s)

	server, runFunc := testenv.RegisterLocalGRPCServer(env)
	scpb.RegisterSchedulerServer(server, env.GetSchedulerService())
	go runFunc()
	t.Cleanup(func() { server.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clientConn, err := testenv.LocalGRPCConn(ctx, env)
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
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestSchedulerServerGetPoolInfoNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestSchedulerServerGetPoolInfoWithOS(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "user1")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)

	p, err = s.GetPoolInfo(ctx, "darwin", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)

	s.forceUserOwnedDarwinExecutors = true
	p, err = s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)

	p, err = s.GetPoolInfo(ctx, "darwin", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)
}

func TestSchedulerServerGetPoolInfoWithRequestedPoolWithAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, true, "user1")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "my-pool", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "my-pool", p.Name)
}

func TestSchedulerServerGetPoolInfoWithRequestedPoolWithNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	s.forceUserOwnedDarwinExecutors = false
	p, err := s.GetPoolInfo(ctx, "linux", "my-pool", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "my-pool", p.Name)
}

func TestSchedulerServerGetPoolInfoDarwin(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "defaultPoolName", p.Name)
}

func TestSchedulerServerGetPoolInfoDarwinNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	s.forceUserOwnedDarwinExecutors = true
	_, err := s.GetPoolInfo(ctx, "darwin", "", "" /*=workflowID*/, false)
	require.Error(t, err)
}

func TestSchedulerServerGetPoolInfoSelfHostedNoAuth(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "")
	_, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, true)
	require.Error(t, err)
}

func TestSchedulerServerGetPoolInfoSelfHosted(t *testing.T) {
	s, ctx := getScheduleServer(t, true, false, "user1")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	// Linux workflows should respect useSelfHosted bool.
	p, err = s.GetPoolInfo(ctx, "linux", "workflows", "WF1234" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "workflows", p.Name)

	p, err = s.GetPoolInfo(ctx, "linux", "workflows", "WF1234" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "workflows", p.Name)
}

func TestSchedulerServerGetPoolInfoSelfHostedByDefault(t *testing.T) {
	s, ctx := getScheduleServer(t, true, true, "user1")
	p, err := s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	p, err = s.GetPoolInfo(ctx, "linux", "", "" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "", p.Name)

	// Linux workflows should respect useSelfHosted bool.
	p, err = s.GetPoolInfo(ctx, "linux", "workflows", "WF1234" /*=workflowID*/, false)
	require.NoError(t, err)
	require.Equal(t, "sharedGroupID", p.GroupID)
	require.Equal(t, "workflows", p.Name)

	p, err = s.GetPoolInfo(ctx, "linux", "workflows", "WF1234" /*=workflowID*/, true)
	require.NoError(t, err)
	require.Equal(t, "group1", p.GroupID)
	require.Equal(t, "workflows", p.Name)
}

type task struct {
	delay time.Duration
}

type fakeExecutor struct {
	t               *testing.T
	schedulerClient scpb.SchedulerClient

	id string

	ctx context.Context

	unhealthy atomic.Bool

	mu    sync.Mutex
	tasks map[string]task
}

func newFakeExecutor(ctx context.Context, t *testing.T, schedulerClient scpb.SchedulerClient) *fakeExecutor {
	id, err := uuid.NewRandom()
	require.NoError(t, err)
	return newFakeExecutorWithId(ctx, t, id.String(), schedulerClient)
}

func newFakeExecutorWithId(ctx context.Context, t *testing.T, id string, schedulerClient scpb.SchedulerClient) *fakeExecutor {
	return &fakeExecutor{
		t:               t,
		schedulerClient: schedulerClient,
		id:              id,
		ctx:             ctx,
		tasks:           make(map[string]task),
	}
}

func (e *fakeExecutor) markUnhealthy() {
	e.unhealthy.Store(true)
}

func (e *fakeExecutor) Register() {
	stream, err := e.schedulerClient.RegisterAndStreamWork(e.ctx)
	require.NoError(e.t, err)
	err = stream.Send(&scpb.RegisterAndStreamWorkRequest{
		RegisterExecutorRequest: &scpb.RegisterExecutorRequest{
			Node: &scpb.ExecutionNode{
				ExecutorId:            e.id,
				Os:                    defaultOS,
				Arch:                  defaultArch,
				Host:                  "foo",
				AssignableMemoryBytes: 1000000,
				AssignableMilliCpu:    1000000,
			}},
	})
	require.NoError(e.t, err)

	go func() {
		for {
			req, err := stream.Recv()
			if status.IsUnavailableError(err) {
				return
			}
			require.NoError(e.t, err)
			log.Infof("received req: %+v", req)
			if e.unhealthy.Load() {
				log.Infof("executor %s got task %q but is unhealthy -- ignoring so it times out", e.id, req.GetEnqueueTaskReservationRequest().GetTaskId())
			} else {
				err = stream.Send(&scpb.RegisterAndStreamWorkRequest{
					EnqueueTaskReservationResponse: &scpb.EnqueueTaskReservationResponse{
						TaskId: req.GetEnqueueTaskReservationRequest().GetTaskId(),
					},
				})
				require.NoError(e.t, err)
				e.mu.Lock()
				log.Infof("executor %s got task %q with scheduling delay %s", e.id, req.GetEnqueueTaskReservationRequest().GetTaskId(), req.GetEnqueueTaskReservationRequest().GetDelay())
				e.tasks[req.GetEnqueueTaskReservationRequest().GetTaskId()] = task{delay: req.GetEnqueueTaskReservationRequest().GetDelay().AsDuration()}
				e.mu.Unlock()
			}
		}
	}()

	// Give the executor a moment to register with the scheduler.
	time.Sleep(100 * time.Millisecond)
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
	taskID  string
	leaseID string
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

	lease := &taskLease{
		t:       e.t,
		stream:  stream,
		taskID:  taskID,
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
	taskBytes, err := proto.Marshal(task)
	require.NoError(t, err)
	_, err = env.GetSchedulerService().ScheduleTask(ctx, &scpb.ScheduleTaskRequest{
		TaskId: taskID,
		Metadata: &scpb.SchedulingMetadata{
			Os:   defaultOS,
			Arch: defaultArch,
			TaskSize: &scpb.TaskSize{
				EstimatedMemoryBytes:   100,
				EstimatedMilliCpu:      100,
				EstimatedFreeDiskBytes: 100,
			},
		},
		SerializedTask: taskBytes,
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

func TestLeaseExpiration(t *testing.T) {
	flags.Set(t, "remote_execution.lease_duration", 10*time.Second)
	flags.Set(t, "remote_execution.lease_grace_period", 10*time.Second)

	fakeClock := clockwork.NewFakeClock()
	env, ctx := getEnv(t, &schedulerOpts{clock: fakeClock}, "user1")

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

	fe1.WaitForTaskWithDelay(taskID, 15*time.Minute)
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
