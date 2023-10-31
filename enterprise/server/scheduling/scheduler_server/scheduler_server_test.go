package scheduler_server

import (
	"context"
	"sync"
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
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
)

const (
	defaultOS   = "linux"
	defaultArch = "amd64"
)

type fakeTaskRouter struct {
}

func (f *fakeTaskRouter) RankNodes(ctx context.Context, cmd *repb.Command, remoteInstanceName string, nodes []interfaces.ExecutionNode) []interfaces.ExecutionNode {
	return nodes
}

func (f *fakeTaskRouter) MarkComplete(ctx context.Context, cmd *repb.Command, remoteInstanceName, executorInstanceID string) {
}

func getEnv(t *testing.T, userOwnedEnabled, groupOwnedEnabled bool, user string) (*testenv.TestEnv, context.Context) {
	redisTarget := testredis.Start(t).Target
	env := enterprise_testenv.GetCustomTestEnv(t, &enterprise_testenv.Options{
		RedisTarget: redisTarget,
	})

	flags.Set(t, "remote_execution.default_pool_name", "defaultPoolName")
	flags.Set(t, "remote_execution.shared_executor_pool_group_id", "sharedGroupID")

	err := execution_server.Register(env)
	require.NoError(t, err)
	env.SetTaskRouter(&fakeTaskRouter{})
	s, err := NewSchedulerServer(env)
	require.NoError(t, err)
	env.SetSchedulerService(s)

	server, runFunc := env.LocalGRPCServer()
	scpb.RegisterSchedulerServer(server, env.GetSchedulerService())
	go runFunc()
	t.Cleanup(func() { server.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clientConn, err := env.LocalGRPCConn(ctx)
	require.NoError(t, err)
	sc := scpb.NewSchedulerClient(clientConn)
	env.SetSchedulerClient(sc)

	testUsers := make(map[string]interfaces.UserInfo, 0)
	testUsers["user1"] = &testauth.TestUser{UserID: "user1", GroupID: "group1", UseGroupOwnedExecutors: groupOwnedEnabled}

	ta := testauth.NewTestAuthenticator(testUsers)
	env.SetAuthenticator(ta)
	s.enableUserOwnedExecutors = userOwnedEnabled

	if user != "" {
		authenticatedCtx, err := ta.WithAuthenticatedUser(context.Background(), user)
		require.NoError(t, err)
		ctx = authenticatedCtx
	}
	return env, ctx
}

func getScheduleServer(t *testing.T, userOwnedEnabled, groupOwnedEnabled bool, user string) (*SchedulerServer, context.Context) {
	env, ctx := getEnv(t, userOwnedEnabled, groupOwnedEnabled, user)
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

type fakeExecutor struct {
	t               *testing.T
	schedulerClient scpb.SchedulerClient

	ctx context.Context

	mu    sync.Mutex
	tasks map[string]struct{}
}

func newFakeExecutor(ctx context.Context, t *testing.T, schedulerClient scpb.SchedulerClient) *fakeExecutor {
	return &fakeExecutor{
		t:               t,
		schedulerClient: schedulerClient,
		ctx:             ctx,
		tasks:           make(map[string]struct{}),
	}
}

func (e *fakeExecutor) Register() {
	stream, err := e.schedulerClient.RegisterAndStreamWork(e.ctx)
	require.NoError(e.t, err)
	go func() {
		err = stream.Send(&scpb.RegisterAndStreamWorkRequest{
			RegisterExecutorRequest: &scpb.RegisterExecutorRequest{
				Node: &scpb.ExecutionNode{
					Os:                    defaultOS,
					Arch:                  defaultArch,
					Host:                  "foo",
					AssignableMemoryBytes: 1000000,
					AssignableMilliCpu:    1000000,
				}},
		})
		require.NoError(e.t, err)
		for {
			req, err := stream.Recv()
			if status.IsUnavailableError(err) {
				return
			}
			require.NoError(e.t, err)
			log.Infof("received req: %+v", req)
			err = stream.Send(&scpb.RegisterAndStreamWorkRequest{
				EnqueueTaskReservationResponse: &scpb.EnqueueTaskReservationResponse{
					TaskId: req.GetEnqueueTaskReservationRequest().GetTaskId(),
				},
			})
			require.NoError(e.t, err)
			e.mu.Lock()
			log.Infof("got task %q", req.GetEnqueueTaskReservationRequest().GetTaskId())
			e.tasks[req.GetEnqueueTaskReservationRequest().GetTaskId()] = struct{}{}
			e.mu.Unlock()
		}
	}()
}

func (e *fakeExecutor) WaitForTask(taskID string) {
	for i := 0; i < 5; i++ {
		e.mu.Lock()
		_, ok := e.tasks[taskID]
		e.mu.Unlock()
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.FailNowf(e.t, "executor did not receive task", "task %q", taskID)
}

func (e *fakeExecutor) ResetTasks() {
	e.mu.Lock()
	e.tasks = make(map[string]struct{})
	e.mu.Unlock()
}

type taskLease struct {
	stream  scpb.Scheduler_LeaseTaskClient
	leaseID string
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
		stream:  stream,
		leaseID: rsp.GetLeaseId(),
	}
	return lease
}

func scheduleTask(ctx context.Context, t *testing.T, env environment.Env) string {
	for i := 0; i < 5; i++ {
		id, err := uuid.NewRandom()
		require.NoError(t, err)
		taskID := id.String()

		task := &repb.ExecutionTask{
			ExecutionId: taskID,
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
		// Allow time for executor to register.
		if status.IsUnavailableError(err) {
			time.Sleep(100 * time.Millisecond)
			continue
		}
		require.NoError(t, err)
		if err == nil {
			return taskID
		}
	}
	require.FailNow(t, "could not schedule task")
	return ""
}

func TestExecutorReEnqueue_NoLeaseID(t *testing.T) {
	env, ctx := getEnv(t, true, true, "user1")

	fe := newFakeExecutor(ctx, t, env.GetSchedulerClient())
	fe.Register()

	taskID := scheduleTask(ctx, t, env)
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
	env, ctx := getEnv(t, true, true, "user1")

	fe := newFakeExecutor(ctx, t, env.GetSchedulerClient())
	fe.Register()

	taskID := scheduleTask(ctx, t, env)
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
	env, ctx := getEnv(t, true, true, "user1")

	fe := newFakeExecutor(ctx, t, env.GetSchedulerClient())
	fe.Register()

	taskID := scheduleTask(ctx, t, env)
	fe.WaitForTask(taskID)
	fe.Claim(taskID)

	_, err := env.GetSchedulerClient().ReEnqueueTask(ctx, &scpb.ReEnqueueTaskRequest{
		TaskId:  taskID,
		Reason:  "for fun",
		LeaseId: "bad lease ID",
	})
	require.True(t, status.IsPermissionDeniedError(err))
}
