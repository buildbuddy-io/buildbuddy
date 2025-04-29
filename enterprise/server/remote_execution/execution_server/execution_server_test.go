package execution_server_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/execution_server"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/remote_execution/operation"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/tasksize"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/execution"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/util/redisutil"
	"github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	"github.com/buildbuddy-io/buildbuddy/server/environment"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/go-redis/redis/v8"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
	gstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"
	tspb "google.golang.org/protobuf/types/known/timestamppb"
)

const (
	sharedPoolGroupID     = "GR000"
	selfHostedPoolGroupID = "GR123"
)

type schedulerServerMock struct {
	interfaces.SchedulerService

	canceledCount int
	scheduleReqs  []*scpb.ScheduleTaskRequest
}

func (s *schedulerServerMock) GetPoolInfo(_ context.Context, _ string, _ string, _ string, poolType interfaces.PoolType) (*interfaces.PoolInfo, error) {
	groupID := sharedPoolGroupID
	if poolType == interfaces.PoolTypeSelfHosted {
		groupID = selfHostedPoolGroupID
	}
	return &interfaces.PoolInfo{
		GroupID:      groupID,
		IsSelfHosted: poolType == interfaces.PoolTypeSelfHosted,
	}, nil
}
func (s *schedulerServerMock) GetSharedExecutorPoolGroupID() string {
	return sharedPoolGroupID
}

func (s *schedulerServerMock) ScheduleTask(ctx context.Context, req *scpb.ScheduleTaskRequest) (*scpb.ScheduleTaskResponse, error) {
	s.scheduleReqs = append(s.scheduleReqs, req)
	return &scpb.ScheduleTaskResponse{}, nil
}

func (s *schedulerServerMock) CancelTask(ctx context.Context, taskID string) (bool, error) {
	s.canceledCount++
	return true, nil
}

func setupEnv(t *testing.T) (*testenv.TestEnv, *grpc.ClientConn) {
	env := testenv.GetTestEnv(t)

	env.SetAuthenticator(testauth.NewTestAuthenticator(testauth.TestUsers("US1", "GR1")))

	redisTarget := testredis.Start(t).Target
	rdb := redis.NewClient(redisutil.TargetToOptions(redisTarget))
	env.SetRemoteExecutionRedisClient(rdb)
	env.SetRemoteExecutionRedisPubSubClient(rdb)

	scheduler := &schedulerServerMock{}
	env.SetSchedulerService(scheduler)

	tasksize.Register(env)

	s, err := execution_server.NewExecutionServer(env)
	require.NoError(t, err)
	env.SetRemoteExecutionService(s)
	env.SetUsageTracker(&fakeUsageTracker{})

	_, run, lis := testenv.RegisterLocalGRPCServer(t, env)
	testcache.Setup(t, env, lis)
	repb.RegisterExecutionServer(env.GetGRPCServer(), env.GetRemoteExecutionService())
	go run()

	conn, err := testenv.LocalGRPCConn(env.GetServerContext(), lis)
	require.NoError(t, err)
	return env, conn
}

func createExecution(ctx context.Context, t *testing.T, db interfaces.DB, execution *tables.Execution) {
	err := db.NewQuery(ctx, "create_execution").Create(execution)
	require.NoError(t, err)
}

func getExecutions(t *testing.T, env environment.Env) []*tables.Execution {
	rq := env.GetDBHandle().NewQuery(env.GetServerContext(), "get_executions").Raw(`SELECT * FROM Executions`)
	out, err := db.ScanAll(rq, &tables.Execution{})
	require.NoError(t, err)
	return out
}

func createInvocation(ctx context.Context, t *testing.T, db interfaces.DB, inv *tables.Invocation) {
	err := db.NewQuery(ctx, "create_invocation").Create(inv)
	require.NoError(t, err)
}

func TestDispatch(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	const iid = "10243d8a-a329-4f46-abfb-bfbceed12baa"
	ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
		ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
		ToolInvocationId: iid,
	})
	ctx, err := env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	arn := uploadAction(ctx, t, env, "" /*=instanceName*/, repb.DigestFunction_SHA256, &repb.Action{})
	ad := arn.GetDigest()

	// note: AttachUserPrefix is normally done by Execute(), which wraps
	// Dispatch().
	ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)
	taskID, err := s.Dispatch(ctx, &repb.ExecuteRequest{ActionDigest: ad})
	require.NoError(t, err)

	rn, err := digest.ParseUploadResourceName(taskID)
	require.NoError(t, err)
	assert.Equal(t, "", rn.GetInstanceName(), "instance name mismatch")
	assert.Equal(t, ad.GetHash(), rn.GetDigest().GetHash(), "action hash mismatch")

	rows := getExecutions(t, env)
	require.Equal(t, 1, len(rows))
	assert.Equal(t, taskID, rows[0].ExecutionID)
	assert.Equal(t, "US1", rows[0].UserID)

	sched := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, len(sched.scheduleReqs))
	b := sched.scheduleReqs[0].SerializedTask
	task := &repb.ExecutionTask{}
	err = proto.Unmarshal(b, task)
	require.NoError(t, err)
	assert.Nil(t, task.GetRequestMetadata().GetToolDetails(), "ToolDetails should be nil")
	assert.Equal(t, iid, task.GetRequestMetadata().GetToolInvocationId(), "invocation ID should be passed along")
}

func TestCancel(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	inv := &tables.Invocation{
		InvocationID:     testInvocationID,
		Attempt:          3, // Test it works for non-first attempts
		InvocationStatus: int64(invocation_status.InvocationStatus_PARTIAL_INVOCATION_STATUS),
		Perms:            perms.OTHERS_READ,
	}
	createInvocation(ctx, t, env.GetDBHandle(), inv)

	// Create Execution rows to be canceled
	executionID := "blobs/1111111111111111111111111111111111111111111111111111111111111111/100"
	execution := &tables.Execution{
		ExecutionID:  executionID,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	createExecution(ctx, t, env.GetDBHandle(), execution)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, schedulerMock.canceledCount)

	inv, err = env.GetInvocationDB().LookupInvocation(ctx, testInvocationID)
	require.NoError(t, err)
	require.Equal(t, int64(invocation_status.InvocationStatus_DISCONNECTED_INVOCATION_STATUS), inv.InvocationStatus)
	// Check that non-status related fields were not affected on the invocation
	require.Equal(t, int32(perms.OTHERS_READ), inv.Perms)
}

func TestCancel_SkipCompletedExecution(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	inv := &tables.Invocation{
		InvocationID:     testInvocationID,
		Attempt:          0,
		InvocationStatus: int64(invocation_status.InvocationStatus_PARTIAL_INVOCATION_STATUS),
		Perms:            perms.OTHERS_READ,
	}
	createInvocation(ctx, t, env.GetDBHandle(), inv)

	// Create Execution rows to be canceled
	executionID1 := "blobs/1111111111111111111111111111111111111111111111111111111111111111/100"
	executionID2 := "blobs/2111111111111111111111111111111111111111111111111111111111111111/100"
	completeExecution := &tables.Execution{
		ExecutionID:  executionID1,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_COMPLETED),
	}
	incompleteExecution := &tables.Execution{
		ExecutionID:  executionID2,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	createExecution(ctx, t, env.GetDBHandle(), completeExecution)
	createExecution(ctx, t, env.GetDBHandle(), incompleteExecution)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, schedulerMock.canceledCount)
}

func TestCancel_MultipleExecutions(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	inv := &tables.Invocation{
		InvocationID:     testInvocationID,
		Attempt:          3,
		InvocationStatus: int64(invocation_status.InvocationStatus_PARTIAL_INVOCATION_STATUS),
		Perms:            perms.OTHERS_READ,
	}
	createInvocation(ctx, t, env.GetDBHandle(), inv)

	// Create Execution rows to be canceled
	executionID1 := "blobs/1111111111111111111111111111111111111111111111111111111111111111/100"
	executionID2 := "blobs/2111111111111111111111111111111111111111111111111111111111111111/100"
	executionID3 := "blobs/3111111111111111111111111111111111111111111111111111111111111111/100"
	completeExecution := &tables.Execution{
		ExecutionID:  executionID1,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_COMPLETED),
	}
	incompleteExecution1 := &tables.Execution{
		ExecutionID:  executionID2,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	incompleteExecution2 := &tables.Execution{
		ExecutionID:  executionID3,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	createExecution(ctx, t, env.GetDBHandle(), completeExecution)
	createExecution(ctx, t, env.GetDBHandle(), incompleteExecution1)
	createExecution(ctx, t, env.GetDBHandle(), incompleteExecution2)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 2, schedulerMock.canceledCount)
}

func TestCancel_InvocationAlreadyCompleted(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	// Create Execution rows to be canceled
	executionID := "blobs/1111111111111111111111111111111111111111111111111111111111111111/100"
	execution := &tables.Execution{
		ExecutionID:  executionID,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	createExecution(ctx, t, env.GetDBHandle(), execution)

	// Simulate a race condition where the invocation is marked as completed
	// while the executions are being canceled.
	inv := &tables.Invocation{
		InvocationID:     testInvocationID,
		Attempt:          3,
		InvocationStatus: int64(invocation_status.InvocationStatus_COMPLETE_INVOCATION_STATUS),
		Perms:            perms.OTHERS_READ,
	}
	createInvocation(ctx, t, env.GetDBHandle(), inv)

	err = s.Cancel(ctx, testInvocationID)
	require.NoError(t, err)

	schedulerMock := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, schedulerMock.canceledCount)

	inv, err = env.GetInvocationDB().LookupInvocation(ctx, testInvocationID)
	require.NoError(t, err)
	// Check that we don't overwrite the completed status if it occurred before.
	require.Equal(t, int64(invocation_status.InvocationStatus_COMPLETE_INVOCATION_STATUS), inv.InvocationStatus)
}

type usage struct {
	labels tables.UsageLabels
	counts tables.UsageCounts
}

type fakeUsageTracker struct {
	interfaces.UsageTracker

	usages []usage
}

func (ut *fakeUsageTracker) Increment(ctx context.Context, labels *tables.UsageLabels, counts *tables.UsageCounts) error {
	ut.usages = append(ut.usages, usage{*labels, *counts})
	return nil
}

func TestExecuteAndPublishOperation(t *testing.T) {
	durationUsec := (5 * time.Second).Microseconds()
	for _, test := range []publishTest{
		{
			name:                   "SharedExecutors",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
		},
		{
			name:                   "SelfHostedExecutors",
			platformOverrides:      map[string]string{"use-self-hosted-executors": "true"},
			expectedSelfHosted:     true,
			expectedExecutionUsage: tables.UsageCounts{SelfHostedLinuxExecutionDurationUsec: durationUsec},
		},
		{
			name:                   "CachedResult",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
			cachedResult:           true,
		},
		{
			name:                   "DoNotCache",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
			doNotCache:             true,
		},
		{
			name:                   "FailedAction",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
			exitCode:               42,
		},
		{
			name:                   "FailedExecution",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
			status:                 status.AbortedError("foo"),
		},
		{
			name:                   "PublishMoreMetadata",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
			publishMoreMetadata:    true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			testExecuteAndPublishOperation(t, test)
		})
	}
}

type publishTest struct {
	name                     string
	platformOverrides        map[string]string
	expectedSelfHosted       bool
	expectedExecutionUsage   tables.UsageCounts
	cachedResult, doNotCache bool
	status                   error
	exitCode                 int32
	publishMoreMetadata      bool
}

type fakeCollector struct {
	interfaces.ExecutionCollector
	invocationLinks []*sipb.StoredInvocationLink
	executions      []*repb.StoredExecution
}

func (fc *fakeCollector) DeleteInvocationLinks(_ context.Context, _ string) error {
	return nil
}

func (fc *fakeCollector) AddInvocationLink(_ context.Context, link *sipb.StoredInvocationLink) error {
	fc.invocationLinks = append(fc.invocationLinks, link)
	return nil
}

func (fc *fakeCollector) GetInvocationLinks(_ context.Context, executionID string) ([]*sipb.StoredInvocationLink, error) {
	var res []*sipb.StoredInvocationLink
	for _, link := range fc.invocationLinks {
		if link.GetExecutionId() == executionID {
			res = append(res, link)
		}
	}
	return res, nil
}

func (fc *fakeCollector) GetInvocation(_ context.Context, invocationID string) (*sipb.StoredInvocation, error) {
	// return nil to always force AppendExecution calls.
	return nil, nil
}

func (fc *fakeCollector) AppendExecution(_ context.Context, _ string, execution *repb.StoredExecution) error {
	fc.executions = append(fc.executions, execution)
	return nil
}

func testExecuteAndPublishOperation(t *testing.T, test publishTest) {
	ctx := context.Background()
	flags.Set(t, "app.enable_write_executions_to_olap_db", true)
	env, conn := setupEnv(t)
	execCollector := new(fakeCollector)
	env.SetExecutionCollector(execCollector)
	client := repb.NewExecutionClient(conn)

	const instanceName = "test-instance"
	const invocationID = "93383cc1-5d6c-4ad1-a321-8ee87c2f6816"
	const digestFunction = repb.DigestFunction_SHA256

	// Schedule execution
	clientCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
		ToolInvocationId: invocationID,
		TargetId:         "//some:test",
		ActionMnemonic:   "TestRunner",
	})
	require.NoError(t, err)
	for k, v := range test.platformOverrides {
		clientCtx = metadata.AppendToOutgoingContext(clientCtx, "x-buildbuddy-platform."+k, v)
	}
	arn := uploadAction(clientCtx, t, env, instanceName, digestFunction, &repb.Action{
		Timeout:    &durationpb.Duration{Seconds: 10},
		DoNotCache: test.doNotCache,
		Platform: &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "EstimatedComputeUnits", Value: "2.5"},
			{Name: "EstimatedFreeDiskBytes", Value: "1000"},
			{Name: "EstimatedCPU", Value: "1.5"},
			{Name: "EstimatedMemory", Value: "2000"},
			{Name: "workload-isolation-type", Value: "oci"},
		}},
	})
	executionClient, err := client.Execute(clientCtx, &repb.ExecuteRequest{
		InstanceName:   arn.GetInstanceName(),
		ActionDigest:   arn.GetDigest(),
		DigestFunction: arn.GetDigestFunction(),
	})
	require.NoError(t, err)
	err = executionClient.CloseSend()
	require.NoError(t, err)
	// Wait for execution to be accepted by the server. This also gives us
	// the task ID.
	op, err := executionClient.Recv()
	require.NoError(t, err)
	taskID := op.GetName()

	// Simulate execution: set up a PublishOperation stream and publish an
	// ExecuteResponse to it.
	executorCtx := metadata.AppendToOutgoingContext(clientCtx, usageutil.ClientHeaderName, "executor")
	executorCtx = metadata.AppendToOutgoingContext(executorCtx, "x-buildbuddy-executor-region", "test-region")
	require.NoError(t, err)
	stream, err := client.PublishOperation(executorCtx)
	require.NoError(t, err)
	queuedTime := time.Unix(100, 0)
	workerStartTime := queuedTime.Add(1 * time.Second)
	workerEndTime := workerStartTime.Add(5 * time.Second)
	aux := &espb.ExecutionAuxiliaryMetadata{PlatformOverrides: &repb.Platform{}}
	for k, v := range test.platformOverrides {
		aux.PlatformOverrides.Properties = append(
			aux.PlatformOverrides.Properties,
			&repb.Platform_Property{Name: k, Value: v},
		)
	}

	executorGroupID := sharedPoolGroupID
	if test.expectedSelfHosted {
		executorGroupID = selfHostedPoolGroupID
	}

	usageStats := &repb.UsageStats{}
	if test.publishMoreMetadata {
		aux.IsolationType = "firecracker"
		aux.Timeout = &durationpb.Duration{Seconds: 11}
		aux.ExecuteRequest = &repb.ExecuteRequest{
			SkipCacheLookup: true, // This is only used for writing to clickhouse
			ExecutionPolicy: &repb.ExecutionPolicy{Priority: 999},
		}
		aux.SchedulingMetadata = &scpb.SchedulingMetadata{
			TaskSize: &scpb.TaskSize{EstimatedFreeDiskBytes: 1001},
			MeasuredTaskSize: &scpb.TaskSize{
				EstimatedMemoryBytes:   2001,
				EstimatedMilliCpu:      2002,
				EstimatedFreeDiskBytes: 2003,
			},
			PredictedTaskSize: &scpb.TaskSize{
				EstimatedMemoryBytes:   3001,
				EstimatedMilliCpu:      3002,
				EstimatedFreeDiskBytes: 3003,
			},
			ExecutorGroupId: executorGroupID,
		}
		usageStats.Timeline = &repb.UsageTimeline{
			StartTime: tspb.New(workerStartTime),
		}
	} else {
		aux.SchedulingMetadata = &scpb.SchedulingMetadata{
			ExecutorGroupId: executorGroupID,
		}
	}
	auxAny, err := anypb.New(aux)
	require.NoError(t, err)
	actionResult := &repb.ActionResult{
		ExitCode:  test.exitCode,
		StderrRaw: []byte("test-stderr"),
		ExecutionMetadata: &repb.ExecutedActionMetadata{
			QueuedTimestamp:          tspb.New(queuedTime),
			WorkerStartTimestamp:     tspb.New(workerStartTime),
			WorkerCompletedTimestamp: tspb.New(workerEndTime),
			AuxiliaryMetadata:        []*anypb.Any{auxAny},
			DoNotCache:               test.doNotCache,
			UsageStats:               usageStats,
		},
	}
	expectedExecuteResponse := &repb.ExecuteResponse{
		CachedResult: test.cachedResult,
		Result:       actionResult,
		Status:       gstatus.Convert(test.status).Proto(),
	}
	op, err = operation.Assemble(
		taskID,
		operation.Metadata(repb.ExecutionStage_COMPLETED, arn.GetDigest()),
		expectedExecuteResponse,
	)
	require.NoError(t, err)
	err = stream.Send(op)
	require.NoError(t, err)
	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	trimmedExecuteResponse := expectedExecuteResponse.CloneVT()
	trimmedExecuteResponse.GetResult().GetExecutionMetadata().AuxiliaryMetadata = nil
	trimmedExecuteResponse.GetResult().GetExecutionMetadata().GetUsageStats().Timeline = nil

	// Wait for the execute response to be streamed back on our initial
	// /Execute stream.
	var executeResponse *repb.ExecuteResponse
	for {
		op, err = executionClient.Recv()
		if err == io.EOF {
			require.NotNil(t, executeResponse, "expected execute response, got EOF")
			break
		}
		require.NoError(t, err)
		if stage := operation.ExtractStage(op); stage != repb.ExecutionStage_COMPLETED {
			continue
		}
		executeResponse = operation.ExtractExecuteResponse(op)
	}
	assert.Empty(t, cmp.Diff(trimmedExecuteResponse, executeResponse, protocmp.Transform()))

	// Check that the action cache contains the right entry, if any.
	arn.ToProto().CacheType = rspb.CacheType_AC
	arnAC, err := arn.CheckAC()
	require.NoError(t, err)
	cachedActionResult, err := cachetools.GetActionResult(ctx, env.GetActionCacheClient(), arnAC)
	if !test.doNotCache && test.exitCode == 0 && test.status == nil && !test.cachedResult {
		require.NoError(t, err)
		assert.Empty(t, cmp.Diff(trimmedExecuteResponse.GetResult(), cachedActionResult, protocmp.Transform()))
	} else {
		require.Equal(t, codes.NotFound, gstatus.Code(err), "Error should be NotFound, but is %v", err)
	}

	// Should also be able to fetch the ExecuteResponse from cache. See field
	// comment on Execution.execute_response_digest for notes on serialization
	// format.
	cachedExecuteResponse, err := execution.GetCachedExecuteResponse(ctx, env.GetActionCacheClient(), taskID)
	require.NoError(t, err)
	assert.Empty(t, cmp.Diff(expectedExecuteResponse, cachedExecuteResponse, protocmp.Transform()))

	// Should also have recorded usage.
	ut := env.GetUsageTracker().(*fakeUsageTracker)
	var executionUsages []usage
	for _, u := range ut.usages {
		if u.labels.Client == "executor" && (u.counts.LinuxExecutionDurationUsec > 0 || u.counts.SelfHostedLinuxExecutionDurationUsec > 0) {
			executionUsages = append(executionUsages, u)
		}
	}
	assert.Equal(t, []usage{
		{
			labels: tables.UsageLabels{Client: "executor"},
			counts: test.expectedExecutionUsage,
		},
	}, executionUsages)

	// Check that we recorded the executions
	assert.Equal(t, 1, len(execCollector.executions))
	expectedExecution := &repb.StoredExecution{
		ExecutionId:            taskID,
		InvocationLinkType:     1,
		InvocationUuid:         strings.ReplaceAll(invocationID, "-", ""),
		Stage:                  4,
		StatusCode:             int32(gstatus.Code(test.status)),
		StatusMessage:          gstatus.Convert(test.status).Proto().GetMessage(),
		ExitCode:               test.exitCode,
		DoNotCache:             test.doNotCache,
		CachedResult:           test.cachedResult,
		RequestedComputeUnits:  2.5,
		RequestedFreeDiskBytes: 1000,
		RequestedMemoryBytes:   2000,
		RequestedMilliCpu:      1500,
		RequestedIsolationType: "oci",
		RequestedTimeoutUsec:   10000000,
		TargetLabel:            "//some:test",
		ActionMnemonic:         "TestRunner",
		SelfHosted:             test.expectedSelfHosted,
		Region:                 "test-region",
		CommandSnippet:         "test",
	}
	if test.publishMoreMetadata {
		expectedExecution.ExecutionPriority = 999
		expectedExecution.SkipCacheLookup = true
		expectedExecution.EstimatedFreeDiskBytes = 1001
		expectedExecution.PreviousMeasuredMemoryBytes = 2001
		expectedExecution.PreviousMeasuredMilliCpu = 2002
		expectedExecution.PreviousMeasuredFreeDiskBytes = 2003
		expectedExecution.PredictedMemoryBytes = 3001
		expectedExecution.PredictedMilliCpu = 3002
		expectedExecution.PredictedFreeDiskBytes = 3003
		expectedExecution.EffectiveIsolationType = "firecracker"
		expectedExecution.EffectiveTimeoutUsec = 11000000
	}
	diff := cmp.Diff(
		expectedExecution,
		execCollector.executions[0],
		protocmp.Transform(),
		protocmp.IgnoreFields(
			&repb.StoredExecution{},
			"created_at_usec",
			"updated_at_usec",
		))
	assert.Emptyf(t, diff, "Recorded execution didn't match the expected one: %s", expectedExecution)
}

func TestMarkFailed(t *testing.T) {
	env, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	// Create Execution rows to be canceled
	testUUID, err := uuid.NewRandom()
	require.NoError(t, err)
	testInvocationID := testUUID.String()

	executionID := "test-instance-name/uploads/1797f326-0cd2-45d2-9ad4-f766fd81f2dc/blobs/1111111111111111111111111111111111111111111111111111111111111111/100"

	completeExecution := &tables.Execution{
		ExecutionID:  executionID,
		InvocationID: testInvocationID,
		Stage:        int64(repb.ExecutionStage_EXECUTING),
	}
	createExecution(ctx, t, env.GetDBHandle(), completeExecution)

	ex := &tables.Execution{}
	err = env.GetDBHandle().GORM(ctx, "select_for_test_mark_failed").Model(ex).Where("execution_id = ?", executionID).Take(ex).Error
	require.NoError(t, err)

	require.Equal(t, int64(repb.ExecutionStage_EXECUTING), ex.Stage)

	err = s.MarkExecutionFailed(ctx, executionID, status.InternalError("It didn't work"))
	require.NoError(t, err)

	// Should exist in DB after marking failed
	ex = &tables.Execution{}
	err = env.GetDBHandle().GORM(ctx, "select_for_test_mark_failed").Model(ex).Where("execution_id = ?", executionID).Take(ex).Error
	require.NoError(t, err)
	assert.Equal(t, executionID, ex.ExecutionID)

	// ExecuteResponse should be cached after marking failed
	executeResponse, err := execution.GetCachedExecuteResponse(ctx, env.GetActionCacheClient(), executionID)
	require.NoError(t, err)
	assert.Equal(t, "It didn't work", executeResponse.GetStatus().GetMessage())

	err = s.MarkExecutionFailed(ctx, executionID, status.InternalError("It didn't work"))
	require.NoError(t, err)

	require.Equal(t, int64(repb.ExecutionStage_COMPLETED), ex.Stage)

	err = s.MarkExecutionFailed(ctx, "blobs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/1", status.InternalError("It didn't work"))
	require.True(t, status.IsNotFoundError(err), "error should be NotFoundError, but was %s", err)
}

func TestInvocationLink_EmptyInvocationID(t *testing.T) {
	flags.Set(t, "app.enable_write_executions_to_olap_db", true)
	env, conn := setupEnv(t)
	client := repb.NewExecutionClient(conn)
	execCollector := new(fakeCollector)
	env.SetExecutionCollector(execCollector)

	// Start an execution with an empty invocation ID.
	clientCtx := context.Background()
	instanceName := ""
	digestFunction := repb.DigestFunction_SHA256
	arn := uploadAction(clientCtx, t, env, instanceName, digestFunction, &repb.Action{})
	executionClient, err := client.Execute(clientCtx, &repb.ExecuteRequest{
		InstanceName:   arn.GetInstanceName(),
		ActionDigest:   arn.GetDigest(),
		DigestFunction: arn.GetDigestFunction(),
	})
	require.NoError(t, err)

	// Wait for the execution to be accepted by the server.
	_, err = executionClient.Recv()
	require.NoError(t, err)

	// No invocation links should be recorded.
	require.Empty(t, execCollector.invocationLinks)

	err = executionClient.CloseSend()
	require.NoError(t, err)
}

func uploadAction(ctx context.Context, t *testing.T, env *real_environment.RealEnv, instanceName string, df repb.DigestFunction_Value, action *repb.Action) *digest.ResourceName {
	cmd := &repb.Command{Arguments: []string{"test"}}
	cd, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, df, cmd)
	require.NoError(t, err)
	action.CommandDigest = cd
	ad, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, df, action)
	require.NoError(t, err)
	return digest.NewResourceName(ad, instanceName, rspb.CacheType_CAS, df)
}

func withIncomingMetadata(t *testing.T, ctx context.Context, rmd *repb.RequestMetadata) context.Context {
	b, err := proto.Marshal(rmd)
	require.NoError(t, err)
	return metadata.NewIncomingContext(ctx, metadata.Pairs(bazel_request.RequestMetadataKey, string(b)))
}
