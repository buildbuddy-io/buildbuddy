package execution_server_test

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_execution_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/experiments"
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
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testfs"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testusage"
	"github.com/buildbuddy-io/buildbuddy/server/util/bazel_request"
	"github.com/buildbuddy-io/buildbuddy/server/util/clientip"
	"github.com/buildbuddy-io/buildbuddy/server/util/db"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/platform"
	"github.com/buildbuddy-io/buildbuddy/server/util/prefix"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/go-redis/redis/v8"
	"github.com/google/go-cmp/cmp"
	"github.com/google/uuid"
	"github.com/jonboulle/clockwork"
	"github.com/open-feature/go-sdk/openfeature"
	"github.com/open-feature/go-sdk/openfeature/memprovider"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/durationpb"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	scpb "github.com/buildbuddy-io/buildbuddy/proto/scheduler"
	flagd "github.com/open-feature/go-sdk-contrib/providers/flagd/pkg"
	gstatus "google.golang.org/grpc/status"
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
	scheduleErr   error
}

func (s *schedulerServerMock) GetPoolInfo(_ context.Context, os, arch, requestedPool, originalPool, workflowID string, poolType platform.PoolType) (*interfaces.PoolInfo, error) {
	groupID := sharedPoolGroupID
	if poolType == platform.PoolTypeSelfHosted {
		groupID = selfHostedPoolGroupID
	}

	effectivePool := requestedPool
	if effectivePool == "" {
		effectivePool = "default-pool"
	}
	return &interfaces.PoolInfo{
		GroupID:      groupID,
		IsSelfHosted: poolType == platform.PoolTypeSelfHosted,
		Name:         effectivePool,
	}, nil
}
func (s *schedulerServerMock) GetSharedExecutorPoolGroupID() string {
	return sharedPoolGroupID
}

func (s *schedulerServerMock) ScheduleTask(ctx context.Context, req *scpb.ScheduleTaskRequest) (*scpb.ScheduleTaskResponse, error) {
	s.scheduleReqs = append(s.scheduleReqs, req)
	if s.scheduleErr != nil {
		return nil, s.scheduleErr
	}
	return &scpb.ScheduleTaskResponse{}, nil
}

func (s *schedulerServerMock) CancelTask(ctx context.Context, taskID string) (bool, error) {
	s.canceledCount++
	return true, nil
}

func setupEnvWithClock(t *testing.T, clock clockwork.Clock) (*testenv.TestEnv, *grpc.ClientConn, *testredis.Handle) {
	env := testenv.GetTestEnv(t)
	env.SetClock(clock)

	env.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1")))

	r := testredis.Start(t)
	rdb := redis.NewClient(redisutil.TargetToOptions(r.Target))
	env.SetDefaultRedisClient(r.Client())
	err := redis_execution_collector.Register(env)
	require.NoError(t, err)
	env.SetRemoteExecutionRedisClient(rdb)
	env.SetRemoteExecutionRedisPubSubClient(rdb)

	scheduler := &schedulerServerMock{}
	env.SetSchedulerService(scheduler)

	tasksize.Register(env)

	_, run, lis := testenv.RegisterLocalGRPCServer(t, env)
	testcache.Setup(t, env, lis)

	s, err := execution_server.NewExecutionServer(env)
	require.NoError(t, err)
	env.SetRemoteExecutionService(s)
	env.SetUsageTracker(testusage.NewTracker())

	repb.RegisterExecutionServer(env.GetGRPCServer(), env.GetRemoteExecutionService())
	go run()

	conn, err := testenv.LocalGRPCConn(env.GetServerContext(), lis)
	require.NoError(t, err)
	return env, conn, r
}

func setupEnv(t *testing.T) (*testenv.TestEnv, *grpc.ClientConn, *testredis.Handle) {
	return setupEnvWithClock(t, clockwork.NewRealClock())
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
	env, _, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	const iid = "10243d8a-a329-4f46-abfb-bfbceed12baa"
	ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
		ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
		ToolInvocationId: iid,
	})
	ctx, err := env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	action := &repb.Action{}
	arn := uploadAction(ctx, t, env, "" /*=instanceName*/, repb.DigestFunction_SHA256, action)
	ad := arn.GetDigest()

	// note: AttachUserPrefix is normally done by Execute(), which wraps
	// Dispatch().
	ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)
	taskID := arn.NewUploadString()
	require.NoError(t, err)
	err = s.Dispatch(ctx, &repb.ExecuteRequest{ActionDigest: ad}, action, taskID)
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

func TestDispatch_UploadOutputsChunkedMaxWriteSize(t *testing.T) {
	env, _, _ := setupEnv(t)

	tmp := testfs.MakeTempDir(t)
	offlineFlagPath := testfs.WriteFile(t, tmp, "config.flagd.json", `
{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "cache.chunking_enabled": {
      "state": "ENABLED",
      "variants": {
        "on": true
      },
      "defaultVariant": "on"
    },
    "executor.upload_outputs_chunked": {
      "state": "ENABLED",
      "variants": {
        "on": true
      },
      "defaultVariant": "on"
    },
    "cache.chunking_max_write_size_bytes": {
      "state": "ENABLED",
      "variants": {
        "limited": 123456789
      },
      "defaultVariant": "limited"
    }
  }
}
`)
	provider, err := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(offlineFlagPath))
	require.NoError(t, err)
	openfeature.SetProviderAndWait(provider)
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	const iid = "10243d8a-a329-4f46-abfb-bfbceed12baa"
	ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
		ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
		ToolInvocationId: iid,
	})
	ctx, err = env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	action := &repb.Action{}
	arn := uploadAction(ctx, t, env, "" /*=instanceName*/, repb.DigestFunction_SHA256, action)
	ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)
	err = s.Dispatch(ctx, &repb.ExecuteRequest{ActionDigest: arn.GetDigest()}, action, arn.NewUploadString())
	require.NoError(t, err)

	sched := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, len(sched.scheduleReqs))
	task := &repb.ExecutionTask{}
	err = proto.Unmarshal(sched.scheduleReqs[0].SerializedTask, task)
	require.NoError(t, err)
	require.Contains(t, task.GetExperiments(), "executor.upload_outputs_chunked")
	require.Equal(t, int64(123456789), task.GetFastCdc_2020Params().GetBuildbuddyMaxChunkedWriteSizeBytes())
}

func TestDispatch_RecordsClientIP(t *testing.T) {
	flags.Set(t, "remote_execution.write_execution_progress_state_to_redis", true)
	env, _, _ := setupEnv(t)
	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	const iid = "10243d8a-a329-4f46-abfb-bfbceed12baa"
	const testClientIP = "192.168.1.100"

	ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
		ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
		ToolInvocationId: iid,
	})
	ctx, err := env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	ctx = context.WithValue(ctx, clientip.ContextKey, testClientIP)

	action := &repb.Action{}
	arn := uploadAction(ctx, t, env, "" /*=instanceName*/, repb.DigestFunction_SHA256, action)
	ad := arn.GetDigest()

	ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)
	taskID := arn.NewUploadString()
	require.NoError(t, err)
	err = s.Dispatch(ctx, &repb.ExecuteRequest{ActionDigest: ad}, action, taskID)
	require.NoError(t, err)

	exec, err := env.GetExecutionCollector().GetInProgressExecution(ctx, taskID)
	require.NoError(t, err)
	assert.Equal(t, testClientIP, exec.GetClientIp())
}

func TestDispatch_WorkingDirectoryValidation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		wd      string
		wantErr bool
	}{
		{name: "empty", wd: "", wantErr: false},
		{name: "valid_subdir", wd: "src/main", wantErr: false},
		{name: "path_traversal", wd: "../escape", wantErr: true},
		{name: "nested_path_traversal", wd: "a/../../escape", wantErr: true},
		{name: "absolute_path", wd: "/etc/passwd", wantErr: true},
		{name: "dot_dot_only", wd: "..", wantErr: true},
		{name: "dot_only", wd: ".", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, _, _ := setupEnv(t)
			ctx := context.Background()
			s := env.GetRemoteExecutionService()

			ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
				ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
				ToolInvocationId: "10243d8a-a329-4f46-abfb-bfbceed12baa",
			})
			ctx, err := env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, "US1")
			require.NoError(t, err)

			action := &repb.Action{}
			cmd := &repb.Command{
				Arguments:        []string{"test"},
				WorkingDirectory: tc.wd,
			}
			arn := uploadActionWithCommand(ctx, t, env, "", repb.DigestFunction_SHA256, action, cmd)
			ad := arn.GetDigest()

			ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
			require.NoError(t, err)
			taskID := arn.NewUploadString()
			err = s.Dispatch(ctx, &repb.ExecuteRequest{ActionDigest: ad}, action, taskID)

			if tc.wantErr {
				require.Error(t, err)
				assert.True(t, status.IsInvalidArgumentError(err), "expected InvalidArgument, got: %v", err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestDispatch_TaskSizeOverridesExperiment(t *testing.T) {
	env, _, _ := setupEnv(t)

	tmp := testfs.MakeTempDir(t)
	offlineFlagPath := testfs.WriteFile(t, tmp, "config.flagd.json", `
{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "remote_execution.task_size_overrides": {
      "state": "ENABLED",
      "variants": {
        "test": {
          "EstimatedMemory": "30GB"
        },
        "default": {}
      },
      "defaultVariant": "default",
      "targeting": {
        "if": [
          { "==": [ { "var": "EstimatedComputeUnits" }, 8.0 ] },
          "test"
        ]
      }
    }
  }
}
`)
	provider, err := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(offlineFlagPath))
	require.NoError(t, err)
	openfeature.SetProviderAndWait(provider)
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)

	ctx := context.Background()
	s := env.GetRemoteExecutionService()

	const iid = "10243d8a-a329-4f46-abfb-bfbceed12baa"
	ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
		ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
		ToolInvocationId: iid,
	})
	ctx, err = env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	action := &repb.Action{
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{
					Name:  "EstimatedComputeUnits",
					Value: "8",
				},
			},
		},
	}
	arn := uploadAction(ctx, t, env, "" /*=instanceName*/, repb.DigestFunction_SHA256, action)
	ad := arn.GetDigest()

	// note: AttachUserPrefix is normally done by Execute(), which wraps
	// Dispatch().
	ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)
	err = s.Dispatch(ctx, &repb.ExecuteRequest{ActionDigest: ad}, action, "12345678")
	require.NoError(t, err)

	sched := env.GetSchedulerService().(*schedulerServerMock)
	require.Equal(t, 1, len(sched.scheduleReqs))
	b := sched.scheduleReqs[0].SerializedTask
	task := &repb.ExecutionTask{}
	err = proto.Unmarshal(b, task)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(&repb.Platform{
		Properties: []*repb.Platform_Property{
			{
				Name:  "EstimatedMemory",
				Value: "30GB",
			},
		},
	}, task.GetPlatformOverrides(), protocmp.Transform()))
}

func TestDispatch_ContainerImageRewriteExperiment(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		containerImage        string
		wantContainerImage    string
		wantExperimentVariant string
	}{
		{
			name:                  "matching prefix is rewritten",
			containerImage:        "docker://gcr.io/flame-public/rbe-ubuntu:latest",
			wantContainerImage:    "docker://buildbuddy.bbcr.io/public/rbe-ubuntu:latest",
			wantExperimentVariant: "bbcr",
		},
		{
			name:                  "non-matching prefix is not rewritten",
			containerImage:        "docker://us-docker.pkg.dev/other/image:latest",
			wantContainerImage:    "",
			wantExperimentVariant: "bbcr",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			env, _, _ := setupEnv(t)

			tmp := testfs.MakeTempDir(t)
			offlineFlagPath := testfs.WriteFile(t, tmp, "config.flagd.json", `
{
  "$schema": "https://flagd.dev/schema/v0/flags.json",
  "flags": {
    "remote_execution.container_image_rewrite": {
      "state": "ENABLED",
      "variants": {
        "bbcr": {
          "prefix": "gcr.io/flame-public/",
          "replacement": "buildbuddy.bbcr.io/public/"
        },
        "default": {}
      },
      "defaultVariant": "default",
      "targeting": {
        "if": [
          true,
          "bbcr"
        ]
      }
    }
  }
}
`)
			provider, err := flagd.NewProvider(flagd.WithInProcessResolver(), flagd.WithOfflineFilePath(offlineFlagPath))
			require.NoError(t, err)
			openfeature.SetProviderAndWait(provider)
			fp, err := experiments.NewFlagProvider("test")
			require.NoError(t, err)
			env.SetExperimentFlagProvider(fp)

			ctx := context.Background()
			s := env.GetRemoteExecutionService()

			const iid = "10243d8a-a329-4f46-abfb-bfbceed12baa"
			ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
				ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
				ToolInvocationId: iid,
			})
			ctx, err = env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, "US1")
			require.NoError(t, err)

			action := &repb.Action{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{
							Name:  "container-image",
							Value: tc.containerImage,
						},
					},
				},
			}
			arn := uploadAction(ctx, t, env, "" /*=instanceName*/, repb.DigestFunction_SHA256, action)
			ad := arn.GetDigest()

			ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
			require.NoError(t, err)
			err = s.Dispatch(ctx, &repb.ExecuteRequest{ActionDigest: ad}, action, "12345678")
			require.NoError(t, err)

			sched := env.GetSchedulerService().(*schedulerServerMock)
			require.Equal(t, 1, len(sched.scheduleReqs))
			b := sched.scheduleReqs[0].SerializedTask
			task := &repb.ExecutionTask{}
			err = proto.Unmarshal(b, task)
			require.NoError(t, err)

			// Check that the container image override is present (or absent).
			var imageOverride string
			for _, p := range task.GetPlatformOverrides().GetProperties() {
				if p.GetName() == "container-image" {
					imageOverride = p.GetValue()
				}
			}
			assert.Equal(t, tc.wantContainerImage, imageOverride)

			// Check experiment tracking.
			if tc.wantExperimentVariant != "" {
				assert.Contains(t, task.GetExperiments(), "remote_execution.container_image_rewrite:"+tc.wantExperimentVariant)
			} else {
				for _, exp := range task.GetExperiments() {
					assert.False(t, strings.HasPrefix(exp, "remote_execution.container_image_rewrite:"), "unexpected experiment entry: %s", exp)
				}
			}
		})
	}
}

func TestCancel(t *testing.T) {
	env, _, _ := setupEnv(t)
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
	env, _, _ := setupEnv(t)
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
	env, _, _ := setupEnv(t)
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
	env, _, _ := setupEnv(t)
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
		{
			name:                   "PublishMoreMetadata_PrimaryDBAndRedisDoubleWrite",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
			publishMoreMetadata:    true,
			flagOverrides: map[string]any{
				"remote_execution.write_execution_progress_state_to_redis": true,
				"remote_execution.write_executions_to_primary_db":          true,
			},
		},
		{
			name:                   "PublishMoreMetadata_NoPrimaryDB_RedisOnly",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
			publishMoreMetadata:    true,
			flagOverrides: map[string]any{
				"remote_execution.write_execution_progress_state_to_redis": true,
				"remote_execution.write_executions_to_primary_db":          false,
			},
		},
		{
			// Restarting Redis wipes the per-execution invocation links
			// before the COMPLETED operation arrives, exercising the same
			// "empty invocation links" path that normal-flow executions
			// without an invocation context hit (teed requests, etc.).
			// The OLAP flush is skipped (no invocation to associate the
			// row with) but usage is still recorded from the merged
			// StoredExecution that updateExecution wrote post-restart.
			name:                   "RedisRestart",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
			redisRestart:           true,
		},
		{
			name:                   "DefaultPool",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
			useDefaultPool:         true,
		},
		{
			name:                   "RecycleRunner",
			expectedExecutionUsage: tables.UsageCounts{LinuxExecutionDurationUsec: durationUsec},
			recycleRunner:          true,
		},
	} {
		for _, flushAfterCleanup := range []bool{false, true} {
			test := test
			test.flushAfterCleanup = flushAfterCleanup
			name := test.name
			if flushAfterCleanup {
				name += "/FlushAfterCleanup"
			}
			t.Run(name, func(t *testing.T) {
				testExecuteAndPublishOperation(t, test)
			})
		}
	}
}

type publishTest struct {
	name                     string
	platformOverrides        map[string]string
	flagOverrides            map[string]any
	expectedSelfHosted       bool
	expectedExecutionUsage   tables.UsageCounts
	cachedResult, doNotCache bool
	status                   error
	exitCode                 int32
	publishMoreMetadata      bool
	redisRestart             bool
	useDefaultPool           bool
	recycleRunner            bool
	flushAfterCleanup        bool
}

func testExecuteAndPublishOperation(t *testing.T, test publishTest) {
	ctx := context.Background()
	flags.Set(t, "app.enable_write_executions_to_olap_db", true)
	flags.Set(t, "remote_execution.write_execution_progress_state_to_redis", true)
	for k, v := range test.flagOverrides {
		flags.Set(t, k, v)
	}
	env, conn, r := setupEnv(t)
	if test.flushAfterCleanup {
		configureExperiments(t, env, map[string]bool{"remote_execution.flush_executions_after_cleanup": true})
	}
	client := repb.NewExecutionClient(conn)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)

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
	platformProperties := []*repb.Platform_Property{
		{Name: "EstimatedComputeUnits", Value: "2.5"},
		{Name: "EstimatedFreeDiskBytes", Value: "1000"},
		{Name: "EstimatedCPU", Value: "1.5"},
		{Name: "EstimatedMemory", Value: "2000"},
		{Name: "workload-isolation-type", Value: "oci"},
	}
	if !test.useDefaultPool {
		platformProperties = append(platformProperties, &repb.Platform_Property{Name: "pool", Value: "test-pool"})
	}
	if test.recycleRunner {
		platformProperties = append(platformProperties, &repb.Platform_Property{Name: "recycle-runner", Value: "true"})
	}
	arn := uploadAction(clientCtx, t, env, instanceName, digestFunction, &repb.Action{
		Timeout:    &durationpb.Duration{Seconds: 10},
		DoNotCache: test.doNotCache,
		Platform:   &repb.Platform{Properties: platformProperties},
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

	if test.redisRestart {
		r.Restart()
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
		effectivePool := "test-pool"
		if test.useDefaultPool {
			effectivePool = "default-pool"
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
			Pool:            effectivePool,
		}
		usageStats.Timeline = &repb.UsageTimeline{
			StartTime: tspb.New(workerStartTime),
		}
		usageStats.NetworkStats = &repb.NetworkStats{
			BytesSent:       1000,
			PacketsSent:     3000,
			BytesReceived:   2000,
			PacketsReceived: 4000,
		}
		usageStats.CpuPressure = &repb.PSI{
			Some: &repb.PSI_Metrics{Total: 1010},
			Full: &repb.PSI_Metrics{Total: 2010},
		}
		usageStats.MemoryPressure = &repb.PSI{
			Some: &repb.PSI_Metrics{Total: 1020},
			Full: &repb.PSI_Metrics{Total: 2020},
		}
		usageStats.IoPressure = &repb.PSI{
			Some: &repb.PSI_Metrics{Total: 1030},
			Full: &repb.PSI_Metrics{Total: 2030},
		}
	} else {
		effectivePool := "test-pool"
		if test.useDefaultPool {
			effectivePool = "default-pool"
		}
		aux.SchedulingMetadata = &scpb.SchedulingMetadata{
			ExecutorGroupId: executorGroupID,
			Pool:            effectivePool,
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
		// Trim the aux metadata before comparing
		cachedActionResult.GetExecutionMetadata().AuxiliaryMetadata = nil
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

	// Should also have recorded usage, unless redis restart wiped it.
	ut := env.GetUsageTracker().(*testusage.Tracker)
	var foundExecutorUsage *testusage.Total
	for _, u := range ut.Totals() {
		if u.Labels.Client == "executor" {
			foundExecutorUsage = &u
			break
		}
	}
	require.NotNil(t, foundExecutorUsage, "expected executor usage to be recorded")
	assert.Equal(t, "group1", foundExecutorUsage.GroupID)
	assert.Equal(t, test.expectedExecutionUsage.LinuxExecutionDurationUsec, foundExecutorUsage.Counts.LinuxExecutionDurationUsec)
	assert.Equal(t, test.expectedExecutionUsage.SelfHostedLinuxExecutionDurationUsec, foundExecutorUsage.Counts.SelfHostedLinuxExecutionDurationUsec)

	collectedExecutions, err := env.GetExecutionCollector().GetExecutions(ctx, invocationID, 0, -1)
	require.NoError(t, err)

	if test.redisRestart {
		assert.Equal(t, 0, len(collectedExecutions))
		return
	}

	// Check that we recorded the executions
	assert.Equal(t, 1, len(collectedExecutions))
	expectedExecution := &repb.StoredExecution{
		ExecutionId:                  taskID,
		GroupId:                      "group1",
		UserId:                       "user1",
		InvocationLinkType:           1,
		InvocationUuid:               strings.ReplaceAll(invocationID, "-", ""),
		Stage:                        4,
		StatusCode:                   int32(gstatus.Code(test.status)),
		StatusMessage:                gstatus.Convert(test.status).Proto().GetMessage(),
		ExitCode:                     test.exitCode,
		DoNotCache:                   test.doNotCache,
		CachedResult:                 test.cachedResult,
		RequestedComputeUnits:        2.5,
		RequestedFreeDiskBytes:       1000,
		RequestedMemoryBytes:         2000,
		RequestedMilliCpu:            1500,
		RequestedIsolationType:       "oci",
		RequestedTimeoutUsec:         10000000,
		TargetLabel:                  "//some:test",
		ActionMnemonic:               "TestRunner",
		SelfHosted:                   test.expectedSelfHosted,
		Region:                       "test-region",
		Os:                           "linux",
		Arch:                         "amd64",
		CommandSnippet:               "test",
		OutputPath:                   "bazel-out/k8-fastbuild/bin/some/test",
		RecycleRunner:                test.recycleRunner,
		QueuedTimestampUsec:          queuedTime.UnixMicro(),
		WorkerStartTimestampUsec:     workerStartTime.UnixMicro(),
		WorkerCompletedTimestampUsec: workerEndTime.UnixMicro(),
	}
	if test.useDefaultPool {
		expectedExecution.RequestedPool = ""
		expectedExecution.EffectivePool = "default-pool"
	} else {
		expectedExecution.RequestedPool = "test-pool"
		expectedExecution.EffectivePool = "test-pool"
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
		expectedExecution.NetworkBytesSent = 1000
		expectedExecution.NetworkBytesReceived = 2000
		expectedExecution.NetworkPacketsSent = 3000
		expectedExecution.NetworkPacketsReceived = 4000
		expectedExecution.CpuPressureSomeStallUsec = 1010
		expectedExecution.CpuPressureFullStallUsec = 2010
		expectedExecution.MemoryPressureSomeStallUsec = 1020
		expectedExecution.MemoryPressureFullStallUsec = 2020
		expectedExecution.IoPressureSomeStallUsec = 1030
		expectedExecution.IoPressureFullStallUsec = 2030
	}
	require.Empty(t, cmp.Diff(
		expectedExecution,
		collectedExecutions[0],
		protocmp.Transform(),
		protocmp.IgnoreFields(
			&repb.StoredExecution{},
			"created_at_usec",
			"updated_at_usec",
		)))
}

// TestPublishOperation_RetriedStream simulates the executor's
// PublishOperation stream being interrupted mid-flight and the retryingClient
// re-opening a new stream to re-publish the same COMPLETED operation. The
// server should flush the OLAP row and record usage exactly once across the
// retry chain.
//
// Only run with the flush_executions_after_cleanup experiment enabled: that's
// where the exactly-once guarantee comes from. The recv loop only calls
// flushAndRecordUsage on io.EOF, and the retryingClient only opens a new
// stream after a non-EOF error — so at most one stream in the chain ever
// triggers the flush. Under the legacy COMPLETED-flush path the flush runs
// inline with the COMPLETED handler regardless of how the stream ends, so
// retries are inherently subject to over-counting; that's pre-existing
// behavior and not what this test pins down.
func TestPublishOperation_RetriedStream(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "app.enable_write_executions_to_olap_db", true)
	flags.Set(t, "remote_execution.write_execution_progress_state_to_redis", true)
	env, conn, _ := setupEnv(t)
	configureExperiments(t, env, map[string]bool{"remote_execution.flush_executions_after_cleanup": true})
	client := repb.NewExecutionClient(conn)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)

	const instanceName = "test-instance"
	const invocationID = "93383cc1-5d6c-4ad1-a321-8ee87c2f6816"

	clientCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
		ToolInvocationId: invocationID,
		TargetId:         "//some:test",
		ActionMnemonic:   "TestRunner",
	})
	require.NoError(t, err)

	arn := uploadAction(clientCtx, t, env, instanceName, repb.DigestFunction_SHA256, &repb.Action{
		Timeout: &durationpb.Duration{Seconds: 10},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "pool", Value: "test-pool"},
				{Name: "workload-isolation-type", Value: "oci"},
				{Name: "EstimatedComputeUnits", Value: "2.5"},
			},
		},
	})

	executionClient, err := client.Execute(clientCtx, &repb.ExecuteRequest{
		InstanceName:   arn.GetInstanceName(),
		ActionDigest:   arn.GetDigest(),
		DigestFunction: arn.GetDigestFunction(),
	})
	require.NoError(t, err)
	require.NoError(t, executionClient.CloseSend())
	op, err := executionClient.Recv()
	require.NoError(t, err)
	taskID := op.GetName()

	executorCtx := metadata.AppendToOutgoingContext(clientCtx, usageutil.ClientHeaderName, "executor")
	executorCtx = metadata.AppendToOutgoingContext(executorCtx, "x-buildbuddy-executor-region", "test-region")

	queuedTime := time.Unix(100, 0)
	workerStartTime := queuedTime.Add(1 * time.Second)
	workerEndTime := workerStartTime.Add(5 * time.Second)
	durationUsec := (5 * time.Second).Microseconds()
	aux := &espb.ExecutionAuxiliaryMetadata{
		PlatformOverrides: &repb.Platform{},
		SchedulingMetadata: &scpb.SchedulingMetadata{
			ExecutorGroupId: sharedPoolGroupID,
			Pool:            "test-pool",
		},
	}
	auxAny, err := anypb.New(aux)
	require.NoError(t, err)
	completedOp, err := operation.Assemble(
		taskID,
		operation.Metadata(repb.ExecutionStage_COMPLETED, arn.GetDigest()),
		&repb.ExecuteResponse{
			Result: &repb.ActionResult{
				ExecutionMetadata: &repb.ExecutedActionMetadata{
					QueuedTimestamp:          tspb.New(queuedTime),
					WorkerStartTimestamp:     tspb.New(workerStartTime),
					WorkerCompletedTimestamp: tspb.New(workerEndTime),
					AuxiliaryMetadata:        []*anypb.Any{auxAny},
				},
			},
		},
	)
	require.NoError(t, err)

	// Stream 1: send COMPLETED and then break the stream by cancelling its
	// context before the client cleanly closes it. The server's recv loop
	// sees context.Canceled (or grpc Canceled), not io.EOF, so
	// flushAndRecordUsage does not run on this stream. Poll Redis to
	// confirm the server processed the COMPLETED (updateExecution wrote)
	// before cancelling so the test isn't racy.
	stream1Ctx, cancelStream1 := context.WithCancel(executorCtx)
	stream1, err := client.PublishOperation(stream1Ctx)
	require.NoError(t, err)
	require.NoError(t, stream1.Send(completedOp))
	require.Eventually(t, func() bool {
		execProto, err := env.GetExecutionCollector().GetInProgressExecution(ctx, taskID)
		return err == nil && execProto != nil
	}, 5*time.Second, 10*time.Millisecond, "stream 1's COMPLETED should be processed by updateExecution")
	cancelStream1()
	// CloseAndRecv on a cancelled stream returns an error — we expect that.
	_, _ = stream1.CloseAndRecv()

	// Stream 2 (the retry): re-publish the same COMPLETED and cleanly
	// close. This is the only stream whose recv loop returns EOF and so
	// the only one that triggers flushAndRecordUsage.
	stream2, err := client.PublishOperation(executorCtx)
	require.NoError(t, err)
	require.NoError(t, stream2.Send(completedOp))
	_, err = stream2.CloseAndRecv()
	require.NoError(t, err, "retry stream closed cleanly")

	// Drain the /Execute stream so the test doesn't leak goroutines.
	for {
		_, err := executionClient.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	collectedExecutions, err := env.GetExecutionCollector().GetExecutions(ctx, invocationID, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, 1, len(collectedExecutions), "execution should be flushed to OLAP exactly once across retried streams")

	ut := env.GetUsageTracker().(*testusage.Tracker)
	var foundExecutorUsage *testusage.Total
	for _, u := range ut.Totals() {
		if u.Labels.Client == "executor" {
			foundExecutorUsage = &u
			break
		}
	}
	require.NotNil(t, foundExecutorUsage, "expected executor usage to be recorded")
	assert.Equal(t, durationUsec, foundExecutorUsage.Counts.LinuxExecutionDurationUsec, "usage should be recorded exactly once")
}

// TestPublishOperation_FlushWithEmptyLinks pins down the contract:
// flushAndRecordUsage still records usage when the merged StoredExecution
// exists in Redis but the per-execution invocation links list is empty.
//
// Empty invocation links is a normal state, not a corner case. It happens
// for any execution that isn't associated with a bazel invocation (teed
// requests, executions invoked outside an invocation context, etc.), as
// well as edge cases like Redis losing the link state between Dispatch
// and the COMPLETED operation. The OLAP row can't be written without an
// invocation to associate it with, but the per-group usage counter is
// still accurate.
//
// This test reaches the "empty links" state by restarting Redis after
// Dispatch but before the executor publishes COMPLETED — that's just a
// convenient mechanism; the contract applies regardless of how the links
// list ends up empty.
func TestPublishOperation_FlushWithEmptyLinks(t *testing.T) {
	for _, flushOnEOF := range []bool{false, true} {
		name := "FlushOnComplete"
		if flushOnEOF {
			name = "FlushAfterCleanup"
		}
		t.Run(name, func(t *testing.T) {
			testPublishOperationFlushWithEmptyLinks(t, flushOnEOF)
		})
	}
}

func testPublishOperationFlushWithEmptyLinks(t *testing.T, flushOnEOF bool) {
	ctx := context.Background()
	flags.Set(t, "app.enable_write_executions_to_olap_db", true)
	flags.Set(t, "remote_execution.write_execution_progress_state_to_redis", true)
	env, conn, r := setupEnv(t)
	if flushOnEOF {
		configureExperiments(t, env, map[string]bool{"remote_execution.flush_executions_after_cleanup": true})
	}
	client := repb.NewExecutionClient(conn)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)

	const instanceName = "test-instance"
	const invocationID = "93383cc1-5d6c-4ad1-a321-8ee87c2f6816"

	clientCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
		ToolInvocationId: invocationID,
		TargetId:         "//some:test",
		ActionMnemonic:   "TestRunner",
	})
	require.NoError(t, err)

	arn := uploadAction(clientCtx, t, env, instanceName, repb.DigestFunction_SHA256, &repb.Action{
		Timeout: &durationpb.Duration{Seconds: 10},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "pool", Value: "test-pool"},
				{Name: "workload-isolation-type", Value: "oci"},
			},
		},
	})
	executionClient, err := client.Execute(clientCtx, &repb.ExecuteRequest{
		InstanceName:   arn.GetInstanceName(),
		ActionDigest:   arn.GetDigest(),
		DigestFunction: arn.GetDigestFunction(),
	})
	require.NoError(t, err)
	require.NoError(t, executionClient.CloseSend())
	op, err := executionClient.Recv()
	require.NoError(t, err)
	taskID := op.GetName()

	// Wipe Redis after the invocation links were written by Dispatch but
	// before the executor publishes COMPLETED. The per-execution links
	// list is now gone, but updateExecution on the server will repopulate
	// the in-progress execution list from the COMPLETED operation.
	r.Restart()

	executorCtx := metadata.AppendToOutgoingContext(clientCtx, usageutil.ClientHeaderName, "executor")
	executorCtx = metadata.AppendToOutgoingContext(executorCtx, "x-buildbuddy-executor-region", "test-region")
	queuedTime := time.Unix(100, 0)
	workerStartTime := queuedTime.Add(1 * time.Second)
	workerEndTime := workerStartTime.Add(5 * time.Second)
	durationUsec := workerEndTime.Sub(workerStartTime).Microseconds()
	aux := &espb.ExecutionAuxiliaryMetadata{
		PlatformOverrides: &repb.Platform{},
		SchedulingMetadata: &scpb.SchedulingMetadata{
			ExecutorGroupId: sharedPoolGroupID,
			Pool:            "test-pool",
		},
	}
	auxAny, err := anypb.New(aux)
	require.NoError(t, err)
	completedOp, err := operation.Assemble(taskID, operation.Metadata(repb.ExecutionStage_COMPLETED, arn.GetDigest()), &repb.ExecuteResponse{
		Result: &repb.ActionResult{
			ExecutionMetadata: &repb.ExecutedActionMetadata{
				QueuedTimestamp:          tspb.New(queuedTime),
				WorkerStartTimestamp:     tspb.New(workerStartTime),
				WorkerCompletedTimestamp: tspb.New(workerEndTime),
				AuxiliaryMetadata:        []*anypb.Any{auxAny},
			},
		},
	})
	require.NoError(t, err)

	stream, err := client.PublishOperation(executorCtx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(completedOp))
	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	for {
		_, err := executionClient.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// Per-invocation execution list should be empty: no invocation link
	// means the OLAP flush has nothing to append the execution to.
	collectedExecutions, err := env.GetExecutionCollector().GetExecutions(ctx, invocationID, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, 0, len(collectedExecutions), "no OLAP row expected when invocation links are missing")

	// Usage should still be recorded — the merged StoredExecution exists
	// in Redis, which is all flushAndRecordUsage needs to compute usage.
	ut := env.GetUsageTracker().(*testusage.Tracker)
	var foundExecutorUsage *testusage.Total
	for _, u := range ut.Totals() {
		if u.Labels.Client == "executor" {
			foundExecutorUsage = &u
			break
		}
	}
	require.NotNil(t, foundExecutorUsage, "expected executor usage to be recorded")
	assert.Equal(t, durationUsec, foundExecutorUsage.Counts.LinuxExecutionDurationUsec)
}

// TestPublishOperation_PeriodicFlushDoesNotClobberCompletedRow exercises the
// race where the periodic flush goroutine fires more than 5 seconds after
// PublishOperation's main loop has already written the authoritative
// COMPLETED row. With the bug, the goroutine wakes up, sees `lastWrite`
// is stale, and pushes a partial update (auxMeta/properties/action/cmd
// all nil) back into Redis — resurrecting an in-progress entry that
// flushExecutionToOLAP had already cleaned up.
func TestPublishOperation_PeriodicFlushDoesNotClobberCompletedRow(t *testing.T) {
	ctx := context.Background()
	flags.Set(t, "app.enable_write_executions_to_olap_db", true)
	flags.Set(t, "remote_execution.write_execution_progress_state_to_redis", true)

	fakeClock := clockwork.NewFakeClock()
	env, conn, _ := setupEnvWithClock(t, fakeClock)
	client := repb.NewExecutionClient(conn)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)

	const instanceName = "test-instance"
	const invocationID = "93383cc1-5d6c-4ad1-a321-8ee87c2f6816"
	const digestFunction = repb.DigestFunction_SHA256

	clientCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
		ToolInvocationId: invocationID,
		TargetId:         "//some:test",
		ActionMnemonic:   "TestRunner",
	})
	require.NoError(t, err)
	arn := uploadAction(clientCtx, t, env, instanceName, digestFunction, &repb.Action{
		Timeout: &durationpb.Duration{Seconds: 10},
		Platform: &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "pool", Value: "test-pool"},
		}},
	})
	executionClient, err := client.Execute(clientCtx, &repb.ExecuteRequest{
		InstanceName:   arn.GetInstanceName(),
		ActionDigest:   arn.GetDigest(),
		DigestFunction: arn.GetDigestFunction(),
	})
	require.NoError(t, err)
	require.NoError(t, executionClient.CloseSend())
	initialOp, err := executionClient.Recv()
	require.NoError(t, err)
	taskID := initialOp.GetName()

	executorCtx := metadata.AppendToOutgoingContext(clientCtx, usageutil.ClientHeaderName, "executor")
	executorCtx = metadata.AppendToOutgoingContext(executorCtx, "x-buildbuddy-executor-region", "test-region")
	stream, err := client.PublishOperation(executorCtx)
	require.NoError(t, err)

	aux := &espb.ExecutionAuxiliaryMetadata{
		ExecutorHostname: "exec-host-1",
		IsolationType:    "firecracker",
		Timeout:          &durationpb.Duration{Seconds: 11},
		SchedulingMetadata: &scpb.SchedulingMetadata{
			ExecutorGroupId: sharedPoolGroupID,
			Pool:            "test-pool",
		},
	}
	auxAny, err := anypb.New(aux)
	require.NoError(t, err)
	completedResponse := &repb.ExecuteResponse{
		Result: &repb.ActionResult{
			ExecutionMetadata: &repb.ExecutedActionMetadata{
				AuxiliaryMetadata: []*anypb.Any{auxAny},
			},
		},
		Status: gstatus.Convert(nil).Proto(),
	}
	completedOp, err := operation.Assemble(
		taskID,
		operation.Metadata(repb.ExecutionStage_COMPLETED, arn.GetDigest()),
		completedResponse,
	)
	require.NoError(t, err)
	require.NoError(t, stream.Send(completedOp))

	// Wait for the main loop's COMPLETED write + flushExecutionToOLAP to
	// land. The flush appends the merged execution to the per-invocation
	// list and deletes the in-progress updates list.
	require.Eventually(t, func() bool {
		execs, err := env.GetExecutionCollector().GetExecutions(ctx, invocationID, 0, -1)
		return err == nil && len(execs) == 1
	}, 5*time.Second, 10*time.Millisecond, "main loop's COMPLETED write never landed in the per-invocation list")

	// Sanity check: in-progress data was cleaned up by flushExecutionToOLAP.
	_, err = env.GetExecutionCollector().GetInProgressExecution(ctx, taskID)
	require.Truef(t, status.IsNotFoundError(err), "expected in-progress data to be cleaned up after flush, got err=%v", err)

	// Advance past the 5s flush threshold to let the periodic goroutine wake up
	fakeClock.Advance(6 * time.Second)

	// Give the periodic goroutine real wall-clock time to fire. With the
	// fix in place it skips the write; with the bug it resurrects the
	// in-progress entry.
	require.Never(t, func() bool {
		_, err := env.GetExecutionCollector().GetInProgressExecution(ctx, taskID)
		return err == nil
	}, 500*time.Millisecond, 25*time.Millisecond, "periodic flush resurrected the in-progress entry after the authoritative COMPLETED write")

	// Close the stream cleanly so PublishOperation returns.
	_, _ = stream.CloseAndRecv()
}

// TestPublishOperation_SecondCompletedCarriesSnapshotStats verifies that a
// second stage=COMPLETED Operation sent on the same PublishOperation stream,
// carrying a PostCompletionStats entry in auxiliary_metadata, is handled
// correctly under both experiment values.
//
// Under flush_executions_after_cleanup=true (the path this PR exists to
// enable), the snapshot fields get merged into the recorded
// StoredExecution. The first COMPLETED's other fields survive the merge
// (proto.Merge semantics).
//
// Under flush_executions_after_cleanup=false (legacy path), the OLAP flush
// already ran on the first COMPLETED, so the second COMPLETED is dropped
// server-side and the snapshot fields don't appear in the recorded row.
//
// In both modes the action result must not be re-cached and usage must be
// recorded exactly once.
func TestPublishOperation_SecondCompletedCarriesSnapshotStats(t *testing.T) {
	for _, flushAfterCleanup := range []bool{false, true} {
		name := "FlushOnComplete"
		if flushAfterCleanup {
			name = "FlushAfterCleanup"
		}
		t.Run(name, func(t *testing.T) {
			testPublishOperationSecondCompletedCarriesSnapshotStats(t, flushAfterCleanup)
		})
	}
}

func testPublishOperationSecondCompletedCarriesSnapshotStats(t *testing.T, flushAfterCleanup bool) {
	ctx := context.Background()
	flags.Set(t, "app.enable_write_executions_to_olap_db", true)
	flags.Set(t, "remote_execution.write_execution_progress_state_to_redis", true)
	env, conn, _ := setupEnv(t)
	if flushAfterCleanup {
		configureExperiments(t, env, map[string]bool{"remote_execution.flush_executions_after_cleanup": true})
	}
	client := repb.NewExecutionClient(conn)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)

	const instanceName = "test-instance"
	const invocationID = "93383cc1-5d6c-4ad1-a321-8ee87c2f6816"

	clientCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
		ToolInvocationId: invocationID,
		TargetId:         "//some:test",
		ActionMnemonic:   "TestRunner",
	})
	require.NoError(t, err)

	arn := uploadAction(clientCtx, t, env, instanceName, repb.DigestFunction_SHA256, &repb.Action{
		Timeout: &durationpb.Duration{Seconds: 10},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "pool", Value: "test-pool"},
				{Name: "workload-isolation-type", Value: "firecracker"},
				{Name: "EstimatedComputeUnits", Value: "2.5"},
			},
		},
	})
	executionClient, err := client.Execute(clientCtx, &repb.ExecuteRequest{
		InstanceName:   arn.GetInstanceName(),
		ActionDigest:   arn.GetDigest(),
		DigestFunction: arn.GetDigestFunction(),
	})
	require.NoError(t, err)
	require.NoError(t, executionClient.CloseSend())
	op, err := executionClient.Recv()
	require.NoError(t, err)
	taskID := op.GetName()

	executorCtx := metadata.AppendToOutgoingContext(clientCtx, usageutil.ClientHeaderName, "executor")
	executorCtx = metadata.AppendToOutgoingContext(executorCtx, "x-buildbuddy-executor-region", "test-region")

	queuedTime := time.Unix(100, 0)
	workerStartTime := queuedTime.Add(1 * time.Second)
	workerEndTime := workerStartTime.Add(5 * time.Second)
	aux := &espb.ExecutionAuxiliaryMetadata{
		PlatformOverrides: &repb.Platform{},
		IsolationType:     "firecracker",
		SchedulingMetadata: &scpb.SchedulingMetadata{
			ExecutorGroupId: sharedPoolGroupID,
			Pool:            "test-pool",
		},
	}
	auxAny, err := anypb.New(aux)
	require.NoError(t, err)

	stream, err := client.PublishOperation(executorCtx)
	require.NoError(t, err)

	// First COMPLETED: full ExecuteResponse with the normal aux metadata.
	firstOp, err := operation.Assemble(taskID, operation.Metadata(repb.ExecutionStage_COMPLETED, arn.GetDigest()), &repb.ExecuteResponse{
		Result: &repb.ActionResult{
			ExecutionMetadata: &repb.ExecutedActionMetadata{
				QueuedTimestamp:          tspb.New(queuedTime),
				WorkerStartTimestamp:     tspb.New(workerStartTime),
				WorkerCompletedTimestamp: tspb.New(workerEndTime),
				AuxiliaryMetadata:        []*anypb.Any{auxAny},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(firstOp))

	// Second COMPLETED: a minimal ExecuteResponse whose auxiliary_metadata
	// contains a single PostCompletionStats entry. This is what the executor
	// sends after TryRecycle -> Pause has run for a firecracker runner.
	secondAuxAny, err := anypb.New(&espb.PostCompletionStats{
		PauseDurationUsec: 67890,
		FirecrackerPostExecStats: &espb.FirecrackerPostExecStats{
			SnapshotSavedLocally:  true,
			SnapshotSavedRemotely: true,
			SnapshotIsDiff:        true,
			SnapshotSavedBytes:    12345,
		},
	})
	require.NoError(t, err)
	secondOp, err := operation.Assemble(taskID, operation.Metadata(repb.ExecutionStage_COMPLETED, arn.GetDigest()), &repb.ExecuteResponse{
		Result: &repb.ActionResult{
			ExecutionMetadata: &repb.ExecutedActionMetadata{
				AuxiliaryMetadata: []*anypb.Any{secondAuxAny},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(secondOp))
	_, err = stream.CloseAndRecv()
	require.NoError(t, err)

	// Drain /Execute so the test doesn't leak goroutines.
	for {
		_, err := executionClient.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	collectedExecutions, err := env.GetExecutionCollector().GetExecutions(ctx, invocationID, 0, -1)
	require.NoError(t, err)
	require.Equal(t, 1, len(collectedExecutions), "expected exactly one flushed execution row")

	got := collectedExecutions[0]
	// First-COMPLETED fields survived the merge.
	assert.Equal(t, taskID, got.GetExecutionId())
	assert.Equal(t, "//some:test", got.GetTargetLabel())
	assert.Equal(t, workerStartTime.UnixMicro(), got.GetWorkerStartTimestampUsec())
	assert.Equal(t, workerEndTime.UnixMicro(), got.GetWorkerCompletedTimestampUsec())
	if flushAfterCleanup {
		// Second-COMPLETED snapshot stats were merged in.
		assert.True(t, got.GetSnapshotSavedLocally(), "snapshot_saved_locally")
		assert.True(t, got.GetSnapshotSavedRemotely(), "snapshot_saved_remotely")
		assert.True(t, got.GetSnapshotIsDiff(), "snapshot_is_diff")
		assert.Equal(t, int64(12345), got.GetSnapshotSavedBytes())
		assert.Equal(t, int64(67890), got.GetPauseDurationUsec())
	} else {
		// Legacy path: the OLAP flush already ran on the first COMPLETED, so
		// the second COMPLETED is dropped server-side and its snapshot
		// fields don't make it to the recorded row.
		assert.False(t, got.GetSnapshotSavedLocally(), "snapshot_saved_locally should be unset on legacy path")
		assert.False(t, got.GetSnapshotSavedRemotely(), "snapshot_saved_remotely should be unset on legacy path")
		assert.False(t, got.GetSnapshotIsDiff(), "snapshot_is_diff should be unset on legacy path")
		assert.Zero(t, got.GetSnapshotSavedBytes(), "snapshot_saved_bytes should be unset on legacy path")
		assert.Zero(t, got.GetPauseDurationUsec(), "pause_duration_usec should be unset on legacy path")
	}

	// Action cache should hold the result from the first COMPLETED only.
	// The sparse second COMPLETED must not have triggered another cache write.
	arn.ToProto().CacheType = rspb.CacheType_AC
	arnAC, err := arn.CheckAC()
	require.NoError(t, err)
	_, err = cachetools.GetActionResult(ctx, env.GetActionCacheClient(), arnAC)
	require.NoError(t, err, "action result should be in cache from first COMPLETED")

	// Usage should be incremented exactly once (5s of Linux execution
	// duration), not double-counted by the second COMPLETED.
	ut := env.GetUsageTracker().(*testusage.Tracker)
	var foundExecutorUsage *testusage.Total
	for _, u := range ut.Totals() {
		if u.Labels.Client == "executor" {
			foundExecutorUsage = &u
			break
		}
	}
	require.NotNil(t, foundExecutorUsage, "expected executor usage to be recorded")
	assert.Equal(t, (5 * time.Second).Microseconds(), foundExecutorUsage.Counts.LinuxExecutionDurationUsec)
}

// TestPublishOperation_RetryStreamWithOnlyPostCompletionStats simulates the
// executor retrying the PublishOperation RPC and sending only the
// post-completion stats COMPLETED on the retry stream (the first regular
// COMPLETED was already delivered on a prior stream). The server must still
// treat the lone op as a post-completion update rather than as a fresh first
// COMPLETED — otherwise it would clobber the cached action result with a
// sparse one, double-count usage, and emit a duplicate StoredExecution row.
func TestPublishOperation_RetryStreamWithOnlyPostCompletionStats(t *testing.T) {
	for _, flushAfterCleanup := range []bool{false, true} {
		name := "FlushOnComplete"
		if flushAfterCleanup {
			name = "FlushAfterCleanup"
		}
		t.Run(name, func(t *testing.T) {
			testPublishOperationRetryStreamWithOnlyPostCompletionStats(t, flushAfterCleanup)
		})
	}
}

func testPublishOperationRetryStreamWithOnlyPostCompletionStats(t *testing.T, flushAfterCleanup bool) {
	ctx := context.Background()
	flags.Set(t, "app.enable_write_executions_to_olap_db", true)
	flags.Set(t, "remote_execution.write_execution_progress_state_to_redis", true)
	env, conn, _ := setupEnv(t)
	if flushAfterCleanup {
		configureExperiments(t, env, map[string]bool{"remote_execution.flush_executions_after_cleanup": true})
	}
	client := repb.NewExecutionClient(conn)
	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("user1", "group1"))
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(ctx, "user1")
	require.NoError(t, err)

	const instanceName = "test-instance"
	const invocationID = "93383cc1-5d6c-4ad1-a321-8ee87c2f6816"

	clientCtx, err := bazel_request.WithRequestMetadata(ctx, &repb.RequestMetadata{
		ToolInvocationId: invocationID,
		TargetId:         "//some:test",
		ActionMnemonic:   "TestRunner",
	})
	require.NoError(t, err)

	arn := uploadAction(clientCtx, t, env, instanceName, repb.DigestFunction_SHA256, &repb.Action{
		Timeout: &durationpb.Duration{Seconds: 10},
		Platform: &repb.Platform{
			Properties: []*repb.Platform_Property{
				{Name: "pool", Value: "test-pool"},
				{Name: "workload-isolation-type", Value: "firecracker"},
				{Name: "EstimatedComputeUnits", Value: "2.5"},
			},
		},
	})
	executionClient, err := client.Execute(clientCtx, &repb.ExecuteRequest{
		InstanceName:   arn.GetInstanceName(),
		ActionDigest:   arn.GetDigest(),
		DigestFunction: arn.GetDigestFunction(),
	})
	require.NoError(t, err)
	require.NoError(t, executionClient.CloseSend())
	op, err := executionClient.Recv()
	require.NoError(t, err)
	taskID := op.GetName()

	executorCtx := metadata.AppendToOutgoingContext(clientCtx, usageutil.ClientHeaderName, "executor")
	executorCtx = metadata.AppendToOutgoingContext(executorCtx, "x-buildbuddy-executor-region", "test-region")

	queuedTime := time.Unix(100, 0)
	workerStartTime := queuedTime.Add(1 * time.Second)
	workerEndTime := workerStartTime.Add(5 * time.Second)
	aux := &espb.ExecutionAuxiliaryMetadata{
		PlatformOverrides: &repb.Platform{},
		IsolationType:     "firecracker",
		SchedulingMetadata: &scpb.SchedulingMetadata{
			ExecutorGroupId: sharedPoolGroupID,
			Pool:            "test-pool",
		},
	}
	auxAny, err := anypb.New(aux)
	require.NoError(t, err)

	// Stream 1: regular first COMPLETED, then close cleanly.
	stream1, err := client.PublishOperation(executorCtx)
	require.NoError(t, err)
	firstOp, err := operation.Assemble(taskID, operation.Metadata(repb.ExecutionStage_COMPLETED, arn.GetDigest()), &repb.ExecuteResponse{
		Result: &repb.ActionResult{
			ExecutionMetadata: &repb.ExecutedActionMetadata{
				QueuedTimestamp:          tspb.New(queuedTime),
				WorkerStartTimestamp:     tspb.New(workerStartTime),
				WorkerCompletedTimestamp: tspb.New(workerEndTime),
				AuxiliaryMetadata:        []*anypb.Any{auxAny},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, stream1.Send(firstOp))
	_, err = stream1.CloseAndRecv()
	require.NoError(t, err)

	// Stream 2 (retry): only a post-completion stats COMPLETED. This is what
	// the executor sends if it has to reopen the PublishOperation RPC after
	// the first COMPLETED was already delivered.
	stream2, err := client.PublishOperation(executorCtx)
	require.NoError(t, err)
	postCompletionAny, err := anypb.New(&espb.PostCompletionStats{
		PauseDurationUsec: 67890,
		FirecrackerPostExecStats: &espb.FirecrackerPostExecStats{
			SnapshotSavedLocally:  true,
			SnapshotSavedRemotely: true,
			SnapshotIsDiff:        true,
			SnapshotSavedBytes:    12345,
		},
	})
	require.NoError(t, err)
	postCompletionOp, err := operation.Assemble(taskID, operation.Metadata(repb.ExecutionStage_COMPLETED, arn.GetDigest()), &repb.ExecuteResponse{
		Result: &repb.ActionResult{
			ExecutionMetadata: &repb.ExecutedActionMetadata{
				AuxiliaryMetadata: []*anypb.Any{postCompletionAny},
			},
		},
	})
	require.NoError(t, err)
	require.NoError(t, stream2.Send(postCompletionOp))
	_, err = stream2.CloseAndRecv()
	require.NoError(t, err)

	// Drain /Execute so the test doesn't leak goroutines.
	for {
		_, err := executionClient.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
	}

	// Exactly one execution row, with the first-COMPLETED fields intact.
	collectedExecutions, err := env.GetExecutionCollector().GetExecutions(ctx, invocationID, 0, -1)
	require.NoError(t, err)
	require.Equal(t, 1, len(collectedExecutions), "expected exactly one flushed execution row")
	got := collectedExecutions[0]
	assert.Equal(t, taskID, got.GetExecutionId())
	assert.Equal(t, "//some:test", got.GetTargetLabel())
	assert.Equal(t, workerStartTime.UnixMicro(), got.GetWorkerStartTimestampUsec())
	assert.Equal(t, workerEndTime.UnixMicro(), got.GetWorkerCompletedTimestampUsec())

	// Action cache must still hold the result from the first COMPLETED, not
	// the sparse one from the retry stream.
	arn.ToProto().CacheType = rspb.CacheType_AC
	arnAC, err := arn.CheckAC()
	require.NoError(t, err)
	cachedAR, err := cachetools.GetActionResult(ctx, env.GetActionCacheClient(), arnAC)
	require.NoError(t, err, "action result should still be in cache from first COMPLETED")
	require.NotNil(t, cachedAR.GetExecutionMetadata())
	assert.Equal(t, workerStartTime.UnixMicro(), cachedAR.GetExecutionMetadata().GetWorkerStartTimestamp().AsTime().UnixMicro(),
		"action cache should not have been clobbered by the retry stream's sparse response")

	// Usage incremented exactly once.
	ut := env.GetUsageTracker().(*testusage.Tracker)
	var foundExecutorUsage *testusage.Total
	for _, u := range ut.Totals() {
		if u.Labels.Client == "executor" {
			foundExecutorUsage = &u
			break
		}
	}
	require.NotNil(t, foundExecutorUsage, "expected executor usage to be recorded")
	assert.Equal(t, (5 * time.Second).Microseconds(), foundExecutorUsage.Counts.LinuxExecutionDurationUsec)
}

func TestMarkFailed(t *testing.T) {
	env, _, _ := setupEnv(t)
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

	err = s.MarkExecutionFailed(ctx, "uploads/aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa/blobs/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/1", status.InternalError("It didn't work"))
	require.True(t, status.IsNotFoundError(err), "error should be NotFoundError, but was %s", err)
}

func TestDispatchFailure_MarksExecutionFailed(t *testing.T) {
	env, conn, _ := setupEnv(t)
	ctx := context.Background()

	ta := testauth.NewTestAuthenticator(t, testauth.TestUsers("US1", "GR1"))
	env.SetAuthenticator(ta)
	ctx, err := ta.WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	env.SetSecretService(nil)

	const iid = "10243d8a-a329-4f46-abfb-bfbceed12baa"
	ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
		ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
		ToolInvocationId: iid,
	})

	action := &repb.Action{
		Platform: &repb.Platform{Properties: []*repb.Platform_Property{
			{Name: "include-secrets", Value: "true"},
		}},
	}
	arn := uploadAction(ctx, t, env, "" /*=instanceName*/, repb.DigestFunction_SHA256, action)
	ad := arn.GetDigest()

	ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)

	client := repb.NewExecutionClient(conn)
	stream, err := client.Execute(ctx, &repb.ExecuteRequest{
		InstanceName:   arn.GetInstanceName(),
		ActionDigest:   ad,
		DigestFunction: arn.GetDigestFunction(),
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	require.Error(t, err)
	require.Contains(t, err.Error(), "Secrets requested but secret service not available")

	rows := getExecutions(t, env)
	require.Equal(t, 1, len(rows))
	require.Equal(t, int64(repb.ExecutionStage_COMPLETED), rows[0].Stage)

	executeResponse, err := execution.GetCachedExecuteResponse(ctx, env.GetActionCacheClient(), rows[0].ExecutionID)
	require.NoError(t, err)
	require.Contains(t, executeResponse.GetStatus().GetMessage(), "Secrets requested but secret service not available")
}

func TestDispatch_RedisAvailabilityMonitoring_CleansUpChannelOnScheduleFailure(t *testing.T) {
	flags.Set(t, "remote_execution.enable_redis_availability_monitoring", true)
	env, _, redisHandle := setupEnv(t)
	scheduler := env.GetSchedulerService().(*schedulerServerMock)
	scheduler.scheduleErr = status.UnavailableError("scheduler unavailable")

	ctx := context.Background()
	const iid = "10243d8a-a329-4f46-abfb-bfbceed12baa"
	ctx = withIncomingMetadata(t, ctx, &repb.RequestMetadata{
		ToolDetails:      &repb.ToolDetails{ToolName: "bazel", ToolVersion: "6.3.0"},
		ToolInvocationId: iid,
	})
	ctx, err := env.GetAuthenticator().(*testauth.TestAuthenticator).WithAuthenticatedUser(ctx, "US1")
	require.NoError(t, err)

	action := &repb.Action{}
	arn := uploadAction(ctx, t, env, "" /*=instanceName*/, repb.DigestFunction_SHA256, action)
	ad := arn.GetDigest()

	ctx, err = prefix.AttachUserPrefixToContext(ctx, env.GetAuthenticator())
	require.NoError(t, err)
	taskID := arn.NewUploadString()

	s := env.GetRemoteExecutionService()
	err = s.Dispatch(ctx, &repb.ExecuteRequest{ActionDigest: ad}, action, taskID)
	require.Error(t, err, "Dispatch should propagate the scheduler error")
	require.True(t, status.IsUnavailableError(err), "expected UNAVAILABLE error, got %s", err)

	// The monitored channel should be deleted on error.
	require.Equal(t, 0, redisHandle.KeyCount("monitoredPubSub/*"),
		"monitored pubsub channel should be deleted after scheduling failure")
}

func TestInvocationLink_EmptyInvocationID(t *testing.T) {
	flags.Set(t, "app.enable_write_executions_to_olap_db", true)
	env, conn, _ := setupEnv(t)
	client := repb.NewExecutionClient(conn)
	redis := testredis.Start(t)
	env.SetDefaultRedisClient(redis.Client())
	redis_execution_collector.Register(env)

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
	rsp, err := executionClient.Recv()
	require.NoError(t, err)

	// No invocation links should be recorded.
	invocationLinks, err := env.GetExecutionCollector().GetExecutionInvocationLinks(clientCtx, rsp.GetName())
	require.NoError(t, err)
	require.Empty(t, invocationLinks)

	err = executionClient.CloseSend()
	require.NoError(t, err)
}

func uploadAction(ctx context.Context, t *testing.T, env *real_environment.RealEnv, instanceName string, df repb.DigestFunction_Value, action *repb.Action) *digest.CASResourceName {
	cmd := &repb.Command{
		Arguments:   []string{"test"},
		OutputFiles: []string{"bazel-out/k8-fastbuild/bin/some/test"},
	}
	cd, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, df, cmd)
	require.NoError(t, err)
	action.CommandDigest = cd
	ad, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, df, action)
	require.NoError(t, err)
	return digest.NewCASResourceName(ad, instanceName, df)
}

func uploadActionWithCommand(ctx context.Context, t *testing.T, env *real_environment.RealEnv, instanceName string, df repb.DigestFunction_Value, action *repb.Action, cmd *repb.Command) *digest.CASResourceName {
	cd, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, df, cmd)
	require.NoError(t, err)
	action.CommandDigest = cd
	ad, err := cachetools.UploadProto(ctx, env.GetByteStreamClient(), instanceName, df, action)
	require.NoError(t, err)
	return digest.NewCASResourceName(ad, instanceName, df)
}

func withIncomingMetadata(t *testing.T, ctx context.Context, rmd *repb.RequestMetadata) context.Context {
	b, err := proto.Marshal(rmd)
	require.NoError(t, err)
	return metadata.NewIncomingContext(ctx, metadata.Pairs(bazel_request.RequestMetadataKey, string(b)))
}

func configureExperiments(t *testing.T, env *testenv.TestEnv, flags map[string]bool) {
	inMemoryFlags := make(map[string]memprovider.InMemoryFlag, len(flags))
	for name, enabled := range flags {
		inMemoryFlags[name] = memprovider.InMemoryFlag{
			State:          memprovider.Enabled,
			DefaultVariant: "default",
			Variants:       map[string]any{"default": enabled},
		}
	}
	testProvider := memprovider.NewInMemoryProvider(inMemoryFlags)
	require.NoError(t, openfeature.SetProviderAndWait(testProvider))
	fp, err := experiments.NewFlagProvider("test")
	require.NoError(t, err)
	env.SetExperimentFlagProvider(fp)
}

func makeAuxAny(t *testing.T, props []*repb.Platform_Property) *anypb.Any {
	auxMd := &espb.ExecutionAuxiliaryMetadata{
		PlatformOverrides: &repb.Platform{
			Properties: props,
		},
	}
	auxAny, err := anypb.New(auxMd)
	require.NoError(t, err)
	return auxAny
}

func TestRedactCachedExecuteResponse(t *testing.T) {
	for _, test := range []struct {
		name                      string
		response                  *repb.ExecuteResponse
		inputProperties           []*repb.Platform_Property
		expectedAuxiliaryMetadata []*anypb.Any
	}{
		{
			name:     "nil response",
			response: nil,
		},
		{
			name:     "no execution metadata",
			response: &repb.ExecuteResponse{},
		},
		{
			name: "no auxiliary metadata",
			response: &repb.ExecuteResponse{
				Result: &repb.ActionResult{
					ExecutionMetadata: &repb.ExecutedActionMetadata{},
				},
			},
		},
		{
			name: "redacts password property",
			inputProperties: []*repb.Platform_Property{
				{Name: "container-registry-password", Value: "secret123"},
			},
			expectedAuxiliaryMetadata: []*anypb.Any{
				makeAuxAny(t, []*repb.Platform_Property{
					{Name: "container-registry-password", Value: "<REDACTED>"},
				}),
			},
		},
		{
			name: "redacts username property",
			inputProperties: []*repb.Platform_Property{
				{Name: "container-registry-username", Value: "admin"},
			},
			expectedAuxiliaryMetadata: []*anypb.Any{
				makeAuxAny(t, []*repb.Platform_Property{
					{Name: "container-registry-username", Value: "<REDACTED>"},
				}),
			},
		},
		{
			name: "redacts env-overrides property",
			inputProperties: []*repb.Platform_Property{
				{Name: "env-overrides", Value: "SECRET_KEY=abc123"},
			},
			expectedAuxiliaryMetadata: []*anypb.Any{
				makeAuxAny(t, []*repb.Platform_Property{
					{Name: "env-overrides", Value: "<REDACTED>"},
				}),
			},
		},
		{
			name: "redacts secret-env-overrides property",
			inputProperties: []*repb.Platform_Property{
				{Name: "secret-env-overrides", Value: "SECRET_KEY=abc123"},
			},
			expectedAuxiliaryMetadata: []*anypb.Any{
				makeAuxAny(t, []*repb.Platform_Property{
					{Name: "secret-env-overrides", Value: "<REDACTED>"},
				}),
			},
		},
		{
			name: "case insensitive redaction",
			inputProperties: []*repb.Platform_Property{
				{Name: "PASSWORD", Value: "secret1"},
				{Name: "UserName", Value: "admin"},
				{Name: "ENV-OVERRIDES", Value: "KEY=val"},
			},
			expectedAuxiliaryMetadata: []*anypb.Any{
				makeAuxAny(t, []*repb.Platform_Property{
					{Name: "PASSWORD", Value: "<REDACTED>"},
					{Name: "UserName", Value: "<REDACTED>"},
					{Name: "ENV-OVERRIDES", Value: "<REDACTED>"},
				}),
			},
		},
		{
			name: "preserves non-sensitive properties",
			inputProperties: []*repb.Platform_Property{
				{Name: "pool", Value: "default"},
				{Name: "workload-isolation-type", Value: "firecracker"},
			},
			expectedAuxiliaryMetadata: []*anypb.Any{
				makeAuxAny(t, []*repb.Platform_Property{
					{Name: "pool", Value: "default"},
					{Name: "workload-isolation-type", Value: "firecracker"},
				}),
			},
		},
		{
			name: "mixed properties",
			inputProperties: []*repb.Platform_Property{
				{Name: "pool", Value: "default"},
				{Name: "container-registry-password", Value: "secret"},
				{Name: "container-registry-username", Value: "admin"},
				{Name: "workload-isolation-type", Value: "firecracker"},
			},
			expectedAuxiliaryMetadata: []*anypb.Any{
				makeAuxAny(t, []*repb.Platform_Property{
					{Name: "pool", Value: "default"},
					{Name: "container-registry-password", Value: "<REDACTED>"},
					{Name: "container-registry-username", Value: "<REDACTED>"},
					{Name: "workload-isolation-type", Value: "firecracker"},
				}),
			},
		},
		{
			name: "redacts multiple auxiliary metadata entries",
			response: &repb.ExecuteResponse{
				Result: &repb.ActionResult{
					ExecutionMetadata: &repb.ExecutedActionMetadata{
						AuxiliaryMetadata: []*anypb.Any{
							makeAuxAny(t, []*repb.Platform_Property{
								{Name: "container-registry-password", Value: "secret1"},
							}),
							makeAuxAny(t, []*repb.Platform_Property{
								{Name: "container-registry-password", Value: "secret2"},
							}),
						},
					},
				},
			},
			expectedAuxiliaryMetadata: []*anypb.Any{
				makeAuxAny(t, []*repb.Platform_Property{
					{Name: "container-registry-password", Value: "<REDACTED>"},
				}),
				makeAuxAny(t, []*repb.Platform_Property{
					{Name: "container-registry-password", Value: "<REDACTED>"},
				}),
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			var rsp *repb.ExecuteResponse

			if test.response != nil {
				rsp = test.response
			} else if test.inputProperties != nil {
				auxMd := &espb.ExecutionAuxiliaryMetadata{
					PlatformOverrides: &repb.Platform{
						Properties: test.inputProperties,
					},
				}
				auxAny, err := anypb.New(auxMd)
				require.NoError(t, err)
				rsp = &repb.ExecuteResponse{
					Result: &repb.ActionResult{
						ExecutionMetadata: &repb.ExecutedActionMetadata{
							AuxiliaryMetadata: []*anypb.Any{auxAny},
						},
					},
				}
			}

			execution_server.RedactCachedExecuteResponse(ctx, rsp)

			if test.expectedAuxiliaryMetadata != nil {
				require.Empty(t, cmp.Diff(
					test.expectedAuxiliaryMetadata,
					rsp.GetResult().GetExecutionMetadata().GetAuxiliaryMetadata(),
					protocmp.Transform(),
				))
			}
		})
	}
}
