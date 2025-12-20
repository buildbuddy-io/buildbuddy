package execution_service_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_execution_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	inspb "github.com/buildbuddy-io/buildbuddy/proto/invocation_status"
	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	olaptables "github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
)

func TestGetExecution_OLAPOnly(t *testing.T) {
	iid1 := uuid.New()
	ad1 := &repb.Digest{Hash: "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae", SizeBytes: 100}
	exid1 := digest.NewCASResourceName(ad1, "test-instance-name", repb.DigestFunction_SHA256).NewUploadString()
	rd1, err := digest.Compute(strings.NewReader(exid1), repb.DigestFunction_SHA256)
	require.NoError(t, err)
	testTimestampUsec := time.Now().UnixMicro()

	for _, test := range []struct {
		name                string
		invocations         []*tables.Invocation
		executions          []*olaptables.Execution
		authenticatedUserID string
		wantExecutions      []*espb.Execution
		wantErr             error
	}{
		{
			name: "anon invocation without executions",
			invocations: []*tables.Invocation{
				{
					InvocationID:     iid1,
					InvocationStatus: int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
					Perms:            perms.OTHERS_READ,
					Model:            tables.Model{CreatedAtUsec: testTimestampUsec, UpdatedAtUsec: testTimestampUsec},
				},
			},
			wantExecutions: []*espb.Execution{},
		},
		{
			name: "anon invocation with executions",
			invocations: []*tables.Invocation{
				{
					InvocationID:     iid1,
					InvocationStatus: int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
					Perms:            perms.OTHERS_READ,
					Model:            tables.Model{CreatedAtUsec: testTimestampUsec, UpdatedAtUsec: testTimestampUsec},
				},
			},
			executions: []*olaptables.Execution{
				{
					ExecutionID:    exid1,
					InvocationUUID: strings.ReplaceAll(iid1, "-", ""),
					CreatedAtUsec:  testTimestampUsec,
					UpdatedAtUsec:  testTimestampUsec,
				},
			},
			wantExecutions: []*espb.Execution{
				{
					ExecutionId:            exid1,
					ActionDigest:           ad1,
					ExecuteResponseDigest:  rd1,
					ExecutedActionMetadata: &repb.ExecutedActionMetadata{},
					Status:                 &statuspb.Status{},
				},
			},
		},
		{
			name: "group-owned invocation with executions",
			invocations: []*tables.Invocation{
				{
					InvocationID:     iid1,
					GroupID:          "GR1",
					InvocationStatus: int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
					Perms:            perms.GROUP_READ,
					Model:            tables.Model{CreatedAtUsec: testTimestampUsec, UpdatedAtUsec: testTimestampUsec},
				},
			},
			executions: []*olaptables.Execution{
				{
					ExecutionID:    exid1,
					GroupID:        "GR1",
					InvocationUUID: strings.ReplaceAll(iid1, "-", ""),
					CreatedAtUsec:  testTimestampUsec,
					UpdatedAtUsec:  testTimestampUsec,
				},
			},
			authenticatedUserID: "US1",
			wantExecutions: []*espb.Execution{
				{
					ExecutionId:            exid1,
					ActionDigest:           ad1,
					ExecuteResponseDigest:  rd1,
					ExecutedActionMetadata: &repb.ExecutedActionMetadata{},
					Status:                 &statuspb.Status{},
				},
			},
		},
		{
			name: "non-group-owned invocation with executions",
			invocations: []*tables.Invocation{
				{
					InvocationID:     iid1,
					GroupID:          "GR1",
					InvocationStatus: int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
					Perms:            perms.GROUP_READ,
					Model:            tables.Model{CreatedAtUsec: testTimestampUsec, UpdatedAtUsec: testTimestampUsec},
				},
			},
			executions: []*olaptables.Execution{
				{
					ExecutionID:    exid1,
					GroupID:        "GR1",
					InvocationUUID: strings.ReplaceAll(iid1, "-", ""),
					CreatedAtUsec:  testTimestampUsec,
					UpdatedAtUsec:  testTimestampUsec,
				},
			},
			authenticatedUserID: "US2",
			wantErr:             status.PermissionDeniedError("lookup invocation: You do not have permission to perform this action."),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			flags.Set(t, "testenv.use_clickhouse", true)
			flags.Set(t, "testenv.reuse_server", true)
			flags.Set(t, "remote_execution.primary_db_reads_enabled", false)
			flags.Set(t, "remote_execution.olap_reads_enabled", true)
			env := testenv.GetTestEnv(t)
			redis := testredis.Start(t)
			env.SetDefaultRedisClient(redis.Client())
			redis_execution_collector.Register(env)
			ta := testauth.NewTestAuthenticator(t, testauth.TestUsers(
				"US1", "GR1",
				"US2", "GR2",
			))
			env.SetAuthenticator(ta)
			es := execution_service.NewExecutionService(env)
			// Populate DB
			ctx := context.Background()
			for _, invocation := range test.invocations {
				// Set created_at and updated_at to a fixed value.
				invocation.CreatedAtUsec = testTimestampUsec
				invocation.UpdatedAtUsec = testTimestampUsec
				err := env.GetDBHandle().GORM(ctx, "test_create_invocation").Create(invocation).Error
				require.NoError(t, err)
			}
			for _, execution := range test.executions {
				execution.CreatedAtUsec = testTimestampUsec
				execution.UpdatedAtUsec = testTimestampUsec
				err := env.GetOLAPDBHandle().GORM(ctx, "test_create_execution").Create(execution).Error
				require.NoError(t, err)
			}

			if test.authenticatedUserID != "" {
				ctx, err = ta.WithAuthenticatedUser(ctx, test.authenticatedUserID)
				require.NoError(t, err)
			}
			rsp, err := es.GetExecution(ctx, &espb.GetExecutionRequest{
				ExecutionLookup: &espb.ExecutionLookup{
					InvocationId: test.invocations[0].InvocationID,
				},
			})
			if test.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				require.Equal(t, test.wantErr.Error(), err.Error())
			}
			require.Empty(t, cmp.Diff(
				test.wantExecutions, rsp.GetExecution(),
				protocmp.Transform(),
				protocmp.IgnoreFields(
					&espb.Execution{},
					// Ignore ExecutedActionMetadata for now (contains a bunch
					// of timestamps)
					"executed_action_metadata",
				),
			))
		})
	}
}
