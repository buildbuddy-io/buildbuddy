package execution_service_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/RoaringBitmap/roaring"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_execution_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/execution_service"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/cachetools"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testauth"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testcache"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/perms"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/testing/flags"
	"github.com/buildbuddy-io/buildbuddy/server/util/uuid"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"

	capb "github.com/buildbuddy-io/buildbuddy/proto/cache"
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
				require.NoError(t, clickhouse.FillExecutionResourceFields(execution))
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

func TestGetExecutionDownloads(t *testing.T) {
	flags.Set(t, "remote_execution.primary_db_reads_enabled", true)
	flags.Set(t, "remote_execution.olap_reads_enabled", false)

	app := testenv.GetTestEnv(t)
	_, runServer, lis := testenv.RegisterLocalGRPCServer(t, app)
	testcache.Setup(t, app, lis)
	go runServer()

	ownerUser := testauth.User("US1", "GR1")
	groupMemberUser := testauth.User("US2", "GR1")
	outsiderUser := testauth.User("US3", "GR2")
	app.SetAuthenticator(testauth.NewTestAuthenticator(t, testauth.TestUsers(
		"US1", "GR1",
		"US2", "GR1",
		"US3", "GR2",
	)))

	service := execution_service.NewExecutionService(app)
	ownerCtx := testauth.WithAuthenticatedUserInfo(context.Background(), ownerUser)
	groupMemberCtx := testauth.WithAuthenticatedUserInfo(context.Background(), groupMemberUser)
	outsiderCtx := testauth.WithAuthenticatedUserInfo(context.Background(), outsiderUser)
	digestFunction := repb.DigestFunction_SHA256
	instanceName := "instance"

	createExecution := func(name string, invocationPerms, executionPerms int32) (string, string, []*capb.ExecutionDownload) {
		smallDigest, err := digest.Compute(strings.NewReader(name+"-small-data"), digestFunction)
		require.NoError(t, err)
		mediumDigest, err := digest.Compute(strings.NewReader(name+"-medium-data-medium"), digestFunction)
		require.NoError(t, err)
		largeDigest, err := digest.Compute(strings.NewReader(name+"-large-data-large-data-large"), digestFunction)
		require.NoError(t, err)
		tinyDigest, err := digest.Compute(strings.NewReader(name+"-tiny"), digestFunction)
		require.NoError(t, err)

		subdir := &repb.Directory{
			Files: []*repb.FileNode{
				{Name: "large.txt", Digest: largeDigest},
				{Name: "tiny.txt", Digest: tinyDigest},
			},
		}
		subdirDigest, err := cachetools.UploadProto(ownerCtx, app.GetByteStreamClient(), instanceName, digestFunction, subdir)
		require.NoError(t, err)

		rootDir := &repb.Directory{
			Files: []*repb.FileNode{
				{Name: "small.txt", Digest: smallDigest},
				{Name: "medium.txt", Digest: mediumDigest},
			},
			Directories: []*repb.DirectoryNode{
				{Name: "subdir", Digest: subdirDigest},
			},
		}
		rootDigest, err := cachetools.UploadProto(ownerCtx, app.GetByteStreamClient(), instanceName, digestFunction, rootDir)
		require.NoError(t, err)

		action := &repb.Action{InputRootDigest: rootDigest}
		actionDigest, err := cachetools.UploadProto(ownerCtx, app.GetByteStreamClient(), instanceName, digestFunction, action)
		require.NoError(t, err)

		invocationID := name + "-invocation"
		// Execution IDs are upload resource names whose digest points back to the
		// Action proto above.
		executionID := digest.NewCASResourceName(actionDigest, instanceName, digestFunction).NewUploadString()
		nowUsec := time.Now().UnixMicro()

		err = app.GetDBHandle().GORM(ownerCtx, "create_invocation").Create(&tables.Invocation{
			InvocationID:     invocationID,
			UserID:           ownerUser.GetUserID(),
			GroupID:          ownerUser.GetGroupID(),
			InvocationStatus: int64(inspb.InvocationStatus_COMPLETE_INVOCATION_STATUS),
			Perms:            invocationPerms,
			Model: tables.Model{
				CreatedAtUsec: nowUsec,
				UpdatedAtUsec: nowUsec,
			},
		}).Error
		require.NoError(t, err)
		err = app.GetDBHandle().GORM(ownerCtx, "create_execution").Create(&tables.Execution{
			ExecutionID:         executionID,
			UserID:              ownerUser.GetUserID(),
			GroupID:             ownerUser.GetGroupID(),
			InvocationID:        invocationID,
			Perms:               executionPerms,
			QueuedTimestampUsec: nowUsec,
			Model: tables.Model{
				CreatedAtUsec: nowUsec,
				UpdatedAtUsec: nowUsec,
			},
		}).Error
		require.NoError(t, err)
		err = app.GetDBHandle().GORM(ownerCtx, "create_invocation_execution").Create(&tables.InvocationExecution{
			InvocationID: invocationID,
			ExecutionID:  executionID,
		}).Error
		require.NoError(t, err)

		bitmap := roaring.New()
		// Match the tree traversal order:
		// 0=small.txt, 1=medium.txt, 2=subdir/large.txt, 3=subdir/tiny.txt
		bitmap.AddMany([]uint32{0, 2, 3})
		bitmapBytes, err := bitmap.MarshalBinary()
		require.NoError(t, err)

		auxMetadata := &espb.ExecutionAuxiliaryMetadata{
			InputFetchDetailedStats: &espb.InputFetchMetadata{
				DownloadedFileIndicesBitmap: bitmapBytes,
			},
		}
		auxAny, err := anypb.New(auxMetadata)
		require.NoError(t, err)

		executeResponse := &repb.ExecuteResponse{
			Result: &repb.ActionResult{
				ExecutionMetadata: &repb.ExecutedActionMetadata{
					AuxiliaryMetadata: []*anypb.Any{auxAny},
				},
			},
		}
		executeResponseBytes, err := proto.Marshal(executeResponse)
		require.NoError(t, err)

		executeResponseDigest, err := digest.Compute(strings.NewReader(executionID), digestFunction)
		require.NoError(t, err)
		// Store the synthetic ExecuteResponse in AC the same way production code
		// does so GetExecutionDownloads can load it by execution ID.
		err = cachetools.UploadActionResult(
			ownerCtx,
			app.GetActionCacheClient(),
			digest.NewACResourceName(executeResponseDigest, instanceName, digestFunction),
			&repb.ActionResult{StdoutRaw: executeResponseBytes},
		)
		require.NoError(t, err)

		return invocationID, executionID, []*capb.ExecutionDownload{
			{Path: "subdir/large.txt", Digest: largeDigest},
			{Path: "small.txt", Digest: smallDigest},
			{Path: "subdir/tiny.txt", Digest: tinyDigest},
		}
	}

	groupReadableInvocationID, groupReadableExecutionID, groupReadableDownloads := createExecution("group-readable", perms.GROUP_READ, perms.GROUP_READ)
	ownerOnlyInvocationID, ownerOnlyExecutionID, ownerOnlyDownloads := createExecution("owner-only", perms.OWNER_READ, perms.OWNER_READ)

	t.Run("pagination", func(t *testing.T) {
		// The service sorts downloads by descending size, so the first page should
		// contain the large file followed by the small root file.
		firstPage, err := service.GetExecutionDownloads(groupMemberCtx, &capb.GetExecutionDownloadsRequest{
			InvocationId: groupReadableInvocationID,
			ExecutionId:  groupReadableExecutionID,
			PageSize:     2,
		})
		require.NoError(t, err)
		require.Empty(t, cmp.Diff(groupReadableDownloads[:2], firstPage.GetDownloads(), protocmp.Transform()))
		require.NotEmpty(t, firstPage.GetNextPageToken())

		secondPage, err := service.GetExecutionDownloads(groupMemberCtx, &capb.GetExecutionDownloadsRequest{
			InvocationId: groupReadableInvocationID,
			ExecutionId:  groupReadableExecutionID,
			PageToken:    firstPage.GetNextPageToken(),
		})
		require.NoError(t, err)
		require.Empty(t, cmp.Diff(groupReadableDownloads[2:], secondPage.GetDownloads(), protocmp.Transform()))
		require.Empty(t, secondPage.GetNextPageToken())
	})

	for _, testCase := range []struct {
		name          string
		ctx           context.Context
		invocationID  string
		executionID   string
		wantDownloads []*capb.ExecutionDownload
		wantErr       func(error) bool
	}{
		{
			name:          "owner_can_read_owner_only_invocation",
			ctx:           ownerCtx,
			invocationID:  ownerOnlyInvocationID,
			executionID:   ownerOnlyExecutionID,
			wantDownloads: ownerOnlyDownloads,
		},
		{
			name:          "group_member_can_read_group_readable_invocation",
			ctx:           groupMemberCtx,
			invocationID:  groupReadableInvocationID,
			executionID:   groupReadableExecutionID,
			wantDownloads: groupReadableDownloads,
		},
		{
			name:         "same_group_non_owner_cannot_read_owner_only_invocation",
			ctx:          groupMemberCtx,
			invocationID: ownerOnlyInvocationID,
			executionID:  ownerOnlyExecutionID,
			wantErr:      status.IsPermissionDeniedError,
		},
		{
			name:         "different_group_member_cannot_read_group_readable_invocation",
			ctx:          outsiderCtx,
			invocationID: groupReadableInvocationID,
			executionID:  groupReadableExecutionID,
			wantErr:      status.IsPermissionDeniedError,
		},
		{
			name:         "execution_id_must_belong_to_requested_invocation",
			ctx:          groupMemberCtx,
			invocationID: groupReadableInvocationID,
			executionID:  ownerOnlyExecutionID,
			wantErr:      status.IsNotFoundError,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			rsp, err := service.GetExecutionDownloads(testCase.ctx, &capb.GetExecutionDownloadsRequest{
				InvocationId: testCase.invocationID,
				ExecutionId:  testCase.executionID,
			})
			if testCase.wantErr != nil {
				require.Error(t, err)
				require.True(t, testCase.wantErr(err), "unexpected error: %s", err)
				return
			}

			require.NoError(t, err)
			require.Empty(t, cmp.Diff(testCase.wantDownloads, rsp.GetDownloads(), protocmp.Transform()))
			require.Empty(t, rsp.GetNextPageToken())
		})
	}
}
