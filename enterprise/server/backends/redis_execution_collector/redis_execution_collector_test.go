package redis_execution_collector_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/backends/redis_execution_collector"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/testutil/testredis"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/testing/protocmp"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
)

func TestCollectExecutionUpdates(t *testing.T) {
	rdb := testredis.Start(t)
	collector := redis_execution_collector.New(rdb.Client())

	executionID := "execution-123"
	invocationID := "invocation-123"
	invocationUUID := "invocation123"
	updates := []*repb.StoredExecution{
		{
			ExecutionId:    executionID,
			InvocationUuid: invocationUUID,
			Stage:          int64(repb.ExecutionStage_QUEUED),
			CommandSnippet: "echo Hello",
		},
		{
			ExecutionId: executionID,
			Stage:       int64(repb.ExecutionStage_EXECUTING),
		},
		{
			ExecutionId:                  executionID,
			Stage:                        int64(repb.ExecutionStage_COMPLETED),
			ExitCode:                     1,
			WorkerStartTimestampUsec:     1e6,
			WorkerCompletedTimestampUsec: 2e6,
		},
		// Updates from duplicate attempts should be ignored. This can happen
		// since the scheduler currently doesn't provide a guarantee that each
		// execution is only completed once, though task leasing provides
		// best-effort protection against duplicates.
		{
			ExecutionId: executionID,
			Stage:       int64(repb.ExecutionStage_EXECUTING),
		},
		{
			ExecutionId:                  executionID,
			Stage:                        int64(repb.ExecutionStage_COMPLETED),
			ExitCode:                     2,
			WorkerStartTimestampUsec:     10e6,
			WorkerCompletedTimestampUsec: 20e6,
		},
	}

	// Add all updates to the collector.
	ctx := context.Background()
	for _, update := range updates {
		err := collector.UpdateInProgressExecution(ctx, update)
		require.NoError(t, err)
	}

	// Also add an invocation link, associating an invocation with the execution.
	err := collector.AddExecutionInvocationLink(ctx, &sipb.StoredInvocationLink{
		ExecutionId:  executionID,
		InvocationId: invocationID,
	}, true /*=storeReverseLink*/)
	require.NoError(t, err)

	// Read the updates back, using the execution ID.
	execution, err := collector.GetInProgressExecution(ctx, executionID)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff(&repb.StoredExecution{
		ExecutionId:                  executionID,
		InvocationUuid:               invocationUUID,
		Stage:                        int64(repb.ExecutionStage_COMPLETED),
		CommandSnippet:               "echo Hello",
		ExitCode:                     1,
		WorkerStartTimestampUsec:     1e6,
		WorkerCompletedTimestampUsec: 2e6,
	}, execution, protocmp.Transform()))

	// Read the updates back, using the invocation ID (via the reverse link).
	executions, err := collector.GetInProgressExecutions(ctx, invocationID)
	require.NoError(t, err)
	require.Empty(t, cmp.Diff([]*repb.StoredExecution{
		{
			ExecutionId:                  executionID,
			InvocationUuid:               invocationUUID,
			Stage:                        int64(repb.ExecutionStage_COMPLETED),
			CommandSnippet:               "echo Hello",
			ExitCode:                     1,
			WorkerStartTimestampUsec:     1e6,
			WorkerCompletedTimestampUsec: 2e6,
		},
	}, executions, protocmp.Transform()))

	// Delete reverse invocation links; should no longer be able to look up
	// in-progress executions by invocation ID.
	err = collector.DeleteInvocationExecutionLinks(ctx, invocationID)
	require.NoError(t, err)
	executions, err = collector.GetInProgressExecutions(ctx, invocationID)
	require.NoError(t, err)
	require.Nil(t, executions)

	// Delete the in-progress execution.
	err = collector.DeleteInProgressExecution(ctx, executionID)
	require.NoError(t, err)

	// Read the updates back again; should return not found error.
	_, err = collector.GetInProgressExecution(ctx, executionID)
	require.True(t, status.IsNotFoundError(err))
}

func TestGetInProgressExecutionsIgnoresDeletedExecutions(t *testing.T) {
	rdb := testredis.Start(t)
	collector := redis_execution_collector.New(rdb.Client())

	executionID := "execution-123"
	invocationID := "invocation-123"
	invocationUUID := "invocation123"

	// Create an in-progress execution and add an invocation link.
	ctx := context.Background()
	err := collector.UpdateInProgressExecution(ctx, &repb.StoredExecution{
		ExecutionId:    executionID,
		InvocationUuid: invocationUUID,
		Stage:          int64(repb.ExecutionStage_COMPLETED),
	})
	require.NoError(t, err)
	err = collector.AddExecutionInvocationLink(ctx, &sipb.StoredInvocationLink{
		ExecutionId:  executionID,
		InvocationId: invocationID,
	}, true /*=storeReverseLink*/)
	require.NoError(t, err)

	// Delete the in-progress execution.
	err = collector.DeleteInProgressExecution(ctx, executionID)
	require.NoError(t, err)

	// Get the in-progress executions for the invocation; should return nil
	// rather than a list with an empty/nil execution.
	executions, err := collector.GetInProgressExecutions(ctx, invocationID)
	require.NoError(t, err)
	require.Empty(t, executions)
}

func TestMergeExecutionUpdatesWithRepeatedFields(t *testing.T) {
	rdb := testredis.Start(t)
	collector := redis_execution_collector.New(rdb.Client())

	executionID := "execution-123"
	updates := []*repb.StoredExecution{
		{
			ExecutionId: executionID,
			Stage:       int64(repb.ExecutionStage_EXECUTING),
		},
		{
			ExecutionId: executionID,
			Stage:       int64(repb.ExecutionStage_COMPLETED),
		},
	}

	// Iterate over the fields of the StoredExecution proto and look for
	// slice-valued fields.
	sliceFields := map[string]struct{}{}
	val := reflect.ValueOf(updates[0]).Elem()
	for i := range val.NumField() {
		field := val.Field(i)
		if field.Kind() == reflect.Slice && field.CanSet() {
			sliceFields[val.Type().Field(i).Name] = struct{}{}
			// Allocate a new slice with the same type as the field, then set
			// both the initial and updated values to the same slice (containing
			// a single element with a zero value).
			slice := reflect.MakeSlice(field.Type(), 1, 1)
			field.Set(slice)
			reflect.ValueOf(updates[1]).Elem().Field(i).Set(slice)
		}
	}

	// Store all the updates then read them back.
	ctx := context.Background()
	for _, update := range updates {
		err := collector.UpdateInProgressExecution(ctx, update)
		require.NoError(t, err)
	}
	execution, err := collector.GetInProgressExecution(ctx, executionID)
	require.NoError(t, err)

	// Verify that the slice fields are merged correctly.
	executionValue := reflect.ValueOf(execution).Elem()
	for fieldName := range sliceFields {
		// Make sure the slice has length 1.
		if executionValue.FieldByName(fieldName).Len() != 1 {
			assert.Failf(t, "unhandled slice field", "StoredExecution field %q was concatenated instead of overwritten by mergeExecutionUpdates - method needs updating?", fieldName)
		}
	}
}
