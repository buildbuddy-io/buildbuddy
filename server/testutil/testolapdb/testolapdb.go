package testolapdb

import (
	"context"
	"sync"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	iepb "github.com/buildbuddy-io/buildbuddy/proto/internal_execution"
)

type TestOLAPDBHandle struct {
	executionIDsByInvID sync.Map // map of invocationID => a slice of execution IDs
	invIDs              sync.Map // map of invocationID => struct{}
}

func NewTestOLAPDBHandle() *TestOLAPDBHandle {
	return &TestOLAPDBHandle{
		executionIDsByInvID: sync.Map{},
	}
}

func (h *TestOLAPDBHandle) DB(ctx context.Context) *gorm.DB {
	return nil
}

func (h *TestOLAPDBHandle) DateFromUsecTimestamp(fieldNmae string, timezoneOffsetMinutes int32) string {
	return ""
}

func (h *TestOLAPDBHandle) FlushInvocationStats(ctx context.Context, ti *tables.Invocation) error {
	h.invIDs.LoadOrStore(ti.InvocationID, struct{}{})
	return nil
}

func (h *TestOLAPDBHandle) FlushExecutionStats(ctx context.Context, ti *tables.Invocation, executions []*iepb.Execution) error {
	executionIDs := make([]string, 0, len(executions))
	for _, e := range executions {
		executionIDs = append(executionIDs, e.GetExecutionId())
	}
	h.executionIDsByInvID.Store(ti.InvocationID, executionIDs)
	return nil
}

func (h *TestOLAPDBHandle) GetExecutionIDsByInvID(t *testing.T, invID string) []string {
	v, ok := h.executionIDsByInvID.Load(invID)
	require.True(t, ok, "invocation ID %q is not found in OLAP DB", invID)
	return v.([]string)
}

func (h *TestOLAPDBHandle) GetInvocationIDs() []string {
	res := []string{}
	h.executionIDsByInvID.Range(func(k, v interface{}) bool {
		invID := k.(string)
		res = append(res, invID)
		return true
	})
	return res
}
