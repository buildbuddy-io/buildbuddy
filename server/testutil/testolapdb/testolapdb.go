package testolapdb

import (
	"context"
	"sync"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"gorm.io/gorm"

	iepb "github.com/buildbuddy-io/buildbuddy/proto/internal_execution"
)

type TestOLAPDBHandle struct {
	executionIDsByInvID sync.Map // map of invocationID => a slice of execution IDs
	invIDs              sync.Map // map of invocationID => struct{}
}

func NewTestOLAPDBHandle() interfaces.OLAPDBHandle {
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
	h.executionIDsByInvID.LoadOrStore(ti.InvocationID, executionIDs)
	return nil
}
