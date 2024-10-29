package testolapdb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/clickhouse/schema"
	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	sipb "github.com/buildbuddy-io/buildbuddy/proto/stored_invocation"
)

type Handle struct {
	executionIDsByInvID sync.Map // map of invocationID => a slice of execution IDs
	invIDs              sync.Map // map of invocationID => struct{}
}

func (h *Handle) DialectName() string {
	return "clickhouse"
}

func (h *Handle) NewQuery(ctx context.Context, name string) interfaces.DBQuery {
	return nil
}

func (h *Handle) GORM(ctx context.Context, name string) *gorm.DB {
	return nil
}

func (h *Handle) NowFunc() time.Time {
	return time.Time{}
}

func NewHandle() *Handle {
	return &Handle{
		executionIDsByInvID: sync.Map{},
	}
}

func (h *Handle) BucketFromUsecTimestamp(fieldName string, loc *time.Location, interval string) (string, []interface{}) {
	return "", nil
}

func (h *Handle) DateFromUsecTimestamp(fieldNmae string, timezoneOffsetMinutes int32) string {
	return ""
}

func (h *Handle) FlushInvocationStats(ctx context.Context, ti *tables.Invocation) error {
	h.invIDs.LoadOrStore(ti.InvocationID, struct{}{})
	return nil
}

func (h *Handle) InsertAuditLog(ctx context.Context, entry *schema.AuditLog) error {
	return nil
}

func (h *Handle) FlushExecutionStats(ctx context.Context, inv *sipb.StoredInvocation, executions []*repb.StoredExecution) error {
	executionIDs := make([]string, 0, len(executions))
	for _, e := range executions {
		executionIDs = append(executionIDs, e.GetExecutionId())
	}
	h.executionIDsByInvID.Store(inv.GetInvocationId(), executionIDs)
	return nil
}

func (h *Handle) FlushTestTargetStatuses(ctx context.Context, entries []*schema.TestTargetStatus) error {
	return errors.New("Not implemented")
}

func (h *Handle) GetExecutionIDsByInvID(t *testing.T, invID string) []string {
	v, ok := h.executionIDsByInvID.Load(invID)
	require.True(t, ok, "invocation ID %q is not found in OLAP DB", invID)
	return v.([]string)
}

func (h *Handle) GetInvocationIDs() []string {
	res := []string{}
	h.executionIDsByInvID.Range(func(k, v interface{}) bool {
		invID := k.(string)
		res = append(res, invID)
		return true
	})
	return res
}
