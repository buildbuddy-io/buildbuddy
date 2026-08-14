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
	executionIDsByInvID   sync.Map // map of invocationID => a slice of execution IDs
	invIDs                sync.Map // map of invocationID => struct{}
	invocationsByInvID    sync.Map // map of invocationID => *tables.Invocation
	errorOccurrencesMu    sync.Mutex
	errorOccurrences      []*schema.ErrorOccurrence
	errorInvocationACLs   map[string]*schema.ErrorInvocationACL
	beforeInvocationFlush func()
	beforeErrorFlush      func()
	beforeErrorACLUpdate  func()
	errorFlushErr         error
	maxErrorACLVersionErr error
	nextErrorACLUpdateErr error
}

func (h *Handle) FlushErrorOccurrences(ctx context.Context, entries []*schema.ErrorOccurrence) error {
	if h.beforeErrorFlush != nil {
		h.beforeErrorFlush()
	}
	h.errorOccurrencesMu.Lock()
	defer h.errorOccurrencesMu.Unlock()
	h.errorOccurrences = append(h.errorOccurrences, entries...)
	return h.errorFlushErr
}

func (h *Handle) SetBeforeErrorFlush(hook func()) {
	h.beforeErrorFlush = hook
}

func (h *Handle) SetBeforeInvocationFlush(hook func()) {
	h.beforeInvocationFlush = hook
}

func (h *Handle) SetErrorFlushError(err error) {
	h.errorFlushErr = err
}

func (h *Handle) SetBeforeErrorACLUpdate(hook func()) {
	h.beforeErrorACLUpdate = hook
}

// SetNextErrorACLUpdateError makes the next ACL flush persist the supplied
// entry and then return err, simulating an ambiguous ClickHouse timeout.
func (h *Handle) SetNextErrorACLUpdateError(err error) {
	h.errorOccurrencesMu.Lock()
	defer h.errorOccurrencesMu.Unlock()
	h.nextErrorACLUpdateErr = err
}

func (h *Handle) GetErrorOccurrences() []*schema.ErrorOccurrence {
	h.errorOccurrencesMu.Lock()
	defer h.errorOccurrencesMu.Unlock()
	return append([]*schema.ErrorOccurrence(nil), h.errorOccurrences...)
}

func (h *Handle) FlushErrorInvocationACL(ctx context.Context, entry *schema.ErrorInvocationACL) error {
	if h.beforeErrorACLUpdate != nil {
		h.beforeErrorACLUpdate()
	}
	h.errorOccurrencesMu.Lock()
	defer h.errorOccurrencesMu.Unlock()
	if h.errorInvocationACLs == nil {
		h.errorInvocationACLs = make(map[string]*schema.ErrorInvocationACL)
	}
	if current := h.errorInvocationACLs[entry.InvocationID]; current == nil || current.ACLVersion <= entry.ACLVersion {
		copy := *entry
		h.errorInvocationACLs[entry.InvocationID] = &copy
	}
	err := h.nextErrorACLUpdateErr
	h.nextErrorACLUpdateErr = nil
	return err
}

func (h *Handle) GetErrorInvocationACL(invocationID string) *schema.ErrorInvocationACL {
	h.errorOccurrencesMu.Lock()
	defer h.errorOccurrencesMu.Unlock()
	entry := h.errorInvocationACLs[invocationID]
	if entry == nil {
		return nil
	}
	copy := *entry
	return &copy
}

func (h *Handle) GetMaxErrorInvocationACLVersion(ctx context.Context, groupID, invocationID string) (int64, error) {
	h.errorOccurrencesMu.Lock()
	defer h.errorOccurrencesMu.Unlock()
	if h.maxErrorACLVersionErr != nil {
		return 0, h.maxErrorACLVersionErr
	}
	entry := h.errorInvocationACLs[invocationID]
	if entry == nil || entry.GroupID != groupID {
		return 0, nil
	}
	return entry.ACLVersion, nil
}

func (h *Handle) SetMaxErrorInvocationACLVersionError(err error) {
	h.errorOccurrencesMu.Lock()
	defer h.errorOccurrencesMu.Unlock()
	h.maxErrorACLVersionErr = err
}

func (h *Handle) ResetErrorTrackingInvocation(ctx context.Context, groupID, invocationID, currentIncarnation string) error {
	h.errorOccurrencesMu.Lock()
	defer h.errorOccurrencesMu.Unlock()
	filtered := h.errorOccurrences[:0]
	for _, occurrence := range h.errorOccurrences {
		if occurrence.GroupID != groupID || occurrence.InvocationID != invocationID {
			filtered = append(filtered, occurrence)
		}
	}
	h.errorOccurrences = filtered
	delete(h.errorInvocationACLs, invocationID)
	h.executionIDsByInvID.Delete(invocationID)
	h.invIDs.Delete(invocationID)
	h.invocationsByInvID.Delete(invocationID)
	return nil
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
	if h.beforeInvocationFlush != nil {
		h.beforeInvocationFlush()
	}
	h.invIDs.LoadOrStore(ti.InvocationID, struct{}{})
	h.invocationsByInvID.Store(ti.InvocationID, ti)
	return nil
}

// GetFlushedInvocation returns the invocation flushed with the given
// invocation ID, or nil if no invocation with that ID was flushed.
func (h *Handle) GetFlushedInvocation(invID string) *tables.Invocation {
	v, ok := h.invocationsByInvID.Load(invID)
	if !ok {
		return nil
	}
	return v.(*tables.Invocation)
}

func (h *Handle) FlushUsages(ctx context.Context, rows []*schema.RawUsage) error {
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
