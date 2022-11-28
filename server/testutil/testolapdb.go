package testolapdb

import (
	"context"

	"github.com/buildbuddy-io/buildbuddy/server/tables"
)

type TestOLAPDB struct{}

func (h *TestOLAPDB) DateFromUsecTimestamp(fieldNmae string, timezoneOffsetMinutes int32) string {
	return ""
}

func (h *TestOLAPDB) FlushInvocationStats(ctx context.Context, ti *tables.Invocation) error {
	return nil
}

func (h *TestOLAPDB) FlushExecutionStats(
