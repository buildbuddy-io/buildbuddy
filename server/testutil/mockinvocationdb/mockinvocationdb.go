package mockinvocationdb

import (
	"context"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
)

type MockInvocationDB struct {
	DB map[string]*tables.Invocation
}

func (m *MockInvocationDB) CreateInvocation(ctx context.Context, in *tables.Invocation) (bool, error) {
	return false, nil
}
func (m *MockInvocationDB) UpdateInvocation(ctx context.Context, in *tables.Invocation) (bool, error) {
	return false, nil
}
func (m *MockInvocationDB) UpdateInvocationACL(ctx context.Context, authenticatedUser *interfaces.UserInfo, invocationID string, acl *aclpb.ACL) error {
	return nil
}
func (m *MockInvocationDB) LookupInvocation(ctx context.Context, invocationID string) (*tables.Invocation, error) {
	inv, ok := m.DB[invocationID]
	if !ok {
		return nil, status.NotFoundError("")
	}
	return inv, nil
}
func (m *MockInvocationDB) LookupGroupFromInvocation(ctx context.Context, invocationID string) (*tables.Group, error) {
	return nil, nil
}
func (m *MockInvocationDB) LookupGroupIDFromInvocation(ctx context.Context, invocationID string) (string, error) {
	return "", nil
}
func (m *MockInvocationDB) LookupExpiredInvocations(ctx context.Context, cutoffTime time.Time, limit int) ([]*tables.Invocation, error) {
	return nil, nil
}
func (m *MockInvocationDB) LookupChildInvocations(ctx context.Context, parentRunID string) ([]string, error) {
	return nil, nil
}
func (m *MockInvocationDB) DeleteInvocation(ctx context.Context, invocationID string) error {
	return nil
}
func (m *MockInvocationDB) DeleteInvocationWithPermsCheck(ctx context.Context, authenticatedUser *interfaces.UserInfo, invocationID string) error {
	return nil
}
func (m *MockInvocationDB) FillCounts(ctx context.Context, log *telpb.TelemetryStat) error {
	return nil
}
func (m *MockInvocationDB) SetNowFunc(now func() time.Time) {
}
