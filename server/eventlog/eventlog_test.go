package eventlog_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/buildbuddy-io/buildbuddy/server/backends/chunkstore"
	"github.com/buildbuddy-io/buildbuddy/server/eventlog"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/tables"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/mockstore"
	"github.com/buildbuddy-io/buildbuddy/server/testutil/testenv"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	aclpb "github.com/buildbuddy-io/buildbuddy/proto/acl"
	elpb "github.com/buildbuddy-io/buildbuddy/proto/eventlog"
	telpb "github.com/buildbuddy-io/buildbuddy/proto/telemetry"
)

type MockInvocationDB struct {
	db map[string]*tables.Invocation
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
	inv, ok := m.db[invocationID]
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


func TestGetEventLogChunkMaxBufferSize(t *testing.T) {
	env := testenv.GetTestEnv(t)

	invocationDB := &MockInvocationDB{db: make(map[string]*tables.Invocation)}
	env.SetInvocationDB(invocationDB)

	testID := "test_id"
	invocationDB.db[testID] = &tables.Invocation{
		InvocationID: testID,
		Attempt: 1,
		LastChunkId: fmt.Sprintf("%04x", uint16(3)),
	}

	blobstore := mockstore.New()
	env.SetBlobstore(blobstore)

	blobPath := eventlog.GetEventLogPathFromInvocationIdAndAttempt(testID, 1)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(0))] = make([]byte, 8)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(1))] = make([]byte, 8)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(2))] = make([]byte, 8)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(3))] = make([]byte, 8)
	blobstore.BlobMap[chunkstore.ChunkName(blobPath, uint16(4))] = make([]byte, 8)

	eventlog.MaxBufferSize = 20
	rsp, err := eventlog.GetEventLogChunk(
		env.GetServerContext(),
		env,
		&elpb.GetEventLogChunkRequest{
			InvocationId: testID,
			ChunkId: fmt.Sprintf("%04x", uint16(0)),
			MinLines: 1_000_000_000,
		},
	)
	require.NoError(t, err)
	assert.Len(t, rsp.Buffer, 16)
	assert.Equal(t, rsp.NextChunkId, fmt.Sprintf("%04x", 2))
}
