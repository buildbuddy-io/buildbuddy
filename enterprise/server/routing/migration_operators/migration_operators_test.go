package migration_operators_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/migration_operators"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ropb "github.com/buildbuddy-io/buildbuddy/proto/routing"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

// Mock ByteStream Read client
type mockReadClient struct {
	responses []*bspb.ReadResponse
	idx       int
	err       error
}

func (m *mockReadClient) Recv() (*bspb.ReadResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	if m.idx >= len(m.responses) {
		return nil, io.EOF
	}
	resp := m.responses[m.idx]
	m.idx++
	return resp, nil
}

func (m *mockReadClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockReadClient) Trailer() metadata.MD {
	return nil
}

func (m *mockReadClient) CloseSend() error {
	return nil
}

func (m *mockReadClient) Context() context.Context {
	return context.Background()
}

func (m *mockReadClient) SendMsg(interface{}) error {
	return nil
}

func (m *mockReadClient) RecvMsg(interface{}) error {
	return nil
}

// Mock ByteStream Write client
type mockWriteClient struct {
	requests       []*bspb.WriteRequest
	response       *bspb.WriteResponse
	err            error
	closeErr       error
	expectedOffset int64
}

func (m *mockWriteClient) Send(req *bspb.WriteRequest) error {
	if m.err != nil {
		return m.err
	}
	// Verify write offset
	if req.WriteOffset != m.expectedOffset {
		return status.InternalErrorf("Expected offset %d, got %d", m.expectedOffset, req.WriteOffset)
	}
	m.expectedOffset += int64(len(req.Data))
	m.requests = append(m.requests, req)
	return nil
}

func (m *mockWriteClient) CloseAndRecv() (*bspb.WriteResponse, error) {
	if m.closeErr != nil {
		return nil, m.closeErr
	}
	return m.response, nil
}

func (m *mockWriteClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockWriteClient) Trailer() metadata.MD {
	return nil
}

func (m *mockWriteClient) CloseSend() error {
	return nil
}

func (m *mockWriteClient) Context() context.Context {
	return context.Background()
}

func (m *mockWriteClient) SendMsg(interface{}) error {
	return nil
}

func (m *mockWriteClient) RecvMsg(interface{}) error {
	return nil
}

// Mock ByteStream client
type mockBSClient struct {
	readStreams  []*mockReadClient
	writeStreams []*mockWriteClient
	readIdx      int
	writeIdx     int
	readError    error
	writeError   error
}

func (m *mockBSClient) Read(ctx context.Context, in *bspb.ReadRequest, opts ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
	if m.readError != nil {
		return nil, m.readError
	}
	if m.readIdx >= len(m.readStreams) {
		return nil, status.InternalError("No more read streams")
	}
	stream := m.readStreams[m.readIdx]
	m.readIdx++
	return stream, nil
}

func (m *mockBSClient) Write(ctx context.Context, opts ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	if m.writeError != nil {
		return nil, m.writeError
	}
	if m.writeIdx >= len(m.writeStreams) {
		return nil, status.InternalError("No more write streams")
	}
	stream := m.writeStreams[m.writeIdx]
	m.writeIdx++
	return stream, nil
}

func (m *mockBSClient) QueryWriteStatus(ctx context.Context, in *bspb.QueryWriteStatusRequest, opts ...grpc.CallOption) (*bspb.QueryWriteStatusResponse, error) {
	return nil, status.UnimplementedError("QueryWriteStatus not implemented")
}

// Mock CacheRoutingService
type mockRouter struct {
	primary   bspb.ByteStreamClient
	secondary bspb.ByteStreamClient
	err       error
}

func (m *mockRouter) GetCacheRoutingConfig(ctx context.Context) (*ropb.CacheRoutingConfig, error) {
	return nil, status.UnimplementedError("GetCacheRoutingConfig not implemented")
}

func (m *mockRouter) GetCASClients(ctx context.Context) (repb.ContentAddressableStorageClient, repb.ContentAddressableStorageClient, error) {
	return nil, nil, status.UnimplementedError("GetCASClients not implemented")
}

func (m *mockRouter) GetACClients(ctx context.Context) (repb.ActionCacheClient, repb.ActionCacheClient, error) {
	return nil, nil, status.UnimplementedError("GetACClients not implemented")
}

func (m *mockRouter) GetBSClients(ctx context.Context) (bspb.ByteStreamClient, bspb.ByteStreamClient, error) {
	if m.err != nil {
		return nil, nil, m.err
	}
	return m.primary, m.secondary, nil
}

func (m *mockRouter) GetPrimaryCapabilitiesClient(ctx context.Context) (repb.CapabilitiesClient, error) {
	return nil, status.UnimplementedError("GetPrimaryCapabilitiesClient not implemented")
}

// Helper functions for creating test data
func digestProto(hash string, sizeBytes int64) *repb.Digest {
	return &repb.Digest{Hash: hash, SizeBytes: sizeBytes}
}

func TestByteStreamCopy_ReadError(t *testing.T) {
	ctx := context.Background()

	digest1 := digestProto(strings.Repeat("1", 64), 10)

	// Create mock read stream with error
	readStream1 := &mockReadClient{
		err: status.InternalError("read failed"),
	}

	// Create mock write stream (needed even though we'll fail on read)
	writeStream1 := &mockWriteClient{
		response: &bspb.WriteResponse{CommittedSize: 10},
	}

	primary := &mockBSClient{
		readStreams: []*mockReadClient{readStream1},
	}
	secondary := &mockBSClient{
		writeStreams: []*mockWriteClient{writeStream1},
	}

	router := &mockRouter{
		primary:   primary,
		secondary: secondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ByteStreamCopy(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read failed")
}

func TestByteStreamCopy_WriteError(t *testing.T) {
	ctx := context.Background()

	digest1 := digestProto(strings.Repeat("1", 64), 10)

	readStream1 := &mockReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
		},
	}

	// Create mock write stream with error
	writeStream1 := &mockWriteClient{
		err: status.InternalError("write failed"),
	}

	primary := &mockBSClient{
		readStreams: []*mockReadClient{readStream1},
	}
	secondary := &mockBSClient{
		writeStreams: []*mockWriteClient{writeStream1},
	}

	router := &mockRouter{
		primary:   primary,
		secondary: secondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ByteStreamCopy(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "write failed")
}

func TestByteStreamCopy_RouterError(t *testing.T) {
	ctx := context.Background()

	router := &mockRouter{
		err: status.InternalError("router failed"),
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ByteStreamCopy(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "router failed")
}

func TestByteStreamReadAndVerify_Success(t *testing.T) {
	ctx := context.Background()

	// Create test digests
	digest1 := digestProto(strings.Repeat("1", 64), 10)
	digest2 := digestProto(strings.Repeat("2", 64), 12)

	// Create mock read streams with correct sizes
	readStream1 := &mockReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	}
	readStream2 := &mockReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("test")},
			{Data: []byte("datamore")},
		},
	}

	secondary := &mockBSClient{
		readStreams: []*mockReadClient{readStream1, readStream2},
	}

	router := &mockRouter{
		secondary: secondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1, digest2},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	// Test with verification enabled
	err := migration_operators.ByteStreamReadAndVerify(ctx, router, true, "test-group", batch)
	require.NoError(t, err)
}

func TestByteStreamReadAndVerify_SizeMismatch(t *testing.T) {
	ctx := context.Background()

	// Create test digest with wrong size
	digest1 := digestProto(strings.Repeat("1", 64), 15) // Expected 15 bytes

	// Create mock read stream returning only 10 bytes
	readStream1 := &mockReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	}

	secondary := &mockBSClient{
		readStreams: []*mockReadClient{readStream1},
	}

	router := &mockRouter{
		secondary: secondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	// Test with verification enabled - should return error
	err := migration_operators.ByteStreamReadAndVerify(ctx, router, true, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Size mismatch")
	require.Contains(t, err.Error(), "expected 15, got 10")
}

func TestByteStreamReadAndVerify_NoVerify(t *testing.T) {
	ctx := context.Background()

	// Create test digest with wrong size
	digest1 := digestProto(strings.Repeat("1", 64), 15) // Expected 15 bytes

	// Create mock read stream returning only 10 bytes
	readStream1 := &mockReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	}

	secondary := &mockBSClient{
		readStreams: []*mockReadClient{readStream1},
	}

	router := &mockRouter{
		secondary: secondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	// Test with verification disabled - should not return error
	err := migration_operators.ByteStreamReadAndVerify(ctx, router, false, "test-group", batch)
	require.NoError(t, err)
}

func TestByteStreamReadAndVerify_ReadError(t *testing.T) {
	ctx := context.Background()

	digest1 := digestProto(strings.Repeat("1", 64), 10)

	// Create mock read stream with error
	readStream1 := &mockReadClient{
		err: status.InternalError("read failed"),
	}

	secondary := &mockBSClient{
		readStreams: []*mockReadClient{readStream1},
	}

	router := &mockRouter{
		secondary: secondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ByteStreamReadAndVerify(ctx, router, false, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "read failed")
}

func TestByteStreamReadAndVerify_RouterError(t *testing.T) {
	ctx := context.Background()

	router := &mockRouter{
		err: status.InternalError("router failed"),
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ByteStreamReadAndVerify(ctx, router, false, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "router failed")
}

func TestByteStreamReadAndVerify_MultipleErrors(t *testing.T) {
	ctx := context.Background()

	// Create test digests - both with wrong sizes
	digest1 := digestProto(strings.Repeat("1", 64), 15) // Expected 15 bytes, will get 10
	digest2 := digestProto(strings.Repeat("2", 64), 20) // Expected 20 bytes, will get 12

	// Create mock read streams with incorrect sizes
	readStream1 := &mockReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	}
	readStream2 := &mockReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("test")},
			{Data: []byte("datamore")},
		},
	}

	secondary := &mockBSClient{
		readStreams: []*mockReadClient{readStream1, readStream2},
	}

	router := &mockRouter{
		secondary: secondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1, digest2},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	// Test with verification enabled - should return last error
	err := migration_operators.ByteStreamReadAndVerify(ctx, router, true, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Size mismatch")
	require.Contains(t, err.Error(), "expected 20, got 12")
}
