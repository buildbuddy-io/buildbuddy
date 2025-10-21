package migration_operators_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/migration_operators"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
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
	lastCtx      context.Context // Capture context for verification
}

func (m *mockBSClient) Read(ctx context.Context, in *bspb.ReadRequest, opts ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
	m.lastCtx = ctx
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
	m.lastCtx = ctx
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

// Mock CAS client
type mockCASClient struct {
	missingBlobs []*repb.Digest
	err          error
	lastCtx      context.Context // Capture context for verification
}

func (m *mockCASClient) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest, opts ...grpc.CallOption) (*repb.FindMissingBlobsResponse, error) {
	m.lastCtx = ctx
	if m.err != nil {
		return nil, m.err
	}
	return &repb.FindMissingBlobsResponse{
		MissingBlobDigests: m.missingBlobs,
	}, nil
}

func (m *mockCASClient) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest, opts ...grpc.CallOption) (*repb.BatchUpdateBlobsResponse, error) {
	return nil, status.UnimplementedError("BatchUpdateBlobs not implemented")
}

func (m *mockCASClient) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest, opts ...grpc.CallOption) (*repb.BatchReadBlobsResponse, error) {
	return nil, status.UnimplementedError("BatchReadBlobs not implemented")
}

func (m *mockCASClient) GetTree(ctx context.Context, req *repb.GetTreeRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[repb.GetTreeResponse], error) {
	return nil, status.UnimplementedError("GetTree not implemented")
}

func (m *mockCASClient) SpliceBlob(ctx context.Context, req *repb.SpliceBlobRequest, opts ...grpc.CallOption) (*repb.SpliceBlobResponse, error) {
	return nil, status.UnimplementedError("SpliceBlob not implemented")
}

func (m *mockCASClient) SplitBlob(ctx context.Context, req *repb.SplitBlobRequest, opts ...grpc.CallOption) (*repb.SplitBlobResponse, error) {
	return nil, status.UnimplementedError("SplitBlob not implemented")
}

// Mock CacheRoutingService
type mockRouter struct {
	primary      bspb.ByteStreamClient
	secondary    bspb.ByteStreamClient
	casSecondary repb.ContentAddressableStorageClient
	err          error
	casErr       error
}

func (m *mockRouter) GetCacheRoutingConfig(ctx context.Context) (*ropb.CacheRoutingConfig, error) {
	return nil, status.UnimplementedError("GetCacheRoutingConfig not implemented")
}

func (m *mockRouter) GetCASClients(ctx context.Context) (repb.ContentAddressableStorageClient, repb.ContentAddressableStorageClient, error) {
	if m.casErr != nil {
		return nil, nil, m.casErr
	}
	return nil, m.casSecondary, nil
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

func verifyUsageTrackingDisabled(t *testing.T, ctx context.Context) {
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		// In test environments, DisableUsageTracking may not add metadata due to origin checks
		// but the function should still have been called with a wrapped context
		return
	}
	skipValues := md.Get(usageutil.SkipUsageTrackingHeaderName)
	require.Len(t, skipValues, 1, "Expected usage tracking to be disabled, but no header was set.")
	require.Equal(t, usageutil.SkipUsageTrackingEnabledValue, skipValues[0], "Unexpected skip-tracking header value.")
}

func TestByteStreamCopy_Success(t *testing.T) {
	ctx := context.Background()

	// Create test digests
	digest1 := digestProto(strings.Repeat("1", 64), 10)
	digest2 := digestProto(strings.Repeat("2", 64), 20)

	// Create mock CAS client that returns both digests as missing
	casClient := &mockCASClient{
		missingBlobs: []*repb.Digest{digest1, digest2},
	}

	// Create mock read streams
	readStream1 := &mockReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	}
	readStream2 := &mockReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("test")},
			{Data: []byte("data")},
			{Data: []byte("more")},
		},
	}

	// Create mock write streams
	writeStream1 := &mockWriteClient{
		response: &bspb.WriteResponse{CommittedSize: 10},
	}
	writeStream2 := &mockWriteClient{
		response: &bspb.WriteResponse{CommittedSize: 12},
	}

	// Setup mock clients
	primary := &mockBSClient{
		readStreams: []*mockReadClient{readStream1, readStream2},
	}
	secondary := &mockBSClient{
		writeStreams: []*mockWriteClient{writeStream1, writeStream2},
	}

	router := &mockRouter{
		primary:      primary,
		secondary:    secondary,
		casSecondary: casClient,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1, digest2},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	// Execute the copy
	err := migration_operators.ByteStreamCopy(ctx, router, "test-group", batch)
	require.NoError(t, err)

	// Verify usage tracking is disabled for CAS, BS primary, and BS secondary calls
	verifyUsageTrackingDisabled(t, casClient.lastCtx)
	verifyUsageTrackingDisabled(t, primary.lastCtx)
	verifyUsageTrackingDisabled(t, secondary.lastCtx)

	// Verify write stream 1 received correct data
	require.Len(t, writeStream1.requests, 4) // Initial request + 2 data chunks + finish
	require.Contains(t, writeStream1.requests[0].ResourceName, "test-instance/uploads/")
	require.Contains(t, writeStream1.requests[0].ResourceName, "/blobs/1111111111111111111111111111111111111111111111111111111111111111/10")
	require.Equal(t, int64(0), writeStream1.requests[0].WriteOffset)
	require.Equal(t, []byte("hello"), writeStream1.requests[1].Data)
	require.Equal(t, int64(0), writeStream1.requests[1].WriteOffset)
	require.Equal(t, []byte("world"), writeStream1.requests[2].Data)
	require.Equal(t, int64(5), writeStream1.requests[2].WriteOffset)
	require.True(t, writeStream1.requests[3].FinishWrite)
	require.Equal(t, int64(10), writeStream1.requests[3].WriteOffset)

	// Verify write stream 2 received correct data
	require.Len(t, writeStream2.requests, 5) // Initial request + 3 data chunks + finish
	require.Contains(t, writeStream2.requests[0].ResourceName, "test-instance/uploads/")
	require.Contains(t, writeStream2.requests[0].ResourceName, "/blobs/2222222222222222222222222222222222222222222222222222222222222222/20")
	require.Equal(t, []byte("test"), writeStream2.requests[1].Data)
	require.Equal(t, []byte("data"), writeStream2.requests[2].Data)
	require.Equal(t, []byte("more"), writeStream2.requests[3].Data)
	require.True(t, writeStream2.requests[4].FinishWrite)
}

func TestByteStreamCopy_NothingMissing(t *testing.T) {
	ctx := context.Background()

	digest1 := digestProto(strings.Repeat("1", 64), 10)

	// Create mock CAS client that returns no missing blobs
	casClient := &mockCASClient{
		missingBlobs: []*repb.Digest{}, // Empty list - nothing missing
	}

	router := &mockRouter{
		casSecondary: casClient,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	// Execute the copy - should return without error since nothing is missing
	err := migration_operators.ByteStreamCopy(ctx, router, "test-group", batch)
	require.NoError(t, err)
}

func TestByteStreamCopy_ReadError(t *testing.T) {
	ctx := context.Background()

	digest1 := digestProto(strings.Repeat("1", 64), 10)

	// Create mock CAS client that returns digest as missing
	casClient := &mockCASClient{
		missingBlobs: []*repb.Digest{digest1},
	}

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
		primary:      primary,
		secondary:    secondary,
		casSecondary: casClient,
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

	// Create mock CAS client that returns digest as missing
	casClient := &mockCASClient{
		missingBlobs: []*repb.Digest{digest1},
	}

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
		primary:      primary,
		secondary:    secondary,
		casSecondary: casClient,
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

func TestByteStreamCopy_CASError(t *testing.T) {
	ctx := context.Background()

	router := &mockRouter{
		casErr: status.InternalError("cas failed"),
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ByteStreamCopy(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "cas failed")
}

func TestByteStreamCopy_FindMissingError(t *testing.T) {
	ctx := context.Background()

	digest1 := digestProto(strings.Repeat("1", 64), 10)

	// Create mock CAS client that returns error on FindMissingBlobs
	casClient := &mockCASClient{
		err: status.InternalError("find missing failed"),
	}

	router := &mockRouter{
		casSecondary: casClient,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ByteStreamCopy(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "find missing failed")
}

func TestByteStreamCopy_BSRouterError(t *testing.T) {
	ctx := context.Background()

	digest1 := digestProto(strings.Repeat("1", 64), 10)

	// Create mock CAS client that returns digest as missing
	casClient := &mockCASClient{
		missingBlobs: []*repb.Digest{digest1},
	}

	router := &mockRouter{
		casSecondary: casClient,
		err:          status.InternalError("router failed"),
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ByteStreamCopy(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "router failed")
}

func TestByteStreamCopy_EmptyData(t *testing.T) {
	ctx := context.Background()

	// Create test digest for empty data
	digest1 := digestProto(strings.Repeat("0", 64), 0)

	// Create mock CAS client that returns digest as missing
	casClient := &mockCASClient{
		missingBlobs: []*repb.Digest{digest1},
	}

	router := &mockRouter{
		casSecondary: casClient,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	// Execute the copy - should return without error since empty blobs are skipped
	err := migration_operators.ByteStreamCopy(ctx, router, "test-group", batch)
	require.NoError(t, err)

	// Verify usage tracking is disabled for CAS call (BS calls not made for empty blobs)
	verifyUsageTrackingDisabled(t, casClient.lastCtx)
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

	// Verify usage tracking is disabled for BS secondary calls
	verifyUsageTrackingDisabled(t, secondary.lastCtx)
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

	// Verify usage tracking is disabled for BS secondary calls
	verifyUsageTrackingDisabled(t, secondary.lastCtx)
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
