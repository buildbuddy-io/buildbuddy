package migration_operators_test

import (
	"context"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/enterprise/server/batch_operator"
	"github.com/buildbuddy-io/buildbuddy/enterprise/server/routing/migration_operators"
	"github.com/buildbuddy-io/buildbuddy/server/interfaces"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/flagutil"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/status"
	"github.com/buildbuddy-io/buildbuddy/server/util/usageutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	ropb "github.com/buildbuddy-io/buildbuddy/proto/routing"
	bspb "google.golang.org/genproto/googleapis/bytestream"
)

type mockBSReadClient struct {
	responses []*bspb.ReadResponse
	idx       int
	err       error
}

func (m *mockBSReadClient) Recv() (*bspb.ReadResponse, error) {
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

func (m *mockBSReadClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockBSReadClient) Trailer() metadata.MD {
	return nil
}

func (m *mockBSReadClient) CloseSend() error {
	return nil
}

func (m *mockBSReadClient) Context() context.Context {
	return context.Background()
}

func (m *mockBSReadClient) SendMsg(interface{}) error {
	return nil
}

func (m *mockBSReadClient) RecvMsg(interface{}) error {
	return nil
}

type mockBSWriteClient struct {
	requests       []*bspb.WriteRequest
	response       *bspb.WriteResponse
	err            error
	closeErr       error
	expectedOffset int64
}

func (m *mockBSWriteClient) Send(req *bspb.WriteRequest) error {
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

func (m *mockBSWriteClient) CloseAndRecv() (*bspb.WriteResponse, error) {
	if m.closeErr != nil {
		return nil, m.closeErr
	}
	return m.response, nil
}

func (m *mockBSWriteClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockBSWriteClient) Trailer() metadata.MD {
	return nil
}

func (m *mockBSWriteClient) CloseSend() error {
	return nil
}

func (m *mockBSWriteClient) Context() context.Context {
	return context.Background()
}

func (m *mockBSWriteClient) SendMsg(interface{}) error {
	return nil
}

func (m *mockBSWriteClient) RecvMsg(interface{}) error {
	return nil
}

type mockBSClient struct {
	readStreams  []*mockBSReadClient
	writeStreams []*mockBSWriteClient
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

type mockGetTreeClient struct {
	responses []*repb.GetTreeResponse
	idx       int
	err       error
}

func (m *mockGetTreeClient) Recv() (*repb.GetTreeResponse, error) {
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

func (m *mockGetTreeClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockGetTreeClient) Trailer() metadata.MD {
	return nil
}

func (m *mockGetTreeClient) CloseSend() error {
	return nil
}

func (m *mockGetTreeClient) Context() context.Context {
	return context.Background()
}

func (m *mockGetTreeClient) SendMsg(interface{}) error {
	return nil
}

func (m *mockGetTreeClient) RecvMsg(interface{}) error {
	return nil
}

type mockBatchOperator struct {
	enqueueSuccess bool
	enqueueCalls   []mockBatchOperatorCall
}

type mockBatchOperatorCall struct {
	instanceName   string
	digests        []*repb.Digest
	digestFunction repb.DigestFunction_Value
}

func (m *mockBatchOperator) Enqueue(ctx context.Context, instanceName string, digests []*repb.Digest, digestFunction repb.DigestFunction_Value) bool {
	m.enqueueCalls = append(m.enqueueCalls, mockBatchOperatorCall{
		instanceName:   instanceName,
		digests:        digests,
		digestFunction: digestFunction,
	})
	return m.enqueueSuccess
}

func (m *mockBatchOperator) EnqueueByResourceName(ctx context.Context, rn *digest.CASResourceName) bool {
	panic("unimplemented")
}

type mockCASClient struct {
	missingBlobs           []*repb.Digest
	err                    error
	lastCtx                context.Context               // Capture context for verification
	forceWrongSizeData     bool                          // Force generation of wrong-sized data for testing
	lastBatchReadRequest   *repb.BatchReadBlobsRequest   // Capture last BatchReadBlobs request for verification
	lastBatchUpdateRequest *repb.BatchUpdateBlobsRequest // Capture last BatchUpdateBlobs request for verification
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
	m.lastCtx = ctx
	m.lastBatchUpdateRequest = req
	if m.err != nil {
		return nil, m.err
	}

	// Create responses for each request
	responses := make([]*repb.BatchUpdateBlobsResponse_Response, len(req.Requests))
	for i, updateReq := range req.Requests {
		responses[i] = &repb.BatchUpdateBlobsResponse_Response{
			Digest: updateReq.Digest,
		}
	}

	return &repb.BatchUpdateBlobsResponse{
		Responses: responses,
	}, nil
}

func (m *mockCASClient) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest, opts ...grpc.CallOption) (*repb.BatchReadBlobsResponse, error) {
	m.lastCtx = ctx
	m.lastBatchReadRequest = req
	if m.err != nil {
		return nil, m.err
	}

	// Create responses for each digest
	responses := make([]*repb.BatchReadBlobsResponse_Response, len(req.Digests))
	for i, digest := range req.Digests {
		var data []byte

		// Check if this looks like a tree digest (starts with 't' characters)
		if strings.HasPrefix(digest.Hash, "tttt") {
			// Create a proper Tree proto for tree digests
			tree := &repb.Tree{
				Root: &repb.Directory{
					Files: []*repb.FileNode{
						{Name: "tree_file1.txt", Digest: digestProto(strings.Repeat("a", 64), 100)},
						{Name: "tree_file2.txt", Digest: digestProto(strings.Repeat("b", 64), 200)},
					},
				},
				Children: []*repb.Directory{
					{
						Files: []*repb.FileNode{
							{Name: "child_file.txt", Digest: digestProto(strings.Repeat("c", 64), 300)},
						},
					},
				},
			}
			var err error
			data, err = proto.Marshal(tree)
			if err != nil {
				return nil, err
			}
		} else {
			// Generate test data based on digest hash for non-tree digests
			dataSize := digest.SizeBytes
			if m.forceWrongSizeData {
				dataSize = 1 // Always return 1 byte regardless of expected size
			}
			data = make([]byte, dataSize)
			for j := range data {
				data[j] = byte(digest.Hash[0])
			}
		}

		responses[i] = &repb.BatchReadBlobsResponse_Response{
			Digest:     digest,
			Data:       data,
			Compressor: repb.Compressor_IDENTITY,
		}
	}

	return &repb.BatchReadBlobsResponse{
		Responses: responses,
	}, nil
}

func (m *mockCASClient) GetTree(ctx context.Context, req *repb.GetTreeRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[repb.GetTreeResponse], error) {
	m.lastCtx = ctx
	if m.err != nil {
		// Return a stream that will fail on Recv() to simulate the real behavior
		return &mockGetTreeClient{
			err: m.err,
		}, nil
	}
	return &mockGetTreeClient{
		responses: []*repb.GetTreeResponse{
			{
				Directories: []*repb.Directory{
					{
						Files: []*repb.FileNode{
							{Name: "file1.txt", Digest: digestProto(strings.Repeat("a", 64), 100)},
							{Name: "file2.txt", Digest: digestProto(strings.Repeat("b", 64), 200)},
						},
						Directories: []*repb.DirectoryNode{
							{Name: "subdir", Digest: digestProto(strings.Repeat("c", 64), 300)},
						},
					},
				},
			},
		},
	}, nil
}

func (m *mockCASClient) SpliceBlob(ctx context.Context, req *repb.SpliceBlobRequest, opts ...grpc.CallOption) (*repb.SpliceBlobResponse, error) {
	return nil, status.UnimplementedError("SpliceBlob not implemented")
}

func (m *mockCASClient) SplitBlob(ctx context.Context, req *repb.SplitBlobRequest, opts ...grpc.CallOption) (*repb.SplitBlobResponse, error) {
	return nil, status.UnimplementedError("SplitBlob not implemented")
}

type mockACClient struct {
	actionResults           map[string]*repb.ActionResult // key is action digest hash
	err                     error
	lastCtx                 context.Context                 // Capture context for verification
	lastGetRequest          *repb.GetActionResultRequest    // Capture last GetActionResult request
	lastUpdateRequest       *repb.UpdateActionResultRequest // Capture last UpdateActionResult request
	getActionResultError    error                           // Separate error for GetActionResult calls
	updateActionResultError error                           // Separate error for UpdateActionResult calls
}

func (m *mockACClient) GetActionResult(ctx context.Context, req *repb.GetActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	m.lastCtx = ctx
	m.lastGetRequest = req
	if m.getActionResultError != nil {
		return nil, m.getActionResultError
	}
	if m.err != nil {
		return nil, m.err
	}

	result, exists := m.actionResults[req.ActionDigest.Hash]
	if !exists {
		return nil, status.NotFoundError("ActionResult not found")
	}
	return result, nil
}

func (m *mockACClient) UpdateActionResult(ctx context.Context, req *repb.UpdateActionResultRequest, opts ...grpc.CallOption) (*repb.ActionResult, error) {
	m.lastCtx = ctx
	m.lastUpdateRequest = req
	if m.updateActionResultError != nil {
		return nil, m.updateActionResultError
	}
	if m.err != nil {
		return nil, m.err
	}

	// Store the action result
	if m.actionResults == nil {
		m.actionResults = make(map[string]*repb.ActionResult)
	}
	m.actionResults[req.ActionDigest.Hash] = req.ActionResult
	return req.ActionResult, nil
}

type mockRouter struct {
	primary      bspb.ByteStreamClient
	secondary    bspb.ByteStreamClient
	casPrimary   repb.ContentAddressableStorageClient
	casSecondary repb.ContentAddressableStorageClient
	acPrimary    repb.ActionCacheClient
	acSecondary  repb.ActionCacheClient
	err          error
	casErr       error
	acErr        error
}

func (m *mockRouter) GetCacheRoutingConfig(ctx context.Context) (*ropb.CacheRoutingConfig, error) {
	return nil, status.UnimplementedError("GetCacheRoutingConfig not implemented")
}

func (m *mockRouter) GetCASClients(ctx context.Context) (repb.ContentAddressableStorageClient, repb.ContentAddressableStorageClient, error) {
	if m.casErr != nil {
		return nil, nil, m.casErr
	}
	return m.casPrimary, m.casSecondary, nil
}

func (m *mockRouter) GetACClients(ctx context.Context) (repb.ActionCacheClient, repb.ActionCacheClient, error) {
	if m.acErr != nil {
		return nil, nil, m.acErr
	}
	return m.acPrimary, m.acSecondary, nil
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
		t.Error("Failed to read headers from context")
	}
	skipValues := md.Get(usageutil.SkipUsageTrackingHeaderName)
	require.Len(t, skipValues, 1, "Expected usage tracking to be disabled, but no header was set.")
	require.Equal(t, "1", skipValues[0], "Unexpected skip-tracking header value.")
}

func TestMain(m *testing.M) {
	// Set values for usage tracking disablement so that it behaves properly.
	flagutil.SetValueForFlagName("grpc_client_origin_header", "internal", nil, false)
	usageutil.SetServerName(interfaces.ClientIdentityCacheProxy)

	code := m.Run()

	os.Exit(code)
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
	readStream1 := &mockBSReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	}
	readStream2 := &mockBSReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("test")},
			{Data: []byte("data")},
			{Data: []byte("more")},
		},
	}

	// Create mock write streams
	writeStream1 := &mockBSWriteClient{
		response: &bspb.WriteResponse{CommittedSize: 10},
	}
	writeStream2 := &mockBSWriteClient{
		response: &bspb.WriteResponse{CommittedSize: 12},
	}

	// Setup mock clients
	primary := &mockBSClient{
		readStreams: []*mockBSReadClient{readStream1, readStream2},
	}
	secondary := &mockBSClient{
		writeStreams: []*mockBSWriteClient{writeStream1, writeStream2},
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
	readStream1 := &mockBSReadClient{
		err: status.InternalError("read failed"),
	}

	// Create mock write stream (needed even though we'll fail on read)
	writeStream1 := &mockBSWriteClient{
		response: &bspb.WriteResponse{CommittedSize: 10},
	}

	primary := &mockBSClient{
		readStreams: []*mockBSReadClient{readStream1},
	}
	secondary := &mockBSClient{
		writeStreams: []*mockBSWriteClient{writeStream1},
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

	readStream1 := &mockBSReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
		},
	}

	// Create mock write stream with error
	writeStream1 := &mockBSWriteClient{
		err: status.InternalError("write failed"),
	}

	primary := &mockBSClient{
		readStreams: []*mockBSReadClient{readStream1},
	}
	secondary := &mockBSClient{
		writeStreams: []*mockBSWriteClient{writeStream1},
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
	readStream1 := &mockBSReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	}
	readStream2 := &mockBSReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("test")},
			{Data: []byte("datamore")},
		},
	}

	secondary := &mockBSClient{
		readStreams: []*mockBSReadClient{readStream1, readStream2},
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
	readStream1 := &mockBSReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	}

	secondary := &mockBSClient{
		readStreams: []*mockBSReadClient{readStream1},
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
	readStream1 := &mockBSReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	}

	secondary := &mockBSClient{
		readStreams: []*mockBSReadClient{readStream1},
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
	readStream1 := &mockBSReadClient{
		err: status.InternalError("read failed"),
	}

	secondary := &mockBSClient{
		readStreams: []*mockBSReadClient{readStream1},
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
	readStream1 := &mockBSReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("hello")},
			{Data: []byte("world")},
		},
	}
	readStream2 := &mockBSReadClient{
		responses: []*bspb.ReadResponse{
			{Data: []byte("test")},
			{Data: []byte("datamore")},
		},
	}

	secondary := &mockBSClient{
		readStreams: []*mockBSReadClient{readStream1, readStream2},
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

func TestCASBatchCopy_Success(t *testing.T) {
	ctx := context.Background()

	// Create test digests
	digest1 := digestProto(strings.Repeat("1", 64), 100)
	digest2 := digestProto(strings.Repeat("2", 64), 200)

	// Create mock CAS clients
	casSecondary := &mockCASClient{
		missingBlobs: []*repb.Digest{digest1, digest2}, // Both are missing
	}
	casPrimary := &mockCASClient{}

	router := &mockRouter{
		casPrimary:   casPrimary,
		casSecondary: casSecondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1, digest2},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.CASBatchCopy(ctx, router, "test-group", batch)
	require.NoError(t, err)

	// Verify usage tracking is disabled
	verifyUsageTrackingDisabled(t, casSecondary.lastCtx)
	verifyUsageTrackingDisabled(t, casPrimary.lastCtx)

	// Verify that the appropriate digests were sent in the batchupdateblobs call to the secondary
	require.NotNil(t, casSecondary.lastBatchUpdateRequest)
	require.Equal(t, "test-instance", casSecondary.lastBatchUpdateRequest.InstanceName)
	require.Equal(t, repb.DigestFunction_SHA256, casSecondary.lastBatchUpdateRequest.DigestFunction)
	require.Len(t, casSecondary.lastBatchUpdateRequest.Requests, 2)

	// Extract digests from the update requests
	updateDigests := make([]*repb.Digest, len(casSecondary.lastBatchUpdateRequest.Requests))
	for i, req := range casSecondary.lastBatchUpdateRequest.Requests {
		updateDigests[i] = req.Digest
	}
	require.Contains(t, updateDigests, digest1)
	require.Contains(t, updateDigests, digest2)
}

func TestCASBatchCopy_NothingMissing(t *testing.T) {
	ctx := context.Background()

	digest1 := digestProto(strings.Repeat("1", 64), 100)

	casSecondary := &mockCASClient{
		missingBlobs: []*repb.Digest{}, // Nothing missing
	}

	router := &mockRouter{
		casSecondary: casSecondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.CASBatchCopy(ctx, router, "test-group", batch)
	// The mocks are set up such that no errors here indicates no batches were flushed.
	require.NoError(t, err)
}

func TestCASBatchCopy_FindMissingError(t *testing.T) {
	ctx := context.Background()

	casSecondary := &mockCASClient{
		err: status.InternalError("find missing failed"),
	}

	router := &mockRouter{
		casSecondary: casSecondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.CASBatchCopy(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "find missing failed")
}

func TestCASBatchCopy_GetCASClientsError(t *testing.T) {
	ctx := context.Background()

	router := &mockRouter{
		casErr: status.InternalError("get cas clients failed"),
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.CASBatchCopy(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get cas clients failed")
}

func TestCASBatchReadAndVerify_Success(t *testing.T) {
	ctx := context.Background()

	// Create test digests
	digest1 := digestProto(strings.Repeat("1", 64), 1) // Size 1 byte
	digest2 := digestProto(strings.Repeat("2", 64), 1) // Size 1 byte

	casSecondary := &mockCASClient{}

	router := &mockRouter{
		casSecondary: casSecondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1, digest2},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.CASBatchReadAndVerify(ctx, router, true, "test-group", batch)
	require.NoError(t, err)

	// Verify usage tracking is disabled
	verifyUsageTrackingDisabled(t, casSecondary.lastCtx)

	// Verify that the appropriate digests were read in the batchreadblobs call to the secondary
	require.NotNil(t, casSecondary.lastBatchReadRequest)
	require.Equal(t, "test-instance", casSecondary.lastBatchReadRequest.InstanceName)
	require.Equal(t, repb.DigestFunction_SHA256, casSecondary.lastBatchReadRequest.DigestFunction)
	require.Len(t, casSecondary.lastBatchReadRequest.Digests, 2)
	require.Contains(t, casSecondary.lastBatchReadRequest.Digests, digest1)
	require.Contains(t, casSecondary.lastBatchReadRequest.Digests, digest2)
}

func TestCASBatchReadAndVerify_SizeMismatch(t *testing.T) {
	ctx := context.Background()

	// Create test digest with wrong size - expecting 5 bytes but mock returns 1
	digest1 := digestProto(strings.Repeat("1", 64), 5)

	casSecondary := &mockCASClient{
		forceWrongSizeData: true, // Force wrong size data
	}

	router := &mockRouter{
		casSecondary: casSecondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.CASBatchReadAndVerify(ctx, router, true, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Size mismatch")
}

func TestCASBatchReadAndVerify_GetCASClientsError(t *testing.T) {
	ctx := context.Background()

	router := &mockRouter{
		casErr: status.InternalError("get cas clients failed"),
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.CASBatchReadAndVerify(ctx, router, true, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get cas clients failed")
}

func TestCASBatchReadAndVerify_BatchReadError(t *testing.T) {
	ctx := context.Background()

	digest1 := digestProto(strings.Repeat("1", 64), 100)

	casSecondary := &mockCASClient{
		err: status.InternalError("batch read failed"),
	}

	router := &mockRouter{
		casSecondary: casSecondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{digest1},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.CASBatchReadAndVerify(ctx, router, true, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "batch read failed")
}

func TestRoutedCopy_Success(t *testing.T) {
	ctx := context.Background()

	// Create digests of different sizes to test routing
	smallDigest := digestProto(strings.Repeat("1", 64), 1000)        // 1KB - should go to CAS
	largeDigest := digestProto(strings.Repeat("2", 64), 4*1024*1024) // 4MB - should go to ByteStream

	casCopy := &mockBatchOperator{enqueueSuccess: true}
	bytestreamCopy := &mockBatchOperator{enqueueSuccess: true}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{smallDigest, largeDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.RoutedCopy(ctx, "test-group", casCopy, bytestreamCopy, batch)
	require.NoError(t, err)

	// Verify CAS operator was called with small digest
	require.Len(t, casCopy.enqueueCalls, 1)
	require.Equal(t, "test-instance", casCopy.enqueueCalls[0].instanceName)
	require.Equal(t, repb.DigestFunction_SHA256, casCopy.enqueueCalls[0].digestFunction)
	require.Len(t, casCopy.enqueueCalls[0].digests, 1)
	require.Equal(t, smallDigest, casCopy.enqueueCalls[0].digests[0])

	// Verify ByteStream operator was called with large digest
	require.Len(t, bytestreamCopy.enqueueCalls, 1)
	require.Equal(t, "test-instance", bytestreamCopy.enqueueCalls[0].instanceName)
	require.Equal(t, repb.DigestFunction_SHA256, bytestreamCopy.enqueueCalls[0].digestFunction)
	require.Len(t, bytestreamCopy.enqueueCalls[0].digests, 1)
	require.Equal(t, largeDigest, bytestreamCopy.enqueueCalls[0].digests[0])
}

func TestRoutedCopy_OnlySmallDigests(t *testing.T) {
	ctx := context.Background()

	smallDigest1 := digestProto(strings.Repeat("1", 64), 1000)
	smallDigest2 := digestProto(strings.Repeat("2", 64), 2000)

	casCopy := &mockBatchOperator{enqueueSuccess: true}
	bytestreamCopy := &mockBatchOperator{enqueueSuccess: true}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{smallDigest1, smallDigest2},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.RoutedCopy(ctx, "test-group", casCopy, bytestreamCopy, batch)
	require.NoError(t, err)

	// Verify CAS operator was called with both digests
	require.Len(t, casCopy.enqueueCalls, 1)
	require.Len(t, casCopy.enqueueCalls[0].digests, 2)
	require.Equal(t, smallDigest1, casCopy.enqueueCalls[0].digests[0])
	require.Equal(t, smallDigest2, casCopy.enqueueCalls[0].digests[1])

	// Verify ByteStream operator was not called
	require.Len(t, bytestreamCopy.enqueueCalls, 0)
}

func TestRoutedCopy_OnlyLargeDigests(t *testing.T) {
	ctx := context.Background()

	largeDigest1 := digestProto(strings.Repeat("1", 64), 4*1024*1024)
	largeDigest2 := digestProto(strings.Repeat("2", 64), 5*1024*1024)

	casCopy := &mockBatchOperator{enqueueSuccess: true}
	bytestreamCopy := &mockBatchOperator{enqueueSuccess: true}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{largeDigest1, largeDigest2},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.RoutedCopy(ctx, "test-group", casCopy, bytestreamCopy, batch)
	require.NoError(t, err)

	// Verify CAS operator was not called
	require.Len(t, casCopy.enqueueCalls, 0)

	// Verify ByteStream operator was called with both digests
	require.Len(t, bytestreamCopy.enqueueCalls, 1)
	require.Len(t, bytestreamCopy.enqueueCalls[0].digests, 2)
	require.Equal(t, largeDigest1, bytestreamCopy.enqueueCalls[0].digests[0])
	require.Equal(t, largeDigest2, bytestreamCopy.enqueueCalls[0].digests[1])
}

func TestRoutedCopy_CASEnqueueFailed(t *testing.T) {
	ctx := context.Background()

	smallDigest := digestProto(strings.Repeat("1", 64), 1000)

	casCopy := &mockBatchOperator{enqueueSuccess: false} // Fail CAS enqueue
	bytestreamCopy := &mockBatchOperator{enqueueSuccess: true}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{smallDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.RoutedCopy(ctx, "test-group", casCopy, bytestreamCopy, batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Failed to enqueue CAS sync")
}

func TestRoutedCopy_ByteStreamEnqueueFailed(t *testing.T) {
	ctx := context.Background()

	largeDigest := digestProto(strings.Repeat("1", 64), 4*1024*1024)

	casCopy := &mockBatchOperator{enqueueSuccess: true}
	bytestreamCopy := &mockBatchOperator{enqueueSuccess: false} // Fail ByteStream enqueue

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{largeDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.RoutedCopy(ctx, "test-group", casCopy, bytestreamCopy, batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "Failed to enqueue bytestream sync")
}

func TestGetTreeMirrorOperator_Success(t *testing.T) {
	ctx := context.Background()

	rootDigest := digestProto(strings.Repeat("1", 64), 100)

	casPrimary := &mockCASClient{}
	copyOperator := &mockBatchOperator{enqueueSuccess: true}

	router := &mockRouter{
		casPrimary: casPrimary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{rootDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.GetTreeMirrorOperator(ctx, router, copyOperator, "test-group", batch)
	require.NoError(t, err)

	// Verify usage tracking is disabled
	verifyUsageTrackingDisabled(t, casPrimary.lastCtx)

	// Verify copy operator was called with discovered files
	require.Len(t, copyOperator.enqueueCalls, 1)
	require.Equal(t, "test-instance", copyOperator.enqueueCalls[0].instanceName)
	require.Equal(t, repb.DigestFunction_SHA256, copyOperator.enqueueCalls[0].digestFunction)

	// Should have discovered the files and directories from the mock GetTree response:
	// file1.txt, file2.txt, subdir, plus the root digest itself
	require.Len(t, copyOperator.enqueueCalls[0].digests, 4)

	// Validate the specific digests that should have been discovered
	foundDigests := copyOperator.enqueueCalls[0].digests

	// Create the expected digests from the mock GetTree response
	expectedFile1Digest := digestProto(strings.Repeat("a", 64), 100)  // file1.txt
	expectedFile2Digest := digestProto(strings.Repeat("b", 64), 200)  // file2.txt
	expectedSubdirDigest := digestProto(strings.Repeat("c", 64), 300) // subdir

	// Verify all expected digests are present (order may vary due to map iteration)
	require.Contains(t, foundDigests, expectedFile1Digest)
	require.Contains(t, foundDigests, expectedFile2Digest)
	require.Contains(t, foundDigests, expectedSubdirDigest)
	require.Contains(t, foundDigests, rootDigest) // The root digest should also be included
}

func TestGetTreeMirrorOperator_GetCASClientsError(t *testing.T) {
	ctx := context.Background()

	router := &mockRouter{
		casErr: status.InternalError("get cas clients failed"),
	}

	copyOperator := &mockBatchOperator{enqueueSuccess: true}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.GetTreeMirrorOperator(ctx, router, copyOperator, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get cas clients failed")
}

func TestGetTreeMirrorOperator_GetTreeError(t *testing.T) {
	ctx := context.Background()

	rootDigest := digestProto(strings.Repeat("1", 64), 100)

	casPrimary := &mockCASClient{
		err: status.InternalError("get tree failed"),
	}
	copyOperator := &mockBatchOperator{enqueueSuccess: true}

	router := &mockRouter{
		casPrimary: casPrimary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{rootDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.GetTreeMirrorOperator(ctx, router, copyOperator, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get tree failed")
}

func TestACReadAndVerify_Success(t *testing.T) {
	ctx := context.Background()

	// Create test action digests
	actionDigest1 := digestProto(strings.Repeat("1", 64), 100)
	actionDigest2 := digestProto(strings.Repeat("2", 64), 200)

	// Create mock action results
	actionResult1 := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{Path: "output1.txt", Digest: digestProto(strings.Repeat("a", 64), 50)},
		},
	}
	actionResult2 := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{Path: "output2.txt", Digest: digestProto(strings.Repeat("b", 64), 75)},
		},
	}

	acSecondary := &mockACClient{
		actionResults: map[string]*repb.ActionResult{
			actionDigest1.Hash: actionResult1,
			actionDigest2.Hash: actionResult2,
		},
	}

	router := &mockRouter{
		acSecondary: acSecondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{actionDigest1, actionDigest2},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACReadAndVerify(ctx, router, "test-group", batch)
	require.NoError(t, err)

	// Verify usage tracking is disabled for AC secondary calls
	verifyUsageTrackingDisabled(t, acSecondary.lastCtx)

	// Verify that GetActionResult was called for each digest
	require.NotNil(t, acSecondary.lastGetRequest)
	// Last request should be for the second digest
	require.Equal(t, actionDigest2, acSecondary.lastGetRequest.ActionDigest)
	require.Equal(t, "test-instance", acSecondary.lastGetRequest.InstanceName)
	require.Equal(t, repb.DigestFunction_SHA256, acSecondary.lastGetRequest.DigestFunction)
}

func TestACReadAndVerify_NotFound(t *testing.T) {
	ctx := context.Background()

	actionDigest := digestProto(strings.Repeat("1", 64), 100)

	acSecondary := &mockACClient{
		actionResults: map[string]*repb.ActionResult{}, // Empty - no action results
	}

	router := &mockRouter{
		acSecondary: acSecondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{actionDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACReadAndVerify(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ActionResult not found")
}

func TestACReadAndVerify_GetACClientsError(t *testing.T) {
	ctx := context.Background()

	router := &mockRouter{
		acErr: status.InternalError("get ac clients failed"),
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACReadAndVerify(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get ac clients failed")
}

func TestACReadAndVerify_GetActionResultError(t *testing.T) {
	ctx := context.Background()

	actionDigest := digestProto(strings.Repeat("1", 64), 100)

	acSecondary := &mockACClient{
		getActionResultError: status.InternalError("get action result failed"),
	}

	router := &mockRouter{
		acSecondary: acSecondary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{actionDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACReadAndVerify(ctx, router, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get action result failed")
}

func TestACCopy_Success_SameDigests(t *testing.T) {
	ctx := context.Background()

	actionDigest := digestProto(strings.Repeat("1", 64), 100)
	actionResultDigest := digestProto(strings.Repeat("r", 64), 150)

	// Create action result with output files
	actionResult := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{Path: "output1.txt", Digest: digestProto(strings.Repeat("a", 64), 50)},
			{Path: "output2.txt", Digest: digestProto(strings.Repeat("b", 64), 75)},
		},
		ActionResultDigest: actionResultDigest,
	}

	acPrimary := &mockACClient{
		actionResults: map[string]*repb.ActionResult{
			actionDigest.Hash: actionResult,
		},
	}
	acSecondary := &mockACClient{
		actionResults: map[string]*repb.ActionResult{
			actionDigest.Hash: actionResult, // Same result exists in secondary
		},
	}

	casPrimary := &mockCASClient{}
	copyOperator := &mockBatchOperator{enqueueSuccess: true}

	router := &mockRouter{
		acPrimary:   acPrimary,
		acSecondary: acSecondary,
		casPrimary:  casPrimary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{actionDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACCopy(ctx, router, copyOperator, "test-group", batch)
	require.NoError(t, err)

	// Verify usage tracking is disabled
	verifyUsageTrackingDisabled(t, acPrimary.lastCtx)
	verifyUsageTrackingDisabled(t, acSecondary.lastCtx)

	// Since the digests are the same, no copy should be performed
	require.Len(t, copyOperator.enqueueCalls, 0)
	require.Nil(t, acSecondary.lastUpdateRequest) // No update should have been called
}

// When the secondary cache has a different ActionResult for the specified
// action digest, we should sync whatever files are needed and overwrite the
// old ActionResult.
func TestACCopy_Success_DifferentDigests(t *testing.T) {
	ctx := context.Background()

	actionDigest := digestProto(strings.Repeat("1", 64), 100)
	primaryResultDigest := digestProto(strings.Repeat("p", 64), 150)
	secondaryResultDigest := digestProto(strings.Repeat("s", 64), 150)

	// Create action results with different digests
	primaryActionResult := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{Path: "output1.txt", Digest: digestProto(strings.Repeat("a", 64), 50)},
			{Path: "output2.txt", Digest: digestProto(strings.Repeat("b", 64), 75)},
		},
		ActionResultDigest: primaryResultDigest,
	}

	secondaryActionResult := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{Path: "old_output.txt", Digest: digestProto(strings.Repeat("x", 64), 25)},
		},
		ActionResultDigest: secondaryResultDigest,
	}

	acPrimary := &mockACClient{
		actionResults: map[string]*repb.ActionResult{
			actionDigest.Hash: primaryActionResult,
		},
	}
	acSecondary := &mockACClient{
		actionResults: map[string]*repb.ActionResult{
			actionDigest.Hash: secondaryActionResult, // Different result in secondary
		},
	}

	casPrimary := &mockCASClient{}
	copyOperator := &mockBatchOperator{enqueueSuccess: true}

	router := &mockRouter{
		acPrimary:   acPrimary,
		acSecondary: acSecondary,
		casPrimary:  casPrimary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{actionDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACCopy(ctx, router, copyOperator, "test-group", batch)
	require.NoError(t, err)

	// Verify usage tracking is disabled
	verifyUsageTrackingDisabled(t, acPrimary.lastCtx)
	verifyUsageTrackingDisabled(t, acSecondary.lastCtx)
	// CAS client not called for this test since there are no output directories

	// Copy should be performed for output files
	require.Len(t, copyOperator.enqueueCalls, 1)
	require.Equal(t, "test-instance", copyOperator.enqueueCalls[0].instanceName)
	require.Equal(t, repb.DigestFunction_SHA256, copyOperator.enqueueCalls[0].digestFunction)

	// Should copy the output file digests
	expectedDigests := []*repb.Digest{
		digestProto(strings.Repeat("a", 64), 50),
		digestProto(strings.Repeat("b", 64), 75),
	}
	require.Len(t, copyOperator.enqueueCalls[0].digests, 2)
	require.Contains(t, copyOperator.enqueueCalls[0].digests, expectedDigests[0])
	require.Contains(t, copyOperator.enqueueCalls[0].digests, expectedDigests[1])

	// UpdateActionResult should be called
	require.NotNil(t, acSecondary.lastUpdateRequest)
	require.Equal(t, actionDigest, acSecondary.lastUpdateRequest.ActionDigest)
	require.Equal(t, primaryActionResult, acSecondary.lastUpdateRequest.ActionResult)
}

func TestACCopy_Success_WithOutputDirectories(t *testing.T) {
	ctx := context.Background()

	actionDigest := digestProto(strings.Repeat("1", 64), 100)
	primaryResultDigest := digestProto(strings.Repeat("p", 64), 150)

	// Create action result with output directories
	primaryActionResult := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{Path: "output1.txt", Digest: digestProto(strings.Repeat("a", 64), 50)},
		},
		OutputDirectories: []*repb.OutputDirectory{
			{Path: "outdir", TreeDigest: digestProto(strings.Repeat("t", 64), 300)},
		},
		ActionResultDigest: primaryResultDigest,
	}

	acPrimary := &mockACClient{
		actionResults: map[string]*repb.ActionResult{
			actionDigest.Hash: primaryActionResult,
		},
	}
	acSecondary := &mockACClient{
		actionResults: map[string]*repb.ActionResult{}, // No result in secondary
	}

	casPrimary := &mockCASClient{}
	copyOperator := &mockBatchOperator{enqueueSuccess: true}

	router := &mockRouter{
		acPrimary:   acPrimary,
		acSecondary: acSecondary,
		casPrimary:  casPrimary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{actionDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACCopy(ctx, router, copyOperator, "test-group", batch)
	require.NoError(t, err)

	// Verify usage tracking is disabled
	verifyUsageTrackingDisabled(t, acPrimary.lastCtx)
	verifyUsageTrackingDisabled(t, acSecondary.lastCtx)
	verifyUsageTrackingDisabled(t, casPrimary.lastCtx)

	// Copy should be performed for output files (directories are handled by expanding the tree)
	require.Len(t, copyOperator.enqueueCalls, 1)

	// Should copy the output file digest and files from the tree
	foundDigests := copyOperator.enqueueCalls[0].digests
	expectedFileDigest := digestProto(strings.Repeat("a", 64), 50)
	require.Contains(t, foundDigests, expectedFileDigest)

	// Should also have files from the expanded tree (from mock BatchReadBlobs response for tree digest)
	expectedTreeFile1 := digestProto(strings.Repeat("a", 64), 100) // tree_file1.txt
	expectedTreeFile2 := digestProto(strings.Repeat("b", 64), 200) // tree_file2.txt
	expectedTreeFile3 := digestProto(strings.Repeat("c", 64), 300) // child_file.txt
	require.Contains(t, foundDigests, expectedTreeFile1)
	require.Contains(t, foundDigests, expectedTreeFile2)
	require.Contains(t, foundDigests, expectedTreeFile3)

	// UpdateActionResult should be called
	require.NotNil(t, acSecondary.lastUpdateRequest)
	require.Equal(t, actionDigest, acSecondary.lastUpdateRequest.ActionDigest)
	require.Equal(t, primaryActionResult, acSecondary.lastUpdateRequest.ActionResult)
}

func TestACCopy_GetACClientsError(t *testing.T) {
	ctx := context.Background()

	router := &mockRouter{
		acErr: status.InternalError("get ac clients failed"),
	}

	copyOperator := &mockBatchOperator{enqueueSuccess: true}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACCopy(ctx, router, copyOperator, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get ac clients failed")
}

func TestACCopy_GetCASClientsError(t *testing.T) {
	ctx := context.Background()

	acPrimary := &mockACClient{}
	acSecondary := &mockACClient{}

	router := &mockRouter{
		acPrimary:   acPrimary,
		acSecondary: acSecondary,
		casErr:      status.InternalError("get cas clients failed"),
	}

	copyOperator := &mockBatchOperator{enqueueSuccess: true}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACCopy(ctx, router, copyOperator, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "get cas clients failed")
}

func TestACCopy_PrimaryGetActionResultError(t *testing.T) {
	ctx := context.Background()

	actionDigest := digestProto(strings.Repeat("1", 64), 100)

	acPrimary := &mockACClient{
		getActionResultError: status.InternalError("primary get failed"),
	}
	acSecondary := &mockACClient{}

	casPrimary := &mockCASClient{}
	copyOperator := &mockBatchOperator{enqueueSuccess: true}

	router := &mockRouter{
		acPrimary:   acPrimary,
		acSecondary: acSecondary,
		casPrimary:  casPrimary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{actionDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACCopy(ctx, router, copyOperator, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "primary get failed")
}

func TestACCopy_UpdateActionResultError(t *testing.T) {
	ctx := context.Background()

	actionDigest := digestProto(strings.Repeat("1", 64), 100)
	primaryResultDigest := digestProto(strings.Repeat("p", 64), 150)
	secondaryResultDigest := digestProto(strings.Repeat("s", 64), 150)

	primaryActionResult := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{Path: "output1.txt", Digest: digestProto(strings.Repeat("a", 64), 50)},
		},
		ActionResultDigest: primaryResultDigest,
	}

	secondaryActionResult := &repb.ActionResult{
		OutputFiles: []*repb.OutputFile{
			{Path: "old_output.txt", Digest: digestProto(strings.Repeat("x", 64), 25)},
		},
		ActionResultDigest: secondaryResultDigest,
	}

	acPrimary := &mockACClient{
		actionResults: map[string]*repb.ActionResult{
			actionDigest.Hash: primaryActionResult,
		},
	}
	acSecondary := &mockACClient{
		actionResults: map[string]*repb.ActionResult{
			actionDigest.Hash: secondaryActionResult,
		},
		updateActionResultError: status.InternalError("update failed"),
	}

	casPrimary := &mockCASClient{}
	copyOperator := &mockBatchOperator{enqueueSuccess: true}

	router := &mockRouter{
		acPrimary:   acPrimary,
		acSecondary: acSecondary,
		casPrimary:  casPrimary,
	}

	batch := &batch_operator.DigestBatch{
		InstanceName:   "test-instance",
		Digests:        []*repb.Digest{actionDigest},
		DigestFunction: repb.DigestFunction_SHA256,
	}

	err := migration_operators.ACCopy(ctx, router, copyOperator, "test-group", batch)
	require.Error(t, err)
	require.Contains(t, err.Error(), "update failed")
}
