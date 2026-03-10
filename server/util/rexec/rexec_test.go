package rexec_test

import (
	"context"
	"io"
	"strings"
	"testing"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"

	"github.com/buildbuddy-io/buildbuddy/server/real_environment"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	rspb "github.com/buildbuddy-io/buildbuddy/proto/resource"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	gstatus "google.golang.org/grpc/status"
)

type fakeActionCacheClient struct {
	t                   *testing.T
	getActionResultFunc func(ctx context.Context, in *repb.GetActionResultRequest) (*repb.ActionResult, error)
}

func (f *fakeActionCacheClient) GetActionResult(ctx context.Context, in *repb.GetActionResultRequest, _ ...grpc.CallOption) (*repb.ActionResult, error) {
	return f.getActionResultFunc(ctx, in)
}

func (f *fakeActionCacheClient) UpdateActionResult(context.Context, *repb.UpdateActionResultRequest, ...grpc.CallOption) (*repb.ActionResult, error) {
	f.t.Helper()
	f.t.Fatal("unexpected call to UpdateActionResult")
	return nil, nil
}

type fakeExecutionClient struct {
	executeReq *repb.ExecuteRequest
}

func (f *fakeExecutionClient) Execute(_ context.Context, in *repb.ExecuteRequest, _ ...grpc.CallOption) (grpc.ServerStreamingClient[longrunningpb.Operation], error) {
	f.executeReq = proto.Clone(in).(*repb.ExecuteRequest)
	return nil, nil
}

func (f *fakeExecutionClient) WaitExecution(context.Context, *repb.WaitExecutionRequest, ...grpc.CallOption) (grpc.ServerStreamingClient[longrunningpb.Operation], error) {
	panic("unexpected call to WaitExecution")
}

func (f *fakeExecutionClient) PublishOperation(context.Context, ...grpc.CallOption) (grpc.ClientStreamingClient[longrunningpb.Operation, repb.PublishOperationResponse], error) {
	panic("unexpected call to PublishOperation")
}

type fakeByteStreamClient struct {
	data map[string][]byte
}

func (f *fakeByteStreamClient) QueryWriteStatus(context.Context, *bspb.QueryWriteStatusRequest, ...grpc.CallOption) (*bspb.QueryWriteStatusResponse, error) {
	panic("unexpected call to QueryWriteStatus")
}

func (f *fakeByteStreamClient) Read(ctx context.Context, in *bspb.ReadRequest, _ ...grpc.CallOption) (bspb.ByteStream_ReadClient, error) {
	data, ok := f.data[in.GetResourceName()]
	if !ok {
		return nil, gstatus.Error(codes.NotFound, "not found")
	}
	return &fakeByteStreamReadClient{ctx: ctx, data: data}, nil
}

func (f *fakeByteStreamClient) Write(context.Context, ...grpc.CallOption) (bspb.ByteStream_WriteClient, error) {
	panic("unexpected call to Write")
}

type fakeByteStreamReadClient struct {
	ctx  context.Context
	data []byte
	done bool
}

func (f *fakeByteStreamReadClient) CloseSend() error {
	return nil
}

func (f *fakeByteStreamReadClient) Context() context.Context {
	return f.ctx
}

func (f *fakeByteStreamReadClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (f *fakeByteStreamReadClient) Recv() (*bspb.ReadResponse, error) {
	if f.done {
		return nil, io.EOF
	}
	f.done = true
	return &bspb.ReadResponse{Data: f.data}, nil
}

func (f *fakeByteStreamReadClient) RecvMsg(any) error {
	return nil
}

func (f *fakeByteStreamReadClient) SendMsg(any) error {
	return nil
}

func (f *fakeByteStreamReadClient) Trailer() metadata.MD {
	return nil
}

func TestStart(t *testing.T) {
	for _, test := range []struct {
		name            string
		opts            []rexec.StartOption
		skipCacheLookup bool
	}{
		{
			name:            "skips cache lookup",
			opts:            nil,
			skipCacheLookup: true,
		},
		{
			name:            "allows cache lookup",
			opts:            []rexec.StartOption{rexec.WithSkipCacheLookup(false)},
			skipCacheLookup: false,
		},
		{
			name:            "explicitly skips cache lookup",
			opts:            []rexec.StartOption{rexec.WithSkipCacheLookup(true)},
			skipCacheLookup: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			env := real_environment.NewBatchEnv()
			client := &fakeExecutionClient{}
			env.SetRemoteExecutionClient(client)
			actionResourceName := &rspb.ResourceName{
				Digest:         &repb.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 1},
				InstanceName:   "instance",
				DigestFunction: repb.DigestFunction_SHA256,
			}

			stream, err := rexec.Start(context.Background(), env, actionResourceName, test.opts...)
			require.NoError(t, err)
			require.NotNil(t, stream)
			require.NotNil(t, client.executeReq)
			require.Equal(t, actionResourceName.GetInstanceName(), client.executeReq.GetInstanceName())
			require.True(t, proto.Equal(actionResourceName.GetDigest(), client.executeReq.GetActionDigest()))
			require.Equal(t, actionResourceName.GetDigestFunction(), client.executeReq.GetDigestFunction())
			require.Equal(t, test.skipCacheLookup, client.executeReq.GetSkipCacheLookup())
		})
	}
}

func TestNormalizeCommand(t *testing.T) {
	for _, test := range []struct {
		name     string
		command  *repb.Command
		expected *repb.Command
	}{
		{
			name:     "handles nil command",
			command:  nil,
			expected: nil,
		},
		{
			name:     "handles nil platform and env",
			command:  &repb.Command{},
			expected: &repb.Command{},
		},
		{
			name: "handles non-nil platform with nil properties",
			command: &repb.Command{
				Platform: &repb.Platform{},
			},
			expected: &repb.Command{
				Platform: &repb.Platform{},
			},
		},
		{
			name: "sorts env vars",
			command: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "B", Value: "2"},
					{Name: "A", Value: "1"},
				},
			},
			expected: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "A", Value: "1"},
					{Name: "B", Value: "2"},
				},
			},
		},
		{
			name: "dedupes env vars",
			command: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "A", Value: "1"},
					{Name: "A", Value: "2"},
				},
			},
			expected: &repb.Command{
				EnvironmentVariables: []*repb.Command_EnvironmentVariable{
					{Name: "A", Value: "2"},
				},
			},
		},
		{
			name: "sorts platform properties",
			command: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{Name: "B", Value: "2"},
						{Name: "A", Value: "1"},
					},
				},
			},
			expected: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{Name: "A", Value: "1"},
						{Name: "B", Value: "2"},
					},
				},
			},
		},
		{
			name: "dedupes platform properties",
			command: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{Name: "A", Value: "1"},
						{Name: "A", Value: "2"},
					},
				},
			},
			expected: &repb.Command{
				Platform: &repb.Platform{
					Properties: []*repb.Platform_Property{
						{Name: "A", Value: "2"},
					},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			cmd := proto.Clone(test.command).(*repb.Command)
			rexec.NormalizeCommand(cmd)
			require.Empty(t, cmp.Diff(test.expected, cmd, protocmp.Transform()))
		})
	}
}

func TestFindFirstAuxiliaryMetadata(t *testing.T) {
	mustMarshalAny := func(t *testing.T, msg proto.Message) *anypb.Any {
		a, err := anypb.New(msg)
		require.NoError(t, err)
		return a
	}

	t.Run("returns first matching entry when multiple exist", func(t *testing.T) {
		first := &repb.Platform{Properties: []*repb.Platform_Property{{Name: "first", Value: "1"}}}
		second := &repb.Platform{Properties: []*repb.Platform_Property{{Name: "second", Value: "2"}}}
		md := &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{
				mustMarshalAny(t, first),
				mustMarshalAny(t, second),
			},
		}

		result := &repb.Platform{}
		ok, err := rexec.FindFirstAuxiliaryMetadata(md, result)

		require.NoError(t, err)
		require.True(t, ok)
		require.Empty(t, cmp.Diff(first, result, protocmp.Transform()))
	})

	t.Run("returns false when no match found", func(t *testing.T) {
		md := &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{
				mustMarshalAny(t, &repb.Command{Arguments: []string{"echo"}}),
			},
		}

		result := &repb.Platform{}
		ok, err := rexec.FindFirstAuxiliaryMetadata(md, result)

		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("returns false for nil metadata", func(t *testing.T) {
		result := &repb.Platform{}
		ok, err := rexec.FindFirstAuxiliaryMetadata(nil, result)

		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("returns false for empty auxiliary metadata", func(t *testing.T) {
		md := &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{},
		}

		result := &repb.Platform{}
		ok, err := rexec.FindFirstAuxiliaryMetadata(md, result)

		require.NoError(t, err)
		require.False(t, ok)
	})

	t.Run("returns error on unmarshal failure", func(t *testing.T) {
		md := &repb.ExecutedActionMetadata{
			AuxiliaryMetadata: []*anypb.Any{
				{
					TypeUrl: "type.googleapis.com/build.bazel.remote.execution.v2.Platform",
					Value:   []byte("invalid proto bytes"),
				},
			},
		}

		result := &repb.Platform{}
		ok, err := rexec.FindFirstAuxiliaryMetadata(md, result)

		require.Error(t, err)
		require.False(t, ok)
	})
}

func TestGetCachedExecuteResponse(t *testing.T) {
	for _, test := range []struct {
		name             string
		executionID      string
		expectedLookupID string
		cachedResponse   *repb.ExecuteResponse
		actionResult     *repb.ActionResult
		wantErrContains  string
	}{
		{
			name:             "execution ID with instance name",
			executionID:      "instance-name/uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142",
			expectedLookupID: "instance-name/uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142",
			cachedResponse: &repb.ExecuteResponse{
				Status: &statuspb.Status{Code: 0},
				Result: &repb.ActionResult{ExitCode: 123},
			},
		},
		{
			name:             "execution ID without instance name",
			executionID:      "/uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142",
			expectedLookupID: "/uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142",
			cachedResponse:   &repb.ExecuteResponse{Result: &repb.ActionResult{ExitCode: 1}},
		},
		{
			name:            "invalid execution ID",
			executionID:     "not-an-execution-id",
			wantErrContains: "parse execution ID",
		},
		{
			name:             "returns error if action result is missing serialized ExecuteResponse",
			executionID:      "/uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142",
			expectedLookupID: "/uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142",
			actionResult:     &repb.ActionResult{},
			wantErrContains:  "did not include inline ExecuteResponse",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			acClient := &fakeActionCacheClient{
				t: t,
				getActionResultFunc: func(_ context.Context, in *repb.GetActionResultRequest) (*repb.ActionResult, error) {
					require.NotEmpty(t, test.expectedLookupID, "unexpected action cache lookup")
					rn, err := digest.ParseUploadResourceName(test.expectedLookupID)
					require.NoError(t, err)
					expectedDigest, err := digest.Compute(strings.NewReader(test.expectedLookupID), rn.GetDigestFunction())
					require.NoError(t, err)
					if !proto.Equal(expectedDigest, in.GetActionDigest()) {
						return nil, gstatus.Error(codes.NotFound, "not found")
					}
					if test.actionResult != nil {
						return test.actionResult, nil
					}
					stdoutRaw, err := proto.Marshal(test.cachedResponse)
					require.NoError(t, err)
					return &repb.ActionResult{StdoutRaw: stdoutRaw}, nil
				},
			}

			rsp, err := rexec.GetCachedExecuteResponse(context.Background(), acClient, test.executionID)
			if test.wantErrContains != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), test.wantErrContains)
				return
			}
			require.NoError(t, err)
			require.Empty(t, cmp.Diff(test.cachedResponse, rsp, protocmp.Transform()))
		})
	}
}

func TestGetExecutionLogs(t *testing.T) {
	instanceName := "instance"
	digestFunction := repb.DigestFunction_SHA256
	stdout := "hello stdout"
	stderr := "hello stderr"

	stdoutDigest, err := digest.Compute(strings.NewReader(stdout), digestFunction)
	require.NoError(t, err)
	stderrDigest, err := digest.Compute(strings.NewReader(stderr), digestFunction)
	require.NoError(t, err)

	bsClient := &fakeByteStreamClient{
		data: map[string][]byte{
			digest.NewCASResourceName(stdoutDigest, instanceName, digestFunction).DownloadString(): []byte(stdout),
			digest.NewCASResourceName(stderrDigest, instanceName, digestFunction).DownloadString(): []byte(stderr),
		},
	}

	for _, test := range []struct {
		name            string
		executeResponse *repb.ExecuteResponse
	}{
		{
			name: "returns inline stdout and stderr",
			executeResponse: &repb.ExecuteResponse{
				Result: &repb.ActionResult{
					StdoutRaw: []byte(stdout),
					StderrRaw: []byte(stderr),
				},
			},
		},
		{
			name: "returns digest-backed stdout and stderr",
			executeResponse: &repb.ExecuteResponse{
				Result: &repb.ActionResult{
					StdoutDigest: stdoutDigest,
					StderrDigest: stderrDigest,
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			details, err := rexec.GetExecutionLogs(context.Background(), bsClient, instanceName, digestFunction, test.executeResponse)
			require.NoError(t, err)
			require.Equal(t, stdout, string(details.Stdout))
			require.Equal(t, stderr, string(details.Stderr))
			require.Nil(t, details.ServerLogs)
		})
	}
}
