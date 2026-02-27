package rexec_test

import (
	"context"
	"strings"
	"testing"

	repb "github.com/buildbuddy-io/buildbuddy/proto/remote_execution"
	"github.com/buildbuddy-io/buildbuddy/server/remote_cache/digest"
	"github.com/buildbuddy-io/buildbuddy/server/util/proto"
	"github.com/buildbuddy-io/buildbuddy/server/util/rexec"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/anypb"
)

type fakeActionCacheClient struct {
	getActionResultFunc func(ctx context.Context, in *repb.GetActionResultRequest) (*repb.ActionResult, error)
}

func (f *fakeActionCacheClient) GetActionResult(ctx context.Context, in *repb.GetActionResultRequest, _ ...grpc.CallOption) (*repb.ActionResult, error) {
	return f.getActionResultFunc(ctx, in)
}

func (f *fakeActionCacheClient) UpdateActionResult(context.Context, *repb.UpdateActionResultRequest, ...grpc.CallOption) (*repb.ActionResult, error) {
	panic("unexpected call")
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
	t.Run("parses execution id and fetches ExecuteResponse from action cache", func(t *testing.T) {
		executionID := "instance-name/uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142"
		expected := &repb.ExecuteResponse{
			Status: &statuspb.Status{Code: 0},
			Result: &repb.ActionResult{ExitCode: 123},
		}
		stdoutRaw, err := proto.Marshal(expected)
		require.NoError(t, err)

		var req *repb.GetActionResultRequest
		acClient := &fakeActionCacheClient{
			getActionResultFunc: func(_ context.Context, in *repb.GetActionResultRequest) (*repb.ActionResult, error) {
				req = in
				return &repb.ActionResult{StdoutRaw: stdoutRaw}, nil
			},
		}

		rsp, err := rexec.GetCachedExecuteResponse(context.Background(), acClient, executionID)
		require.NoError(t, err)
		require.Empty(t, cmp.Diff(expected, rsp, protocmp.Transform()))

		rn, err := digest.ParseUploadResourceName(executionID)
		require.NoError(t, err)
		d, err := digest.Compute(strings.NewReader(executionID), rn.GetDigestFunction())
		require.NoError(t, err)
		require.Equal(t, rn.GetInstanceName(), req.GetInstanceName())
		require.Equal(t, rn.GetDigestFunction(), req.GetDigestFunction())
		require.Empty(t, cmp.Diff(d, req.GetActionDigest(), protocmp.Transform()))
	})

	t.Run("accepts leading slash in execution id", func(t *testing.T) {
		executionID := "uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142"
		expected := &repb.ExecuteResponse{Result: &repb.ActionResult{ExitCode: 1}}
		stdoutRaw, err := proto.Marshal(expected)
		require.NoError(t, err)

		var req *repb.GetActionResultRequest
		acClient := &fakeActionCacheClient{
			getActionResultFunc: func(_ context.Context, in *repb.GetActionResultRequest) (*repb.ActionResult, error) {
				req = in
				return &repb.ActionResult{StdoutRaw: stdoutRaw}, nil
			},
		}

		rsp, err := rexec.GetCachedExecuteResponse(context.Background(), acClient, "/"+executionID)
		require.NoError(t, err)
		require.Empty(t, cmp.Diff(expected, rsp, protocmp.Transform()))

		rn, err := digest.ParseUploadResourceName(executionID)
		require.NoError(t, err)
		d, err := digest.Compute(strings.NewReader("/"+executionID), rn.GetDigestFunction())
		require.NoError(t, err)
		require.Empty(t, cmp.Diff(d, req.GetActionDigest(), protocmp.Transform()))
	})

	t.Run("falls back to alternate slash normalization for compatibility", func(t *testing.T) {
		executionID := "uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142"
		expected := &repb.ExecuteResponse{Result: &repb.ActionResult{ExitCode: 7}}
		stdoutRaw, err := proto.Marshal(expected)
		require.NoError(t, err)

		rn, err := digest.ParseUploadResourceName(executionID)
		require.NoError(t, err)
		withSlash, err := digest.Compute(strings.NewReader("/"+executionID), rn.GetDigestFunction())
		require.NoError(t, err)
		withoutSlash, err := digest.Compute(strings.NewReader(executionID), rn.GetDigestFunction())
		require.NoError(t, err)

		var calls int
		acClient := &fakeActionCacheClient{
			getActionResultFunc: func(_ context.Context, in *repb.GetActionResultRequest) (*repb.ActionResult, error) {
				calls++
				if cmp.Equal(in.GetActionDigest(), withSlash, protocmp.Transform()) {
					return nil, status.Error(codes.NotFound, "not found")
				}
				if cmp.Equal(in.GetActionDigest(), withoutSlash, protocmp.Transform()) {
					return &repb.ActionResult{StdoutRaw: stdoutRaw}, nil
				}
				return nil, status.Error(codes.NotFound, "unexpected digest")
			},
		}

		rsp, err := rexec.GetCachedExecuteResponse(context.Background(), acClient, "/"+executionID)
		require.NoError(t, err)
		require.Empty(t, cmp.Diff(expected, rsp, protocmp.Transform()))
		require.Equal(t, 2, calls)
	})

	t.Run("returns error when execution id is invalid", func(t *testing.T) {
		_, err := rexec.GetCachedExecuteResponse(context.Background(), &fakeActionCacheClient{}, "not-an-execution-id")
		require.Error(t, err)
		require.Contains(t, err.Error(), "parse execution ID")
	})

	t.Run("returns error when ExecuteResponse is missing from action result", func(t *testing.T) {
		acClient := &fakeActionCacheClient{
			getActionResultFunc: func(_ context.Context, in *repb.GetActionResultRequest) (*repb.ActionResult, error) {
				return &repb.ActionResult{}, nil
			},
		}
		executionID := "uploads/0f8fad5b-d9cb-469f-a165-70867728950e/blobs/f33f7f8f85f0c4f68f68422e6159c252f2b0f73cc75ad1df69c46733465ff7f7/142"
		_, err := rexec.GetCachedExecuteResponse(context.Background(), acClient, executionID)
		require.Error(t, err)
		require.Contains(t, err.Error(), "did not include inline ExecuteResponse")
	})
}

func TestGetExecutionLogs(t *testing.T) {
	t.Run("returns inline stdout and stderr", func(t *testing.T) {
		rsp := &repb.ExecuteResponse{
			Result: &repb.ActionResult{
				StdoutRaw: []byte("hello stdout"),
				StderrRaw: []byte("hello stderr"),
			},
		}
		details, err := rexec.GetExecutionLogs(context.Background(), nil, "", repb.DigestFunction_SHA256, rsp)
		require.NoError(t, err)
		require.Equal(t, "hello stdout", string(details.Stdout))
		require.Equal(t, "hello stderr", string(details.Stderr))
		require.Nil(t, details.ServerLogs)
	})

	t.Run("returns error when digest fetch is needed and bytestream client is missing", func(t *testing.T) {
		rsp := &repb.ExecuteResponse{
			Result: &repb.ActionResult{
				StdoutDigest: &repb.Digest{Hash: strings.Repeat("a", 64), SizeBytes: 1},
			},
		}
		_, err := rexec.GetExecutionLogs(context.Background(), nil, "", repb.DigestFunction_SHA256, rsp)
		require.Error(t, err)
		require.Contains(t, err.Error(), "ByteStreamClient not configured")
	})
}
