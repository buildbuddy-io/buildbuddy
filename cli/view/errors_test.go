package view_test

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/buildbuddy-io/buildbuddy/cli/util/download/downloadtest"
	"github.com/buildbuddy-io/buildbuddy/cli/view"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	fdpb "github.com/buildbuddy-io/buildbuddy/proto/failure_details"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const (
	stderrURI           = "bytestream://localhost/blobs/aabbcc/10"
	stdoutURI           = "bytestream://localhost/blobs/ddeeff/20"
	sandboxDebugMessage = "Use --sandbox_debug to see verbose messages from the sandbox and retain the sandbox build root for debugging"
)

type errorsBBClient struct {
	bbspb.BuildBuddyServiceClient
	response *inpb.GetInvocationResponse
	err      error
}

func (c *errorsBBClient) GetInvocation(ctx context.Context, req *inpb.GetInvocationRequest, opts ...grpc.CallOption) (*inpb.GetInvocationResponse, error) {
	return c.response, c.err
}

func invocationEvent(event *bespb.BuildEvent) *inpb.InvocationEvent {
	return &inpb.InvocationEvent{BuildEvent: event}
}

func TestViewErrors(t *testing.T) {
	actionFailure := &fdpb.FailureDetail{
		Message: "compilation failed",
		Category: &fdpb.FailureDetail_Spawn{
			Spawn: &fdpb.Spawn{Code: *fdpb.Spawn_NON_ZERO_EXIT.Enum()},
		},
	}
	actionEvent := invocationEvent(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Action{Action: &bespb.ActionExecuted{
			FailureDetail: actionFailure,
			Stderr: &bespb.File{
				File: &bespb.File_Uri{Uri: stderrURI},
			},
			Stdout: &bespb.File{
				File: &bespb.File_Uri{Uri: stdoutURI},
			},
		}},
	})
	secondActionEvent := invocationEvent(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Action{Action: &bespb.ActionExecuted{
			FailureDetail: &fdpb.FailureDetail{Message: "second action failure"},
		}},
	})
	abortedEvent := invocationEvent(&bespb.BuildEvent{
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_ConfiguredLabel{
			ConfiguredLabel: &bespb.BuildEventId_ConfiguredLabelId{Label: "//foo:bar"},
		}},
		Payload: &bespb.BuildEvent_Aborted{Aborted: &bespb.Aborted{
			Reason:      bespb.Aborted_ANALYSIS_FAILURE,
			Description: "target failed",
		}},
	})
	noBuildEvent := invocationEvent(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Aborted{Aborted: &bespb.Aborted{
			Reason:      bespb.Aborted_NO_BUILD,
			Description: "not an error",
		}},
	})
	skippedEvent := invocationEvent(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Aborted{Aborted: &bespb.Aborted{
			Reason:      bespb.Aborted_SKIPPED,
			Description: "also not an error",
		}},
	})
	finishedEvent := invocationEvent(&bespb.BuildEvent{
		Payload: &bespb.BuildEvent_Finished{Finished: &bespb.BuildFinished{
			FailureDetail: &fdpb.FailureDetail{
				Message: "bad target pattern",
				Category: &fdpb.FailureDetail_TargetPatterns{
					TargetPatterns: &fdpb.TargetPatterns{Code: *fdpb.TargetPatterns_TARGET_PATTERN_PARSE_FAILURE.Enum()},
				},
			},
		}},
	})

	client := &errorsBBClient{response: &inpb.GetInvocationResponse{
		Invocation: []*inpb.Invocation{{
			InvocationId: invocationID,
			Event: []*inpb.InvocationEvent{
				actionEvent,
				secondActionEvent,
				abortedEvent,
				abortedEvent, // Duplicate errors are only printed once.
				noBuildEvent,
				skippedEvent,
				finishedEvent,
			},
		}},
	}}
	downloader := downloadtest.New().
		Add(stderrURI, []byte("compile.go:10:20\n"+sandboxDebugMessage)).
		Add(stdoutURI, []byte("action stdout"))

	var buf bytes.Buffer
	code, err := view.ViewErrors(context.Background(), client, downloader, &buf, invocationID)

	require.NoError(t, err)
	require.Equal(t, 0, code)
	output := buf.String()
	require.Contains(t, output, "\x1b[1;91mERROR:\x1b[m non zero exit: compilation failed")
	require.NotContains(t, output, "second action failure")
	require.Contains(t, output, "\x1b[1;4m\x1b[1mcompile.go:10:20\x1b[0m")
	require.Contains(t, output, "\x1b[90m"+sandboxDebugMessage+"\x1b[0m\n \n")
	require.Contains(t, output, "action stdout")
	require.Contains(t, output, "//foo:bar: analysis failure: target failed")
	require.NotContains(t, output, "not an error")
	require.NotContains(t, output, "also not an error")
	require.Contains(t, output, "target pattern parse failure: bad target pattern")
	require.Equal(t, 3, strings.Count(output, "ERROR:"))
}

func TestViewErrors_NoErrors(t *testing.T) {
	client := &errorsBBClient{response: &inpb.GetInvocationResponse{
		Invocation: []*inpb.Invocation{{
			InvocationId: invocationID,
			Event: []*inpb.InvocationEvent{
				invocationEvent(&bespb.BuildEvent{
					Payload: &bespb.BuildEvent_Action{Action: &bespb.ActionExecuted{Success: true}},
				}),
				invocationEvent(&bespb.BuildEvent{
					Payload: &bespb.BuildEvent_Aborted{Aborted: &bespb.Aborted{Reason: bespb.Aborted_NO_ANALYZE}},
				}),
				invocationEvent(&bespb.BuildEvent{
					Payload: &bespb.BuildEvent_Finished{Finished: &bespb.BuildFinished{}},
				}),
			},
		}},
	}}

	var buf bytes.Buffer
	code, err := view.ViewErrors(context.Background(), client, downloadtest.New(), &buf, invocationID)

	require.NoError(t, err)
	require.Equal(t, 0, code)
	require.Empty(t, buf.String())
}

func TestViewErrors_InvocationNotFound(t *testing.T) {
	client := &errorsBBClient{response: &inpb.GetInvocationResponse{}}

	var buf bytes.Buffer
	code, err := view.ViewErrors(context.Background(), client, downloadtest.New(), &buf, invocationID)

	require.Equal(t, -1, code)
	require.EqualError(t, err, "invocation "+invocationID+" not found")
	require.Empty(t, buf.String())
}
