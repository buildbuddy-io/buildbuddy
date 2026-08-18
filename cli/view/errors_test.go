package view_test

import (
	"bytes"
	"context"
	"io"
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
	stderrURI = "bytestream://localhost/blobs/aabbcc/10"
	stdoutURI = "bytestream://localhost/blobs/ddeeff/20"
)

type errorsBBClient struct {
	bbspb.BuildBuddyServiceClient
	response *inpb.GetInvocationResponse
	err      error
}

type noCallDownloader struct{}

func (d *noCallDownloader) GetBytestreamFile(ctx context.Context, uri string, w io.Writer) error {
	panic("GetBytestreamFile should not be called")
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
		Id: &bespb.BuildEventId{Id: &bespb.BuildEventId_TargetCompleted{
			TargetCompleted: &bespb.BuildEventId_TargetCompletedId{Label: "//foo:bar"},
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
				noBuildEvent,
				skippedEvent,
				abortedEvent,
				actionEvent,
				secondActionEvent,
				abortedEvent, // Duplicate errors are only printed once.
				finishedEvent,
			},
		}},
	}}
	downloader := downloadtest.New().
		Add(stderrURI, []byte("compile.go:10:20")).
		Add(stdoutURI, []byte("action stdout"))

	var buf bytes.Buffer
	err := view.ViewErrors(context.Background(), client, downloader, &buf, invocationID)

	require.NoError(t, err)
	output := buf.String()
	// Output should contain the failure message.
	require.Contains(t, output, "compilation failed")
	// Output should contain the failing action's stderr and stdout.
	require.Contains(t, output, "compile.go:10:20")
	require.Contains(t, output, "action stdout")
	// Only the first action failure should be printed.
	require.NotContains(t, output, "second action failure")
	require.NotContains(t, output, "//foo:bar: analysis failure: target failed")
	require.NotContains(t, output, "not an error")
	require.NotContains(t, output, "bad target pattern")
}

func TestViewErrors_SkipsLocalActionOutput(t *testing.T) {
	client := &errorsBBClient{response: &inpb.GetInvocationResponse{
		Invocation: []*inpb.Invocation{{
			InvocationId: invocationID,
			Event: []*inpb.InvocationEvent{
				invocationEvent(&bespb.BuildEvent{
					Payload: &bespb.BuildEvent_Action{Action: &bespb.ActionExecuted{
						FailureDetail: &fdpb.FailureDetail{Message: "compilation failed"},
						Stderr: &bespb.File{File: &bespb.File_Uri{
							Uri: "file:///tmp/action-stderr",
						}},
						Stdout: &bespb.File{File: &bespb.File_Uri{
							Uri: "file:///tmp/action-stdout",
						}},
					}},
				}),
			},
		}},
	}}

	var buf bytes.Buffer
	err := view.ViewErrors(context.Background(), client, &noCallDownloader{}, &buf, invocationID)

	require.NoError(t, err)
	require.Contains(t, buf.String(), "compilation failed")
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
	err := view.ViewErrors(context.Background(), client, downloadtest.New(), &buf, invocationID)

	require.NoError(t, err)
	require.Empty(t, buf.String())
}
