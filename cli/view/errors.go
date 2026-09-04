package view

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/util/download"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	fdpb "github.com/buildbuddy-io/buildbuddy/proto/failure_details"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

type invocationError struct {
	action       *bespb.ActionExecuted
	actionStderr string
	actionStdout string
	aborted      *bespb.BuildEvent
	finished     *bespb.BuildFinished
}

// ViewErrors writes the first invocation error to w.
func ViewErrors(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, downloader download.Downloader, w io.Writer, invocationID string) error {
	resp, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{InvocationId: invocationID},
	})
	if err != nil {
		return fmt.Errorf("failed to get invocation: %w", err)
	}
	if len(resp.GetInvocation()) == 0 {
		return fmt.Errorf("invocation %s not found", invocationID)
	}

	invocationError := firstInvocationError(ctx, downloader, resp.GetInvocation()[0])
	text := formatInvocationError(invocationError)
	if text != "" {
		fmt.Fprintln(w, text)
	}
	return nil
}

// firstInvocationError returns the first failed action, or the first relevant
// aborted or finished event if the invocation has no failed actions.
func firstInvocationError(ctx context.Context, downloader download.Downloader, invocation *inpb.Invocation) *invocationError {
	var fallback *invocationError
	for _, event := range invocation.GetEvent() {
		buildEvent := event.GetBuildEvent()
		action := buildEvent.GetAction()
		if action.GetFailureDetail().GetMessage() != "" {
			return &invocationError{
				action:       action,
				actionStderr: downloadActionOutput(ctx, downloader, action.GetStderr().GetUri(), "stderr"),
				actionStdout: downloadActionOutput(ctx, downloader, action.GetStdout().GetUri(), "stdout"),
			}
		}

		aborted := buildEvent.GetAborted()
		if aborted != nil {
			reason := aborted.GetReason()
			if reason != bespb.Aborted_SKIPPED &&
				reason != bespb.Aborted_NO_BUILD &&
				reason != bespb.Aborted_NO_ANALYZE &&
				reason != bespb.Aborted_INCOMPLETE &&
				reason != bespb.Aborted_UNKNOWN &&
				fallback == nil {
				fallback = &invocationError{aborted: buildEvent}
			}
		}

		finished := buildEvent.GetFinished()
		if finished.GetFailureDetail().GetMessage() != "" && fallback == nil {
			fallback = &invocationError{finished: finished}
		}
	}

	return fallback
}

func downloadActionOutput(ctx context.Context, downloader download.Downloader, uri, outputType string) string {
	// file:// URIs refer to paths on the machine that ran Bazel and are not
	// available through the remote ByteStream API.
	if !strings.HasPrefix(uri, "bytestream://") {
		return ""
	}
	var buf bytes.Buffer
	if err := downloader.GetBytestreamFile(ctx, uri, &buf); err != nil {
		log.Warnf("Failed to download failed action %s: %s", outputType, err)
		return ""
	}
	return buf.String()
}

func formatInvocationError(invocationError *invocationError) string {
	if invocationError == nil {
		return ""
	}

	const errorPrefix = "\x1b[1;91mERROR:\x1b[m "
	var lines []string

	if aborted := invocationError.aborted; aborted.GetAborted().GetReason() != bespb.Aborted_UNKNOWN {
		lines = append(lines, errorPrefix+joinNonEmpty([]string{
			aborted.GetId().GetConfiguredLabel().GetLabel(),
			aborted.GetId().GetTargetConfigured().GetLabel(),
			aborted.GetId().GetTargetCompleted().GetLabel(),
			aborted.GetAborted().GetReason().String(),
			aborted.GetAborted().GetDescription(),
		}, ": "))
	}
	if action := invocationError.action; action != nil {
		if action.GetFailureDetail() != nil {
			lines = append(lines, errorPrefix+formatFailureDescription(action.GetFailureDetail()))
		}
		if invocationError.actionStderr != "" {
			lines = append(lines, invocationError.actionStderr)
		}
		if invocationError.actionStdout != "" {
			lines = append(lines, invocationError.actionStdout)
		}
	}
	if finished := invocationError.finished; finished.GetFailureDetail() != nil {
		lines = append(lines, errorPrefix+formatFailureDescription(finished.GetFailureDetail()))
	}

	text := strings.Join(lines, "\n")
	return text
}

func formatFailureDescription(detail *fdpb.FailureDetail) string {
	code := ""
	switch {
	case detail.GetSpawn() != nil:
		code = detail.GetSpawn().GetCode().String()
	case detail.GetExecution() != nil:
		code = detail.GetExecution().GetCode().String()
	case detail.GetTargetPatterns() != nil:
		code = detail.GetTargetPatterns().GetCode().String()
	}
	return joinNonEmpty([]string{code, detail.GetMessage()}, ": ")
}

func joinNonEmpty(parts []string, separator string) string {
	var nonEmpty []string
	for _, part := range parts {
		if part != "" {
			nonEmpty = append(nonEmpty, part)
		}
	}
	return strings.Join(nonEmpty, separator)
}
