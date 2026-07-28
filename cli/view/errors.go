package view

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"

	"github.com/buildbuddy-io/buildbuddy/cli/log"
	"github.com/buildbuddy-io/buildbuddy/cli/util/download"

	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	bbspb "github.com/buildbuddy-io/buildbuddy/proto/buildbuddy_service"
	fdpb "github.com/buildbuddy-io/buildbuddy/proto/failure_details"
	inpb "github.com/buildbuddy-io/buildbuddy/proto/invocation"
)

const sandboxDebugMessage = "Use --sandbox_debug to see verbose messages from the sandbox and retain the sandbox build root for debugging"

var fileNamePattern = regexp.MustCompile(`([^\s:]+:\d+:\d+)`)

type invocationError struct {
	action       *bespb.ActionExecuted
	actionStderr string
	actionStdout string
	aborted      *bespb.BuildEvent
	finished     *bespb.BuildFinished
}

// ViewErrors writes the errors displayed by the invocation error card to w.
func ViewErrors(ctx context.Context, bbClient bbspb.BuildBuddyServiceClient, downloader download.Downloader, w io.Writer, invocationID string) (int, error) {
	resp, err := bbClient.GetInvocation(ctx, &inpb.GetInvocationRequest{
		Lookup: &inpb.InvocationLookup{InvocationId: invocationID},
	})
	if err != nil {
		return -1, fmt.Errorf("failed to get invocation: %w", err)
	}
	if len(resp.GetInvocation()) == 0 {
		return -1, fmt.Errorf("invocation %s not found", invocationID)
	}

	errors := collectInvocationErrors(resp.GetInvocation()[0])
	for _, invocationError := range errors {
		if invocationError.action == nil {
			continue
		}
		invocationError.actionStderr = downloadActionOutput(
			ctx, downloader, invocationError.action.GetStderr().GetUri(), "stderr")
		invocationError.actionStdout = downloadActionOutput(
			ctx, downloader, invocationError.action.GetStdout().GetUri(), "stdout")
	}

	text := formatInvocationErrors(errors)
	if text != "" {
		fmt.Fprintln(w, text)
	}
	return 0, nil
}

func collectInvocationErrors(invocation *inpb.Invocation) []*invocationError {
	var errors []*invocationError

	// InvocationModel keeps only the first action carrying a FailureDetail.
	for _, event := range invocation.GetEvent() {
		action := event.GetBuildEvent().GetAction()
		if action.GetFailureDetail().GetMessage() != "" {
			errors = append(errors, &invocationError{action: action})
			break
		}
	}

	for _, event := range invocation.GetEvent() {
		buildEvent := event.GetBuildEvent()
		aborted := buildEvent.GetAborted()
		if aborted == nil {
			continue
		}
		reason := aborted.GetReason()
		if reason == bespb.Aborted_SKIPPED ||
			reason == bespb.Aborted_NO_BUILD ||
			reason == bespb.Aborted_NO_ANALYZE {
			continue
		}
		errors = append(errors, &invocationError{aborted: buildEvent})
	}

	for _, event := range invocation.GetEvent() {
		finished := event.GetBuildEvent().GetFinished()
		if finished.GetFailureDetail().GetMessage() != "" {
			errors = append(errors, &invocationError{finished: finished})
		}
	}

	return errors
}

func downloadActionOutput(ctx context.Context, downloader download.Downloader, uri, outputType string) string {
	if uri == "" {
		return ""
	}
	var buf bytes.Buffer
	if err := downloader.GetBytestreamFile(ctx, uri, &buf); err != nil {
		log.Debugf("Failed to download failed action %s: %s", outputType, err)
		return ""
	}
	return buf.String()
}

func formatInvocationErrors(errors []*invocationError) string {
	const errorPrefix = "\x1b[1;91mERROR:\x1b[m "
	var lines []string

	for _, invocationError := range errors {
		if aborted := invocationError.aborted; aborted.GetAborted().GetReason() != bespb.Aborted_UNKNOWN {
			lines = append(lines, errorPrefix+joinNonEmpty([]string{
				aborted.GetId().GetConfiguredLabel().GetLabel(),
				aborted.GetId().GetTargetConfigured().GetLabel(),
				naiveFormatEnum(aborted.GetAborted().GetReason().String()),
				aborted.GetAborted().GetDescription(),
			}, ": "))
		}
		if action := invocationError.action; action != nil {
			if action.GetFailureDetail() != nil {
				lines = append(lines, errorPrefix+formatFailureDescription(action.GetFailureDetail()))
			}
			if invocationError.actionStderr != "" {
				lines = append(lines, "\x1b[1m"+invocationError.actionStderr+"\x1b[m")
			}
			if invocationError.actionStdout != "" {
				lines = append(lines, "\x1b[1m"+invocationError.actionStdout+"\x1b[m")
			}
		}
		if finished := invocationError.finished; finished.GetFailureDetail() != nil {
			lines = append(lines, errorPrefix+formatFailureDescription(finished.GetFailureDetail()))
		}
	}

	text := strings.Join(deduplicateLines(lines), "\n")
	text = strings.ReplaceAll(text, sandboxDebugMessage, "\x1b[90m"+sandboxDebugMessage+"\x1b[0m\n \n")
	return fileNamePattern.ReplaceAllString(text, "\x1b[1;4m$1\x1b[0m")
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
	return joinNonEmpty([]string{naiveFormatEnum(code), detail.GetMessage()}, ": ")
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

func deduplicateLines(lines []string) []string {
	seen := make(map[string]struct{}, len(lines))
	deduplicated := make([]string, 0, len(lines))
	for _, line := range lines {
		if _, ok := seen[line]; ok {
			continue
		}
		seen[line] = struct{}{}
		deduplicated = append(deduplicated, line)
	}
	return deduplicated
}

func naiveFormatEnum(value string) string {
	return strings.ToLower(strings.ReplaceAll(value, "_", " "))
}
