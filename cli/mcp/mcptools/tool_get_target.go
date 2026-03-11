package mcptools

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"strings"

	cmpb "github.com/buildbuddy-io/buildbuddy/proto/api/v1/common"
	bespb "github.com/buildbuddy-io/buildbuddy/proto/build_event_stream"
	trpb "github.com/buildbuddy-io/buildbuddy/proto/target"
	durationpb "google.golang.org/protobuf/types/known/durationpb"
)

func (s *Service) getTarget(ctx context.Context, args map[string]any) (any, error) {
	invocationRef, err := requiredString(args, "invocation_id")
	if err != nil {
		return nil, err
	}
	invocationID, err := extractInvocationID(invocationRef)
	if err != nil {
		return nil, err
	}
	targetLabel, err := optionalString(args, "target_label")
	if err != nil {
		return nil, err
	}
	filter, err := optionalString(args, "filter")
	if err != nil {
		return nil, err
	}
	pageToken, err := optionalString(args, "page_token")
	if err != nil {
		return nil, err
	}
	statusValue, hasStatus := args["status"]

	req := &trpb.GetTargetRequest{
		InvocationId: invocationID,
		TargetLabel:  targetLabel,
		Filter:       filter,
		PageToken:    pageToken,
	}
	if hasStatus {
		parsedStatus, err := parseTargetStatus(statusValue)
		if err != nil {
			return nil, err
		}
		req.Status = &parsedStatus
	}

	authCtx, err := s.authenticatedContext(ctx)
	if err != nil {
		return nil, err
	}
	rsp, err := s.bbClient.GetTarget(authCtx, req)
	if err != nil {
		return nil, apiRequestError(fmt.Sprintf("get target for invocation %q", invocationID), req, err)
	}

	groupSummaries := make([]map[string]any, 0, len(rsp.GetTargetGroups()))
	totalTargetsReturned := 0
	for _, group := range rsp.GetTargetGroups() {
		targetSummaries := make([]map[string]any, 0, len(group.GetTargets()))
		for _, target := range group.GetTargets() {
			targetSummaries = append(targetSummaries, targetSummary(target))
		}
		totalTargetsReturned += len(targetSummaries)
		groupSummaries = append(groupSummaries, map[string]any{
			"status":          group.GetStatus().String(),
			"total_count":     group.GetTotalCount(),
			"next_page_token": group.GetNextPageToken(),
			"targets":         targetSummaries,
		})
	}

	query := map[string]any{
		"target_label": targetLabel,
		"filter":       filter,
		"page_token":   pageToken,
	}
	if hasStatus {
		query["status"] = req.GetStatus().String()
	}
	return map[string]any{
		"invocation_id":          invocationID,
		"query":                  query,
		"target_group_count":     len(groupSummaries),
		"targets_returned_count": totalTargetsReturned,
		"target_groups":          groupSummaries,
	}, nil
}

func parseTargetStatus(value any) (cmpb.Status, error) {
	switch typed := value.(type) {
	case float64:
		if math.Trunc(typed) != typed {
			return cmpb.Status_STATUS_UNSPECIFIED, fmt.Errorf("\"status\" must be an integer or status name")
		}
		n := int32(typed)
		status := cmpb.Status(n)
		if _, ok := cmpb.Status_name[n]; ok {
			return status, nil
		}
		return cmpb.Status_STATUS_UNSPECIFIED, fmt.Errorf("unknown target status numeric value %d", n)
	case string:
		s := strings.TrimSpace(typed)
		if s == "" {
			return cmpb.Status_STATUS_UNSPECIFIED, fmt.Errorf("\"status\" must be non-empty")
		}
		if n, err := strconv.Atoi(s); err == nil {
			status := cmpb.Status(n)
			if _, ok := cmpb.Status_name[int32(status)]; ok {
				return status, nil
			}
			return cmpb.Status_STATUS_UNSPECIFIED, fmt.Errorf("unknown target status numeric value %d", n)
		}
		normalized := strings.ToUpper(strings.ReplaceAll(strings.ReplaceAll(s, "-", "_"), " ", "_"))
		if value, ok := cmpb.Status_value[normalized]; ok {
			return cmpb.Status(value), nil
		}
		if value, ok := cmpb.Status_value["STATUS_"+normalized]; ok {
			return cmpb.Status(value), nil
		}
		return cmpb.Status_STATUS_UNSPECIFIED, fmt.Errorf("unknown target status %q", s)
	default:
		return cmpb.Status_STATUS_UNSPECIFIED, fmt.Errorf("\"status\" must be an integer or status name")
	}
}

func targetSummary(target *trpb.Target) map[string]any {
	files := make([]map[string]any, 0, len(target.GetFiles()))
	for _, file := range target.GetFiles() {
		files = append(files, fileSummary(file))
	}

	summary := map[string]any{
		"metadata": map[string]any{
			"label":       target.GetMetadata().GetLabel(),
			"rule_type":   target.GetMetadata().GetRuleType(),
			"target_type": target.GetMetadata().GetTargetType().String(),
			"test_size":   target.GetMetadata().GetTestSize().String(),
		},
		"status":                  target.GetStatus().String(),
		"timing":                  timingSummary(target.GetTiming()),
		"root_cause":              target.GetRootCause(),
		"files":                   files,
		"test_result_event_count": len(target.GetTestResultEvents()),
		"action_event_count":      len(target.GetActionEvents()),
	}
	if target.GetTestSummary() != nil {
		summary["test_summary"] = map[string]any{
			"overall_status":          target.GetTestSummary().GetOverallStatus().String(),
			"total_run_count":         target.GetTestSummary().GetTotalRunCount(),
			"run_count":               target.GetTestSummary().GetRunCount(),
			"attempt_count":           target.GetTestSummary().GetAttemptCount(),
			"shard_count":             target.GetTestSummary().GetShardCount(),
			"total_num_cached":        target.GetTestSummary().GetTotalNumCached(),
			"passed_log_count":        len(target.GetTestSummary().GetPassed()),
			"failed_log_count":        len(target.GetTestSummary().GetFailed()),
			"first_start_time":        timestampString(target.GetTestSummary().GetFirstStartTime()),
			"last_stop_time":          timestampString(target.GetTestSummary().GetLastStopTime()),
			"total_run_duration_usec": durationUsec(target.GetTestSummary().GetTotalRunDuration()),
		}
	}
	return summary
}

func fileSummary(file *bespb.File) map[string]any {
	summary := map[string]any{
		"path":         filePath(file),
		"name":         file.GetName(),
		"uri":          file.GetUri(),
		"digest":       file.GetDigest(),
		"length_bytes": file.GetLength(),
	}
	if symlinkTarget := file.GetSymlinkTargetPath(); symlinkTarget != "" {
		summary["symlink_target_path"] = symlinkTarget
	}
	if contentsLen := len(file.GetContents()); contentsLen > 0 {
		summary["inlined_contents_bytes"] = contentsLen
	}
	return summary
}

func timingSummary(timing *cmpb.Timing) map[string]any {
	if timing == nil {
		return map[string]any{}
	}
	return map[string]any{
		"start_time":    timestampString(timing.GetStartTime()),
		"duration_usec": durationUsec(timing.GetDuration()),
	}
}

func durationUsec(d *durationpb.Duration) int64 {
	if d == nil {
		return 0
	}
	return d.AsDuration().Microseconds()
}
