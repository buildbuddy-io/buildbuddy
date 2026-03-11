package mcptools

import (
	"context"
	"encoding/json"
	"fmt"

	espb "github.com/buildbuddy-io/buildbuddy/proto/execution_stats"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func (s *Service) getExecutions(ctx context.Context, args map[string]any) (any, error) {
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
	actionDigest, err := optionalString(args, "action_digest")
	if err != nil {
		return nil, err
	}

	req := &espb.GetExecutionRequest{
		ExecutionLookup: &espb.ExecutionLookup{
			InvocationId:     invocationID,
			TargetLabel:      targetLabel,
			ActionDigestHash: actionDigest,
		},
	}
	authCtx, err := s.authenticatedContext(ctx)
	if err != nil {
		return nil, err
	}
	rsp, err := s.bbClient.GetExecution(authCtx, req)
	if err != nil {
		return nil, apiRequestError(fmt.Sprintf("get executions for invocation %q", invocationID), req, err)
	}

	executionSummaries := make([]map[string]any, 0, len(rsp.GetExecution()))
	for _, execution := range rsp.GetExecution() {
		executionSummaries = append(executionSummaries, executionSummary(execution))
	}

	query := map[string]any{
		"target_label":  targetLabel,
		"action_digest": actionDigest,
	}
	return map[string]any{
		"invocation_id":   invocationID,
		"query":           query,
		"execution_count": len(executionSummaries),
		"executions":      executionSummaries,
	}, nil
}

func (s *Service) getExecution(ctx context.Context, args map[string]any) (any, error) {
	executionID, err := requiredString(args, "execution_id")
	if err != nil {
		return nil, err
	}
	invocationRef, err := optionalString(args, "invocation_id")
	if err != nil {
		return nil, err
	}
	invocationID := ""
	if invocationRef != "" {
		invocationID, err = extractInvocationID(invocationRef)
		if err != nil {
			return nil, err
		}
	}

	req := &espb.GetExecutionRequest{
		ExecutionLookup: &espb.ExecutionLookup{
			InvocationId: invocationID,
			ExecutionId:  executionID,
		},
		InlineExecuteResponse: true,
	}
	authCtx, err := s.authenticatedContext(ctx)
	if err != nil {
		return nil, err
	}
	rsp, err := s.bbClient.GetExecution(authCtx, req)
	if err != nil {
		return nil, apiRequestError(fmt.Sprintf("get execution %q", executionID), req, err)
	}
	if len(rsp.GetExecution()) == 0 {
		return nil, fmt.Errorf("execution %q not found", executionID)
	}

	matched := rsp.GetExecution()[0]
	for _, execution := range rsp.GetExecution() {
		if execution.GetExecutionId() == executionID {
			matched = execution
			break
		}
	}

	return map[string]any{
		"execution_id":  executionID,
		"invocation_id": invocationID,
		"execution":     executionSummary(matched),
	}, nil
}

func executionSummary(execution *espb.Execution) map[string]any {
	if execution == nil {
		return map[string]any{}
	}
	executedMetadata := execution.GetExecutedActionMetadata()
	return map[string]any{
		"execution_id":             execution.GetExecutionId(),
		"stage":                    execution.GetStage().String(),
		"status":                   protoMessageMap(execution.GetStatus()),
		"exit_code":                execution.GetExitCode(),
		"target_label":             execution.GetTargetLabel(),
		"action_mnemonic":          execution.GetActionMnemonic(),
		"command_snippet":          execution.GetCommandSnippet(),
		"primary_output_path":      execution.GetPrimaryOutputPath(),
		"executor_hostname":        execution.GetExecutorHostname(),
		"action_digest":            protoMessageMap(execution.GetActionDigest()),
		"action_result_digest":     protoMessageMap(execution.GetActionResultDigest()),
		"execute_response_digest":  protoMessageMap(execution.GetExecuteResponseDigest()),
		"task_size":                protoMessageMap(execution.GetTaskSize()),
		"executed_action_metadata": protoMessageMap(executedMetadata),
		"io_stats":                 protoMessageMap(executedMetadata.GetIoStats()),
		"usage_stats":              protoMessageMap(executedMetadata.GetUsageStats()),
		"estimated_task_size":      protoMessageMap(executedMetadata.GetEstimatedTaskSize()),
		"execute_response":         protoMessageMap(execution.GetExecuteResponse()),
	}
}

func protoMessageMap(message proto.Message) map[string]any {
	if message == nil {
		return map[string]any{}
	}
	encoded, err := protojson.MarshalOptions{
		UseProtoNames: true,
	}.Marshal(message)
	if err != nil {
		return map[string]any{"_marshal_error": err.Error()}
	}
	out := map[string]any{}
	if err := json.Unmarshal(encoded, &out); err != nil {
		return map[string]any{
			"_unmarshal_error": err.Error(),
			"_proto_json":      string(encoded),
		}
	}
	return out
}
