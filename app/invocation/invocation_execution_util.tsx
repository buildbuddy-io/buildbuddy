import React from "react";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import { google as google_grpc } from "../../proto/grpc_code_ts_proto";
import { google as google_ts } from "../../proto/timestamp_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";
import { RotateCw, Package, Clock, AlertCircle, XCircle, CheckCircle } from "lucide-react";

const ExecutionStage = build.bazel.remote.execution.v2.ExecutionStage;

const GRPC_STATUS_LABEL_BY_CODE: Record<number, string> = Object.fromEntries(
  Object.entries(google_grpc.rpc.Code).map(([name, value]) => [value, name])
);

const STATUSES_BY_STAGE: Record<number, ExecutionStatus> = {
  [ExecutionStage.Value.UNKNOWN]: { name: "Starting", icon: <RotateCw className="icon blue rotating" /> },
  [ExecutionStage.Value.CACHE_CHECK]: { name: "Cache check", icon: <Package className="icon brown" /> },
  [ExecutionStage.Value.QUEUED]: { name: "Queued", icon: <Clock className="icon" /> },
  [ExecutionStage.Value.EXECUTING]: { name: "Executing", icon: <RotateCw className="icon blue rotating" /> },
  // COMPLETED is not included here because it depends on the gRPC status and exit code.
};

export type ExecutionStatus = {
  name: string;
  icon: JSX.Element;
  className?: string;
};

export function getExecutionStatus(execution: execution_stats.IExecution): ExecutionStatus {
  if (execution.stage === ExecutionStage.Value.COMPLETED) {
    if (execution.status.code !== 0) {
      return {
        name: `Error (${GRPC_STATUS_LABEL_BY_CODE[execution.status.code] || "UNKNOWN"})`,
        icon: <AlertCircle className="icon red" />,
      };
    }
    if (execution.exitCode !== 0) {
      return {
        name: `Failed (exit code ${execution.exitCode})`,
        icon: <XCircle className="icon red" />,
      };
    }
    return { name: "Succeeded", icon: <CheckCircle className="icon green" /> };
  }

  return STATUSES_BY_STAGE[execution.stage];
}

export function subtractTimestamp(
  timestampA?: google_ts.protobuf.ITimestamp | null,
  timestampB?: google_ts.protobuf.ITimestamp | null
) {
  if (!timestampA || !timestampB) return NaN;
  let microsA = +timestampA.seconds * 1000000 + +timestampA.nanos / 1000;
  let microsB = +timestampB.seconds * 1000000 + +timestampB.nanos / 1000;
  return microsA - microsB;
}

export function totalDuration(execution: execution_stats.IExecution) {
  return subtractTimestamp(
    execution?.executedActionMetadata?.workerCompletedTimestamp,
    execution?.executedActionMetadata?.queuedTimestamp
  );
}

export function queuedDuration(execution: execution_stats.IExecution) {
  return subtractTimestamp(
    execution?.executedActionMetadata?.workerStartTimestamp,
    execution?.executedActionMetadata?.queuedTimestamp
  );
}

export function downloadDuration(execution: execution_stats.IExecution) {
  return subtractTimestamp(
    execution?.executedActionMetadata?.inputFetchCompletedTimestamp,
    execution?.executedActionMetadata?.inputFetchStartTimestamp
  );
}

export function executionDuration(execution: execution_stats.IExecution) {
  return subtractTimestamp(
    execution?.executedActionMetadata?.executionCompletedTimestamp,
    execution?.executedActionMetadata?.executionStartTimestamp
  );
}

export function uploadDuration(execution: execution_stats.IExecution) {
  return subtractTimestamp(
    execution?.executedActionMetadata?.outputUploadCompletedTimestamp,
    execution?.executedActionMetadata?.outputUploadStartTimestamp
  );
}
