import rpc_service, { ServerStreamHandler, ServerStream } from "../service/rpc_service";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";

const ExecutionStage = build.bazel.remote.execution.v2.ExecutionStage.Value;
const ExecutionState = build.bazel.remote.execution.v2.ExecutionProgress.ExecutionState;

/**
 * Live-streams execution updates using the WaitExecution API.
 * Each operation is unpacked into a more usable format.
 */
export function waitExecution(
  executionId: string,
  handler: ServerStreamHandler<ExecuteOperation>
): ServerStream<ExecuteOperation> {
  return rpc_service.service.waitExecution(
    { executionId },
    {
      next: (response) => {
        const unpackedOperation = unpackResponse(response);
        if (unpackedOperation) {
          handler.next(unpackedOperation);
        }
      },
      error: (e) => {
        // TODO: if we disconnected from the app (e.g. app restart), don't
        // bubble up the error, just transparently reconnect.
        handler.error(e);
      },
      complete: () => {
        handler.complete();
      },
    }
  );
}

export function executionStatusLabel(op: ExecuteOperation) {
  if (op.done) {
    return "Completed";
  }

  switch (op.metadata?.stage) {
    case ExecutionStage.CACHE_CHECK:
      return "Checking cache";
    case ExecutionStage.QUEUED:
      return "Queued";
    case ExecutionStage.EXECUTING:
      // Return fine-grained progress using BB-specific metadata
      return executionProgressLabel(op);
    case ExecutionStage.COMPLETED:
      return "Completed";
    default:
      return "Unknown";
  }
}

function executionProgressLabel(op: ExecuteOperation) {
  switch (op.progress?.executionState) {
    case ExecutionState.PULLING_CONTAINER_IMAGE:
      return "Pulling container image";
    case ExecutionState.DOWNLOADING_INPUTS:
      return "Downloading inputs";
    case ExecutionState.EXECUTING_COMMAND:
      return "Executing command";
    case ExecutionState.UPLOADING_OUTPUTS:
      return "Uploading outputs";
    case ExecutionState.BOOTING_VM:
      return "Booting VM";
    case ExecutionState.RESUMING_VM:
      return "Resuming VM snapshot";
    default:
      return "Executing";
  }
}

/** Unpacked execute operation. */
export type ExecuteOperation = {
  metadata?: build.bazel.remote.execution.v2.ExecuteOperationMetadata;
  progress?: build.bazel.remote.execution.v2.ExecutionProgress;
  response?: build.bazel.remote.execution.v2.ExecuteResponse;
  done: boolean;
};

function unpackResponse(response: execution_stats.WaitExecutionResponse): ExecuteOperation | null {
  if (!response.operation) return null;
  const unpacked: ExecuteOperation = {
    done: response.operation.done,
  };
  if (response.operation.metadata) {
    unpacked.metadata = build.bazel.remote.execution.v2.ExecuteOperationMetadata.decode(
      response.operation.metadata.value
    );
  }
  if (response.operation.response) {
    unpacked.response = build.bazel.remote.execution.v2.ExecuteResponse.decode(response.operation.response.value);
  }
  // Unpack BuildBuddy auxiliary metadata
  if (unpacked.metadata) {
    for (const auxiliaryMetadata of unpacked.metadata?.partialExecutionMetadata?.auxiliaryMetadata ?? []) {
      if (auxiliaryMetadata.typeUrl === build.bazel.remote.execution.v2.ExecutionProgress.getTypeUrl()) {
        unpacked.progress = build.bazel.remote.execution.v2.ExecutionProgress.decode(auxiliaryMetadata.value);
      }
    }
  }
  return unpacked;
}
