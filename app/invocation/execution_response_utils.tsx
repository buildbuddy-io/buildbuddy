import { build } from "../../proto/remote_execution_ts_proto";
import rpcService, { CancelablePromise } from "../service/rpc_service";
import rpc_service from "../service/rpc_service";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import InvocationModel from "./invocation_model";
import GetExecutionResponse = execution_stats.GetExecutionResponse;
import ExecuteResponse = build.bazel.remote.execution.v2.ExecuteResponse;

// fetchExecuteResponseWithFallback attempts to fetch the execute response with
// the execute response digest if available, but falls back to using the action
// digest if the original fetch fails.
export async function fetchExecuteResponseWithFallback(
  model: InvocationModel,
  executeResponseDigest: any,
  executionActionDigest: any
): Promise<ExecuteResponse | null> {
  if (executeResponseDigest) {
    try {
      const executeResponse = await fetchExecuteResponseByDigest(model, executeResponseDigest);
      if (!executeResponse) {
        throw new Error("empty execute response");
      }
      return executeResponse;
    } catch (e) {
      console.log(
        `Failed to fetch execute response with execute response digest: ${e}. Attempting with action digest.`
      );
    }
  }

  if (!executionActionDigest) {
    throw new Error("Expected execution action digest");
  }
  const executeResponse = await fetchExecuteResponseByActionDigest(model, executionActionDigest);
  return executeResponse;
}

function fetchExecuteResponseByDigest(
  model: InvocationModel,
  executeResponseDigest: build.bazel.remote.execution.v2.Digest
) {
  return rpcService
    .fetchBytestreamFile(model.getActionCacheURL(executeResponseDigest), model.getInvocationId(), "arraybuffer")
    .then((buffer) => {
      const actionResult = build.bazel.remote.execution.v2.ActionResult.decode(new Uint8Array(buffer));
      // ExecuteResponse is encoded in ActionResult.stdout_raw field. See
      // proto field docs on `Execution.execute_response_digest`.
      const executeResponseBytes = actionResult.stdoutRaw;
      const executeResponse = build.bazel.remote.execution.v2.ExecuteResponse.decode(executeResponseBytes);
      return executeResponse;
    });
}

function fetchExecuteResponseByActionDigest(
  model: InvocationModel,
  actionDigest: build.bazel.remote.execution.v2.Digest
) {
  const service = rpc_service.getRegionalServiceOrDefault(model.stringCommandLineOption("remote_executor"));
  return service
    .getExecution({
      executionLookup: new execution_stats.ExecutionLookup({
        invocationId: model.getInvocationId(),
        actionDigestHash: actionDigest.hash,
      }),
      inlineExecuteResponse: true,
    })
    .then((response: GetExecutionResponse) => {
      const execution = response.execution?.[0];
      return execution?.executeResponse ?? null;
    });
}
