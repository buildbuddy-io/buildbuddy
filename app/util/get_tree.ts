import { cache } from "../../proto/cache_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";
import type { Cancelable, ExtendedBuildBuddyService } from "../service/rpc_service";
import { CancelablePromise } from "./async";
import { streamWithRetry } from "./rpc";

type GetTreeService = Pick<ExtendedBuildBuddyService, "getTree">;

export type GetTreeRequest = Omit<cache.IGetTreeRequest, "requestContext">;
export type GetTreeBatchCallback = (directories: build.bazel.remote.execution.v2.Directory[]) => void;

/**
 * Fetches the directory tree, invoking the callback for each streamed batch
 * of directories.
 */
export function getTree(
  rpcService: GetTreeService,
  request: GetTreeRequest,
  onDirectoryBatch: GetTreeBatchCallback
): CancelablePromise<void> {
  let pageToken = request.pageToken ?? "";
  let stream: Cancelable | undefined;

  return new CancelablePromise(
    new Promise((resolve, reject) => {
      stream = streamWithRetry(
        rpcService.getTree,
        () =>
          new cache.GetTreeRequest({
            ...request,
            pageToken,
          }),
        {
          next: (response) => {
            onDirectoryBatch(response.directories ?? []);
            // Resume from the last fully received page if the stream reconnects.
            pageToken = response.nextPageToken || pageToken;
          },
          error: reject,
          complete: () => {
            resolve();
          },
        }
      );
    }),
    {
      oncancelled: () => {
        stream?.cancel();
      },
    }
  );
}
