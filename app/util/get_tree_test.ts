import Long from "long";
import { cache } from "../../proto/cache_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";
import { FetchError } from "./errors";
import { getTree } from "./get_tree";

describe("getTree", () => {
  let originalWindow: any;

  beforeEach(() => {
    originalWindow = (globalThis as any).window;
    (globalThis as any).window = globalThis;
    // streamWithRetry schedules reconnects with setTimeout, so use a fake
    // clock to trigger retries without sleeping in real time.
    jasmine.clock().install();
    // Retry backoff includes jitter; pin it so the retry delay is predictable.
    spyOn(Math, "random").and.returnValue(0.5);
  });

  afterEach(() => {
    jasmine.clock().uninstall();
    if (originalWindow) {
      (globalThis as any).window = originalWindow;
    } else {
      delete (globalThis as any).window;
    }
  });

  it("retries from the last streamed page token and aggregates the full tree", async () => {
    const rootDigest = new build.bazel.remote.execution.v2.Digest({
      hash: "root",
      sizeBytes: Long.fromNumber(1),
    });
    const firstDir = new build.bazel.remote.execution.v2.Directory({
      files: [new build.bazel.remote.execution.v2.FileNode({ name: "first.txt" })],
    });
    const secondDir = new build.bazel.remote.execution.v2.Directory({
      files: [new build.bazel.remote.execution.v2.FileNode({ name: "second.txt" })],
    });

    const requests: cache.GetTreeRequest[] = [];
    let callCount = 0;
    const getTreeRpc = (
      request: cache.IGetTreeRequest,
      handler: { next: Function; error: Function; complete: Function }
    ) => {
      // Capture every request so we can verify that retries resume from the
      // last emitted page token rather than restarting from the beginning.
      requests.push(new cache.GetTreeRequest(request));
      callCount++;

      if (callCount === 1) {
        // The first stream attempt delivers one page and then fails, forcing a
        // retry.
        handler.next(
          new cache.GetTreeResponse({
            directories: [firstDir],
            nextPageToken: "page-1",
          })
        );
        handler.error(new FetchError(new Error("transient")));
      } else {
        // The retried stream resumes from page-1 and completes the tree.
        handler.next(
          new cache.GetTreeResponse({
            directories: [secondDir],
          })
        );
        handler.complete();
      }

      return { cancel() {} };
    };
    Object.defineProperty(getTreeRpc, "name", { value: "GetTree" });
    Object.defineProperty(getTreeRpc, "serverStreaming", { value: true });

    const rpcService = {
      getTree: getTreeRpc,
    };

    const batches: build.bazel.remote.execution.v2.Directory[][] = [];
    const promise = getTree(
      rpcService as never,
      {
        rootDigest,
        instanceName: "remote",
      },
      (directories) => batches.push(directories)
    );

    // Advance to the first retry attempt's delay.
    jasmine.clock().tick(250);
    await promise;

    // We should reconnect from the last successfully streamed page and emit
    // each streamed batch to callers in order.
    expect(requests.map((request) => request.pageToken)).toEqual(["", "page-1"]);
    expect(batches).toEqual([[firstDir], [secondDir]]);
  });
});
