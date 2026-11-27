import Long from "long";
import React from "react";
import { invocation } from "../../proto/invocation_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";
import type UserPreferences from "../preferences/preferences";
import rpcService, { CancelablePromise } from "../service/rpc_service";
import type InvocationActionCardComponent from "./invocation_action_card";
import InvocationModel from "./invocation_model";

const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (error: Error) => void;
  const promise = new CancelablePromise<T>(
    new Promise((res, rej) => {
      resolve = res;
      reject = rej;
    })
  );
  return { promise, resolve, reject };
}

function elements(node: React.ReactNode): React.ReactElement<any>[] {
  const result: React.ReactElement<any>[] = [];
  React.Children.forEach(node, (child) => {
    if (!React.isValidElement(child)) return;
    result.push(child);
    result.push(...elements((child.props as any).children));
  });
  return result;
}

describe("InvocationActionCard regional execute response fetch", () => {
  let Component: typeof import("./invocation_action_card").default;

  beforeAll(async () => {
    // One UI dependency registers a storage listener at import time.
    const previousWindow = (globalThis as any).window;
    const previousStorage = (globalThis as any).localStorage;
    (globalThis as any).window = { addEventListener: () => {} };
    (globalThis as any).localStorage = {};
    try {
      Component = (await import("./invocation_action_card")).default;
    } finally {
      (globalThis as any).window = previousWindow;
      (globalThis as any).localStorage = previousStorage;
    }
  });

  for (const { name, executionHost } of [
    { name: "Europe public executor with a global remote cache", executionHost: "remote.europe.buildbuddy.io" },
    { name: "Europe organization executor with a global remote cache", executionHost: "test-org.europe.buildbuddy.io" },
  ]) {
    it(`fetches and displays the execute response from the ${name}`, async () => {
      const model = new InvocationModel(new invocation.Invocation({ invocationId: "invocation" }));
      model.optionsMap.set("remote_executor", `grpcs://${executionHost}`);
      model.optionsMap.set("remote_cache", "grpcs://remote.buildbuddy.io");
      const executeResponse = new build.bazel.remote.execution.v2.ExecuteResponse({
        result: new build.bazel.remote.execution.v2.ActionResult({ exitCode: 42 }),
      });
      // The persisted response is wrapped in ActionResult.stdout_raw, matching
      // Execution.execute_response_digest's wire format.
      const bytes = build.bazel.remote.execution.v2.ActionResult.encode({
        stdoutRaw: build.bazel.remote.execution.v2.ExecuteResponse.encode(executeResponse).finish(),
      }).finish();
      const responseHash = "b".repeat(64);
      const card: InvocationActionCardComponent = new Component({
        model,
        search: new URLSearchParams({
          actionDigest: `${"a".repeat(64)}/1`,
          executeResponseDigest: `${responseHash}/${bytes.length}`,
        }),
        preferences: {} as UserPreferences,
      });
      spyOn(card, "setState").and.callFake((update: any) => {
        card.state = { ...card.state, ...update };
      });
      const fetchFile = spyOn(rpcService, "fetchBytestreamFile").and.returnValue(
        new CancelablePromise(Promise.resolve(new Uint8Array(bytes).buffer))
      );

      card.fetchExecuteResponseOrActionResult({ streamFallback: false });
      await flush();

      expect(fetchFile).toHaveBeenCalledOnceWith(
        `actioncache://${executionHost}/blobs/ac/${responseHash}/${bytes.length}`,
        "invocation",
        "arraybuffer"
      );
      expect(fetchFile.calls.first().args[0]).not.toContain("actioncache://remote.buildbuddy.io/");
      expect(card.state.executeResponse?.result?.exitCode).toBe(42);
      expect(card.state.actionResult?.exitCode).toBe(42);
    });
  }

  it("keeps ordinary action-result fallback on the separately configured remote cache", async () => {
    const model = new InvocationModel(new invocation.Invocation({ invocationId: "invocation" }));
    model.optionsMap.set("remote_executor", "grpcs://test-org.europe.buildbuddy.io");
    model.optionsMap.set("remote_cache", "grpcs://remote.buildbuddy.io");
    const actionHash = "a".repeat(64);
    const card = new Component({
      model,
      search: new URLSearchParams({ actionDigest: `${actionHash}/1` }),
      preferences: {} as UserPreferences,
    });
    spyOn(card, "setState").and.callFake((update: any) => {
      card.state = { ...card.state, ...update };
    });
    const bytes = build.bazel.remote.execution.v2.ActionResult.encode({ exitCode: 7 }).finish();
    const fetchFile = spyOn(rpcService, "fetchBytestreamFile").and.returnValue(
      new CancelablePromise(Promise.resolve(new Uint8Array(bytes).buffer))
    );

    card.fetchExecuteResponseOrActionResult({ streamFallback: false });
    await flush();

    expect(fetchFile).toHaveBeenCalledOnceWith(
      `actioncache://remote.buildbuddy.io/blobs/ac/${actionHash}/1`,
      "invocation",
      "arraybuffer"
    );
    expect(card.state.actionResult?.exitCode).toBe(7);
    expect(card.state.executeResponse).toBeUndefined();
  });

  it("does not restore an old execute response when navigating to an ordinary cached action", async () => {
    const model = new InvocationModel(new invocation.Invocation({ invocationId: "invocation" }));
    model.optionsMap.set("remote_executor", "grpcs://test-org.europe.buildbuddy.io");
    model.optionsMap.set("remote_cache", "grpcs://remote.buildbuddy.io");
    const oldResponseBytes = build.bazel.remote.execution.v2.ActionResult.encode({
      stdoutRaw: build.bazel.remote.execution.v2.ExecuteResponse.encode({
        result: new build.bazel.remote.execution.v2.ActionResult({ exitCode: 7 }),
      }).finish(),
    }).finish();
    const newActionBytes = build.bazel.remote.execution.v2.ActionResult.encode({ exitCode: 0 }).finish();
    const card = new Component({
      model,
      search: new URLSearchParams({
        actionDigest: `${"a".repeat(64)}/1`,
        executeResponseDigest: `${"b".repeat(64)}/${oldResponseBytes.length}`,
      }),
      preferences: {} as UserPreferences,
    });
    spyOn(card, "setState").and.callFake((update: any) => {
      card.state = { ...card.state, ...update };
    });
    const fetchFile = spyOn(rpcService, "fetchBytestreamFile").and.returnValues(
      new CancelablePromise(Promise.resolve(new Uint8Array(oldResponseBytes).buffer)),
      new CancelablePromise(Promise.resolve(new Uint8Array(newActionBytes).buffer))
    );
    card.fetchExecuteResponseOrActionResult({ streamFallback: false });
    await flush();
    expect(card.state.executeResponse?.result?.exitCode).toBe(7);

    // Navigating between actions reuses the mounted action card. The previous
    // response is already settled when the new action takes the AC fallback.
    card.props.search.delete("executeResponseDigest");
    card.props.search.set("actionDigest", `${"c".repeat(64)}/1`);
    card.fetchExecuteResponseOrActionResult({ streamFallback: false });
    await flush();

    expect(fetchFile.calls.count()).toBe(2);
    expect(fetchFile.calls.mostRecent().args[0]).toBe(
      `actioncache://remote.buildbuddy.io/blobs/ac/${"c".repeat(64)}/1`
    );
    expect(card.state.executeResponse).toBeUndefined();
    expect(card.state.actionResult?.exitCode).toBe(0);
  });

  for (const { name, role, options, inputHost } of [
    {
      name: "workflow runner inputs use the execution backend rather than its Bazel cache",
      role: "CI_RUNNER",
      options: { rbe_backend: "grpcs://test-org.europe.buildbuddy.io", cache_backend: "grpcs://remote.buildbuddy.io" },
      inputHost: "test-org.europe.buildbuddy.io",
    },
    {
      name: "hosted runner inputs use the execution backend rather than its Bazel cache",
      role: "HOSTED_BAZEL",
      options: { rbe_backend: "grpcs://test-org.europe.buildbuddy.io", cache_backend: "grpcs://remote.buildbuddy.io" },
      inputHost: "test-org.europe.buildbuddy.io",
    },
    {
      name: "ordinary Bazel inputs use the separately configured remote cache",
      role: "",
      options: {
        remote_executor: "grpcs://test-org.europe.buildbuddy.io",
        remote_cache: "grpcs://remote.buildbuddy.io",
      },
      inputHost: "remote.buildbuddy.io",
    },
  ]) {
    it(name, async () => {
      const model = new InvocationModel(new invocation.Invocation({ invocationId: "invocation", role }));
      for (const [key, endpoint] of Object.entries(options)) {
        if (endpoint) model.optionsMap.set(key, endpoint);
      }
      const commandBytes = build.bazel.remote.execution.v2.Command.encode({ arguments: ["echo", "hello"] }).finish();
      const directoryBytes = build.bazel.remote.execution.v2.Directory.encode({}).finish();
      const commandDigest = new build.bazel.remote.execution.v2.Digest({
        hash: "d".repeat(64),
        sizeBytes: Long.fromNumber(commandBytes.length),
      });
      const inputRootDigest = new build.bazel.remote.execution.v2.Digest({
        hash: "e".repeat(64),
        sizeBytes: Long.fromNumber(directoryBytes.length),
      });
      const actionBytes = build.bazel.remote.execution.v2.Action.encode({ commandDigest, inputRootDigest }).finish();
      const actionHash = "a".repeat(64);
      const card = new Component({
        model,
        search: new URLSearchParams({ actionDigest: `${actionHash}/${actionBytes.length}` }),
        preferences: {} as UserPreferences,
      });
      spyOn(card, "setState").and.callFake((update: any, callback?: () => void) => {
        card.state = { ...card.state, ...update };
        callback?.();
      });
      spyOn(card, "fetchDirectorySizes");
      const fetchFile = spyOn(rpcService, "fetchBytestreamFile").and.returnValues(
        new CancelablePromise(Promise.resolve(new Uint8Array(actionBytes).buffer)),
        new CancelablePromise(Promise.resolve(new Uint8Array(commandBytes).buffer)),
        new CancelablePromise(Promise.resolve(new Uint8Array(directoryBytes).buffer))
      );

      card.fetchAction();
      await flush();

      expect(fetchFile.calls.allArgs()).toEqual([
        [`bytestream://${inputHost}/blobs/${actionHash}/${actionBytes.length}`, "invocation", "arraybuffer"],
        [`bytestream://${inputHost}/blobs/${commandDigest.hash}/${commandBytes.length}`, "invocation", "arraybuffer"],
        [
          `bytestream://${inputHost}/blobs/${inputRootDigest.hash}/${directoryBytes.length}`,
          "invocation",
          "arraybuffer",
        ],
      ]);
      expect(card.state.action?.commandDigest?.hash).toBe(commandDigest.hash);
      expect(card.state.command?.arguments).toEqual(["echo", "hello"]);
      expect(card.state.inputRoot?.files).toEqual([]);
      expect(card.state.loadingAction).toBe(false);
    });
  }

  for (const { name, role, options, artifactHost } of [
    {
      name: "workflow runner artifacts stay with its Europe execution backend",
      role: "CI_RUNNER",
      options: { rbe_backend: "grpcs://test-org.europe.buildbuddy.io", cache_backend: "grpcs://remote.buildbuddy.io" },
      artifactHost: "test-org.europe.buildbuddy.io",
    },
    {
      name: "hosted runner artifacts stay with its Europe execution backend",
      role: "HOSTED_BAZEL",
      options: { rbe_backend: "grpcs://test-org.europe.buildbuddy.io", cache_backend: "grpcs://remote.buildbuddy.io" },
      artifactHost: "test-org.europe.buildbuddy.io",
    },
    {
      name: "ordinary Bazel artifacts stay with its global remote cache",
      role: "",
      options: {
        remote_executor: "grpcs://test-org.europe.buildbuddy.io",
        remote_cache: "grpcs://remote.buildbuddy.io",
      },
      artifactHost: "remote.buildbuddy.io",
    },
  ]) {
    it(name, async () => {
      const model = new InvocationModel(new invocation.Invocation({ invocationId: "invocation", role }));
      for (const [key, endpoint] of Object.entries(options)) {
        if (endpoint) model.optionsMap.set(key, endpoint);
      }
      const digest = (hash: string, size: number) =>
        new build.bazel.remote.execution.v2.Digest({ hash: hash.repeat(64), sizeBytes: Long.fromNumber(size) });
      const stdoutDigest = digest("c", 6);
      const stderrDigest = digest("d", 6);
      const logDigest = digest("e", 3);
      const outputDigest = digest("f", 6);
      const outputFile = new build.bazel.remote.execution.v2.OutputFile({ path: "output.txt", digest: outputDigest });
      const response = new build.bazel.remote.execution.v2.ExecuteResponse({
        result: new build.bazel.remote.execution.v2.ActionResult({
          stdoutDigest,
          stderrDigest,
          outputFiles: [outputFile],
        }),
        serverLogs: { worker: new build.bazel.remote.execution.v2.LogFile({ digest: logDigest }) },
      });
      const responseBytes = build.bazel.remote.execution.v2.ActionResult.encode({
        stdoutRaw: build.bazel.remote.execution.v2.ExecuteResponse.encode(response).finish(),
      }).finish();
      const card = new Component({
        model,
        search: new URLSearchParams({
          actionDigest: `${"a".repeat(64)}/1`,
          executeResponseDigest: `${"b".repeat(64)}/${responseBytes.length}`,
        }),
        preferences: {} as UserPreferences,
      });
      spyOn(card, "setState").and.callFake((update: any) => {
        card.state = { ...card.state, ...update };
      });
      const fetchFile = spyOn(rpcService, "fetchBytestreamFile").and.returnValues(
        new CancelablePromise(Promise.resolve(new Uint8Array(responseBytes).buffer)),
        new CancelablePromise(Promise.resolve("stdout")),
        new CancelablePromise(Promise.resolve("stderr")),
        new CancelablePromise(Promise.resolve("log"))
      );
      const download = spyOn(rpcService, "downloadBytestreamFile");

      card.fetchExecuteResponseOrActionResult({ streamFallback: false });
      await flush();
      card.handleOutputFileClicked(card.state.actionResult!.outputFiles[0]);

      expect(fetchFile.calls.allArgs().slice(1)).toEqual([
        [`bytestream://${artifactHost}/blobs/${stdoutDigest.hash}/6`, "invocation"],
        [`bytestream://${artifactHost}/blobs/${stderrDigest.hash}/6`, "invocation"],
        [`bytestream://${artifactHost}/blobs/${logDigest.hash}/3`, "invocation"],
      ]);
      expect(card.state.stdout).toBe("stdout");
      expect(card.state.stderr).toBe("stderr");
      expect(card.state.serverLogs).toEqual([{ name: "worker", text: "log" }]);
      expect(download).toHaveBeenCalledOnceWith(
        "output.txt",
        `bytestream://${artifactHost}/blobs/${outputDigest.hash}/6`,
        "invocation"
      );
    });
  }

  for (const role of ["CI_RUNNER", "HOSTED_BAZEL"]) {
    it(`uses the execution region for ${role} AC fallback and copied replay command`, async () => {
      const model = new InvocationModel(new invocation.Invocation({ invocationId: "invocation", role }));
      model.optionsMap.set("rbe_backend", "grpcs://test-org.europe.buildbuddy.io");
      model.optionsMap.set("cache_backend", "grpcs://remote.buildbuddy.io");
      model.optionsMap.set("remote_executor", "grpcs://remote.buildbuddy.io");
      const actionHash = "a".repeat(64);
      const card = new Component({
        model,
        search: new URLSearchParams({ actionDigest: `${actionHash}/1` }),
        preferences: {} as UserPreferences,
      });
      spyOn(card, "setState").and.callFake((update: any) => {
        card.state = { ...card.state, ...update };
      });
      const bytes = build.bazel.remote.execution.v2.ActionResult.encode({ exitCode: 0 }).finish();
      const fetchFile = spyOn(rpcService, "fetchBytestreamFile").and.returnValue(
        new CancelablePromise(Promise.resolve(new Uint8Array(bytes).buffer))
      );
      card.fetchExecuteResponseOrActionResult({ streamFallback: false });
      await flush();
      expect(fetchFile).toHaveBeenCalledOnceWith(
        `actioncache://test-org.europe.buildbuddy.io/blobs/ac/${actionHash}/1`,
        "invocation",
        "arraybuffer"
      );
      card.state.action = new build.bazel.remote.execution.v2.Action();
      card.state.command = new build.bazel.remote.execution.v2.Command({ arguments: ["echo", "hello"] });
      card.state.loadingAction = false;
      const copyButton = elements(card.render()).find(
        (element) => element.props.className === "copy-bb-execute-button"
      );
      expect(copyButton).toBeDefined();
      const previousDocument = (globalThis as any).document;
      const textArea = { value: "", style: {}, focus: () => {}, select: () => {}, remove: () => {} };
      (globalThis as any).document = {
        createElement: () => textArea,
        body: { appendChild: () => {} },
        execCommand: () => true,
      };
      try {
        copyButton!.props.onClick();
      } finally {
        (globalThis as any).document = previousDocument;
      }
      expect(textArea.value).toContain("--remote_executor=grpcs://test-org.europe.buildbuddy.io");
      expect(textArea.value).not.toContain("--remote_executor=grpcs://remote.buildbuddy.io");
    });
  }

  it("ignores an old-region action response and completion while a replacement action is loading", async () => {
    const model = new InvocationModel(new invocation.Invocation({ invocationId: "invocation" }));
    model.optionsMap.set("remote_cache", "grpcs://remote.buildbuddy.io");
    const card = new Component({
      model,
      search: new URLSearchParams({ actionDigest: `${"a".repeat(64)}/1` }),
      preferences: {} as UserPreferences,
    });
    spyOn(card, "setState").and.callFake((update: any) => {
      card.state = { ...card.state, ...update };
    });
    const oldRead = deferred<ArrayBuffer>();
    const newRead = deferred<ArrayBuffer>();
    const fetchFile = spyOn(rpcService, "fetchBytestreamFile").and.returnValues(oldRead.promise, newRead.promise);
    spyOn(card, "fetchDirectorySizes");
    card.fetchAction();
    model.optionsMap.set("remote_cache", "grpcs://test-org.europe.buildbuddy.io");
    card.fetchAction();
    oldRead.resolve(new Uint8Array(build.bazel.remote.execution.v2.Action.encode({}).finish()).buffer);
    await flush();
    expect(card.state.action).toBeUndefined();
    expect(card.state.loadingAction).toBe(true);
    expect(fetchFile.calls.count()).toBe(2);
    newRead.reject(new Error("replacement action unavailable"));
    await flush();
    expect(card.state.loadingAction).toBe(false);
  });

  it("ignores old-region command, input root, and directory-size errors after replacement reads succeed", async () => {
    const model = new InvocationModel(new invocation.Invocation({ invocationId: "invocation" }));
    model.optionsMap.set("remote_cache", "grpcs://remote.buildbuddy.io");
    const card = new Component({
      model,
      search: new URLSearchParams({ actionDigest: `${"a".repeat(64)}/1` }),
      preferences: {} as UserPreferences,
    });
    spyOn(card, "setState").and.callFake((update: any) => {
      card.state = { ...card.state, ...update };
    });
    const oldCommand = deferred<ArrayBuffer>();
    const oldRoot = deferred<ArrayBuffer>();
    const oldSizes = deferred<any>();
    const commandDigest = new build.bazel.remote.execution.v2.Digest({ hash: "b".repeat(64), sizeBytes: Long.ONE });
    const rootDigest = new build.bazel.remote.execution.v2.Digest({ hash: "c".repeat(64), sizeBytes: Long.ONE });
    const action = new build.bazel.remote.execution.v2.Action({ commandDigest, inputRootDigest: rootDigest });
    const actionBytes = build.bazel.remote.execution.v2.Action.encode(action).finish();
    const newCommand = build.bazel.remote.execution.v2.Command.encode({ arguments: ["new"] }).finish();
    const newRoot = build.bazel.remote.execution.v2.Directory.encode({
      files: [new build.bazel.remote.execution.v2.FileNode({ name: "new-file" })],
    }).finish();
    const fetchFile = spyOn(rpcService, "fetchBytestreamFile").and.returnValues(
      new CancelablePromise(Promise.resolve(new Uint8Array(actionBytes).buffer)),
      oldCommand.promise,
      oldRoot.promise,
      new CancelablePromise(Promise.resolve(new Uint8Array(actionBytes).buffer)),
      new CancelablePromise(Promise.resolve(new Uint8Array(newCommand).buffer)),
      new CancelablePromise(Promise.resolve(new Uint8Array(newRoot).buffer))
    );
    const service = Object.create(rpcService.service);
    service.getTreeDirectorySizes = jasmine.createSpy().and.returnValues(
      oldSizes.promise,
      new CancelablePromise(
        Promise.resolve({
          sizes: [{ digest: "new-directory", totalSize: Long.fromNumber(10), childCount: Long.ONE }],
        })
      )
    );
    spyOn(rpcService, "getRegionalServiceOrDefault").and.returnValue(service);
    card.fetchAction();
    await flush();
    model.optionsMap.set("remote_cache", "grpcs://test-org.europe.buildbuddy.io");
    card.fetchAction();
    await flush();
    oldCommand.resolve(
      new Uint8Array(build.bazel.remote.execution.v2.Command.encode({ arguments: ["old"] }).finish()).buffer
    );
    oldRoot.resolve(new Uint8Array(build.bazel.remote.execution.v2.Directory.encode({}).finish()).buffer);
    oldSizes.reject(new Error("old region no longer owns input root"));
    await flush();
    expect(fetchFile.calls.count()).toBe(6);
    expect(card.state.command?.arguments).toEqual(["new"]);
    expect(card.state.inputRoot?.files.map((file) => file.name)).toEqual(["new-file"]);
    expect(card.state.treeShaToTotalSizeMap.get("new-directory")).toEqual([10, 1]);
  });

  it("refetches runner artifacts when its role arrives without changing recorded endpoints", () => {
    const modelForRole = (role: string) => {
      const model = new InvocationModel(new invocation.Invocation({ invocationId: "invocation", role }));
      model.optionsMap.set("rbe_backend", "grpcs://test-org.europe.buildbuddy.io");
      model.optionsMap.set("remote_executor", "grpcs://test-org.europe.buildbuddy.io");
      model.optionsMap.set("remote_cache", "grpcs://remote.buildbuddy.io");
      model.optionsMap.set("cache_backend", "grpcs://remote.buildbuddy.io");
      return model;
    };
    const previousModel = modelForRole("");
    const model = modelForRole("CI_RUNNER");
    expect(model.getCacheEndpoint()).toBe(previousModel.getCacheEndpoint());
    expect(model.getRemoteExecutorEndpoint()).toBe(previousModel.getRemoteExecutorEndpoint());
    const actionHash = "a".repeat(64);
    const card = new Component({
      model,
      search: new URLSearchParams({ actionDigest: `${actionHash}/1` }),
      preferences: {} as UserPreferences,
    });
    spyOn(card, "setState").and.callFake((update: any) => {
      card.state = { ...card.state, ...update };
    });
    spyOn(card, "fetchExecuteResponseOrActionResult");
    spyOn(card, "fetchSpawnMetrics");
    const fetchFile = spyOn(rpcService, "fetchBytestreamFile").and.returnValue(deferred<ArrayBuffer>().promise);
    card.componentDidUpdate({ ...card.props, model: previousModel }, card.state);
    expect(fetchFile).toHaveBeenCalledOnceWith(
      `bytestream://test-org.europe.buildbuddy.io/blobs/${actionHash}/1`,
      "invocation",
      "arraybuffer"
    );
    card.componentWillUnmount();
  });
});
