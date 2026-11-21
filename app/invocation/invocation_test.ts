import { config } from "../../proto/config_ts_proto";
import { eventlog } from "../../proto/eventlog_ts_proto";
import { execution_stats } from "../../proto/execution_stats_ts_proto";
import { google as grpc } from "../../proto/grpc_code_ts_proto";
import { build } from "../../proto/remote_execution_ts_proto";
import { invocation } from "../../proto/invocation_ts_proto";
import capabilities from "../capabilities/capabilities";
import rpcService, { CancelablePromise, ExtendedBuildBuddyService, ServerStreamHandler } from "../service/rpc_service";
import { BuildBuddyError } from "../util/errors";
import type InvocationComponent from "./invocation";
import InvocationModel from "./invocation_model";

const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
const response = (id?: string) =>
  new execution_stats.GetExecutionResponse({
    execution: id ? [new execution_stats.Execution({ executionId: id })] : [],
  });
const resolved = (id?: string) => new CancelablePromise(Promise.resolve(response(id)));

function pendingExecution() {
  let resolve!: (value: execution_stats.GetExecutionResponse) => void;
  const promise = new CancelablePromise<execution_stats.GetExecutionResponse>(new Promise((r) => (resolve = r)));
  return { promise, resolve };
}

function syntheticMissingResponse(message: string) {
  return execution_stats.WaitExecutionResponse.fromObject({
    operation: {
      done: true,
      response: {
        value: build.bazel.remote.execution.v2.ExecuteResponse.encode(
          build.bazel.remote.execution.v2.ExecuteResponse.fromObject({
            status: { code: grpc.rpc.Code.NOT_FOUND, message },
          })
        ).finish(),
      },
    },
  });
}

describe("InvocationComponent regional runner reads", () => {
  let Component: typeof import("./invocation").default;
  beforeAll(async () => {
    // One UI dependency registers a storage listener at import time.
    const previousWindow = (globalThis as any).window;
    const previousStorage = (globalThis as any).localStorage;
    (globalThis as any).window = { addEventListener: () => {} };
    (globalThis as any).localStorage = {};
    try {
      Component = (await import("./invocation")).default;
    } finally {
      (globalThis as any).window = previousWindow;
      (globalThis as any).localStorage = previousStorage;
    }
  });

  let previousWindow: Window;
  let previousRegions: config.Region[];
  let previousServices: typeof rpcService.regionalServices;
  let previousStreaming: boolean;
  let eu: ExtendedBuildBuddyService;
  let us: ExtendedBuildBuddyService;
  let components: InvocationComponent[];

  beforeEach(() => {
    previousWindow = (globalThis as any).window;
    (globalThis as any).window = {
      location: { origin: "https://test-org.buildbuddy.io", href: "https://test-org.buildbuddy.io/invocation/example" },
      setTimeout,
      clearTimeout,
    };
    previousRegions = capabilities.config.regions;
    previousServices = rpcService.regionalServices;
    previousStreaming = capabilities.config.streamingHttpEnabled;
    capabilities.config.streamingHttpEnabled = true;
    capabilities.config.regions = [
      new config.Region({ name: "US", server: "https://app.buildbuddy.io", subdomains: "https://*.buildbuddy.io" }),
      new config.Region({
        name: "Europe",
        server: "https://app.europe.buildbuddy.io",
        subdomains: "https://*.europe.buildbuddy.io",
      }),
    ];
    us = Object.create(rpcService.service);
    eu = Object.create(rpcService.service);
    us.getExecution = jasmine.createSpy().and.returnValue(resolved());
    eu.getExecution = jasmine.createSpy().and.returnValue(resolved("eu-execution"));
    spyOn(rpcService.service, "waitExecution").and.returnValue({ cancel: jasmine.createSpy() });
    eu.waitExecution = jasmine
      .createSpy()
      .and.returnValue({ cancel: jasmine.createSpy() }) as unknown as typeof eu.waitExecution;
    rpcService.regionalServices = new Map([
      ["US", us],
      ["Europe", eu],
    ]);
    components = [];
  });

  afterEach(() => {
    for (const component of components) component.componentWillUnmount();
    capabilities.config.regions = previousRegions;
    capabilities.config.streamingHttpEnabled = previousStreaming;
    rpcService.regionalServices = previousServices;
    (globalThis as any).window = previousWindow;
  });

  function component() {
    const c = new Component({
      invocationId: "example",
      tab: "",
      search: new URLSearchParams("queued=true"),
      preferences: {} as any,
    });
    // No DOM renderer is needed: apply state updates synchronously while keeping
    // the component's public fetch and streaming paths intact.
    spyOn(c, "setState").and.callFake((update: any, callback?: () => void) => {
      c.state = { ...c.state, ...(typeof update === "function" ? update(c.state, c.props) : update) };
      callback?.();
    });
    components.push(c);
    return c;
  }

  for (const { name, localResponse } of [
    { name: "empty", localResponse: () => resolved() },
    {
      name: "NotFound",
      localResponse: () => new CancelablePromise(Promise.reject(new BuildBuddyError("NotFound", "missing"))),
    },
  ]) {
    it(`discovers an EU runner after an ${name} local lookup and retains EU for streaming`, async () => {
      const local = spyOn(rpcService.service, "getExecution").and.callFake(localResponse);
      const c = component();
      await c.fetchRunnerExecution();
      expect(c.state.runnerExecution?.executionId).toBe("eu-execution");
      expect(local).toHaveBeenCalledTimes(1);
      // The canonical US service duplicates the current-origin lookup.
      expect(us.getExecution).not.toHaveBeenCalled();
      expect(eu.getExecution).toHaveBeenCalledTimes(1);
      c.streamRunnerExecution();
      expect(eu.waitExecution).toHaveBeenCalled();
      expect((eu.waitExecution as unknown as jasmine.Spy).calls.first().args[0].executionId).toBe("eu-execution");
    });
  }

  it("prefers a current-origin result without querying any other region", async () => {
    spyOn(rpcService.service, "getExecution").and.returnValue(resolved("local"));
    const c = component();
    await c.fetchRunnerExecution();
    expect(c.state.runnerExecution?.executionId).toBe("local");
    expect(eu.getExecution).not.toHaveBeenCalled();
    expect(us.getExecution).not.toHaveBeenCalled();
  });

  it("restarts a same-ID stream in the newly discovered region and ignores its old callbacks", async () => {
    spyOn(rpcService.service, "getExecution").and.returnValues(resolved("same-id"), resolved());
    (eu.getExecution as unknown as jasmine.Spy).and.returnValue(resolved("same-id"));
    const cancel = jasmine.createSpy();
    let oldHandler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
    (rpcService.service.waitExecution as unknown as jasmine.Spy).and.callFake((_request, handler) => {
      oldHandler = handler;
      return { cancel };
    });
    const c = component();
    await c.fetchRunnerExecution();
    c.streamRunnerExecution();
    const staleHandler = oldHandler;
    await c.fetchRunnerExecution();
    expect(cancel).toHaveBeenCalled();
    expect(eu.waitExecution).toHaveBeenCalledTimes(2);
    staleHandler.next(execution_stats.WaitExecutionResponse.fromObject({ operation: { done: true } }));
    expect(c.state.runnerLastExecuteOperation).toBeUndefined();
  });

  it("finds the stream owner despite globally shared metadata and cancels losing streams", async () => {
    spyOn(rpcService.service, "getExecution").and.returnValue(resolved("shared-id"));
    const localCancel = jasmine.createSpy();
    let localHandler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
    let euHandler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
    (rpcService.service.waitExecution as unknown as jasmine.Spy).and.callFake((_request, handler) => {
      localHandler = handler;
      return { cancel: localCancel };
    });
    (eu.waitExecution as unknown as jasmine.Spy).and.callFake((_request, handler) => {
      euHandler = handler;
      return { cancel: jasmine.createSpy() };
    });
    const c = component();
    await c.fetchRunnerExecution();
    c.streamRunnerExecution();
    euHandler.next(execution_stats.WaitExecutionResponse.fromObject({ operation: { done: false } }));
    expect(localCancel).toHaveBeenCalled();
    expect(c.state.runnerLastExecuteOperation?.done).toBe(false);
    localHandler.next(execution_stats.WaitExecutionResponse.fromObject({ operation: { done: true } }));
    expect(c.state.runnerLastExecuteOperation?.done).toBe(false);
    // Subsequent reads use the demonstrated owner, not the globally shared DB.
    (eu.getExecution as unknown as jasmine.Spy).and.returnValue(resolved("shared-id"));
    await c.fetchRunnerExecution();
    expect(eu.getExecution).toHaveBeenCalledTimes(1);
    expect(rpcService.service.getExecution).toHaveBeenCalledTimes(1);
  });

  it("does not elect a synthetic regional NOT_FOUND response as stream owner", async () => {
    spyOn(rpcService.service, "getExecution").and.returnValue(resolved("shared-id"));
    const localCancel = jasmine.createSpy();
    let localHandler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
    let euHandler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
    (rpcService.service.waitExecution as unknown as jasmine.Spy).and.callFake((_request, handler) => {
      localHandler = handler;
      return { cancel: localCancel };
    });
    (eu.waitExecution as unknown as jasmine.Spy).and.callFake((_request, handler) => {
      euHandler = handler;
      return { cancel: jasmine.createSpy() };
    });
    const c = component();
    await c.fetchRunnerExecution();
    c.streamRunnerExecution();
    localHandler.next(
      execution_stats.WaitExecutionResponse.fromObject({
        operation: {
          done: true,
          response: {
            value: build.bazel.remote.execution.v2.ExecuteResponse.encode(
              build.bazel.remote.execution.v2.ExecuteResponse.fromObject({
                status: { code: grpc.rpc.Code.NOT_FOUND, message: "receive execution update: missing channel" },
              })
            ).finish(),
          },
        },
      })
    );
    expect(localCancel).toHaveBeenCalled();
    expect(c.state.runnerLastExecuteOperation).toBeUndefined();
    expect(c.state.runnerExecution?.executeResponse).toBeNull();
    localHandler.next(execution_stats.WaitExecutionResponse.fromObject({ operation: { done: true } }));
    expect(c.state.runnerLastExecuteOperation).toBeUndefined();
    euHandler.next(execution_stats.WaitExecutionResponse.fromObject({ operation: { done: false } }));
    expect(c.state.runnerLastExecuteOperation?.done).toBe(false);
  });

  for (const { name, regionNames, message } of [
    {
      name: "non-regional deployment",
      regionNames: [],
      message: "receive execution update: local subscription failed",
    },
    { name: "single US region", regionNames: ["US"], message: "receive execution update: US subscription failed" },
  ]) {
    it(`shows a synthetic subscription failure with only one candidate in a ${name}`, async () => {
      capabilities.config.regions = capabilities.config.regions.filter((region) => regionNames.includes(region.name));
      rpcService.regionalServices = new Map(
        [...rpcService.regionalServices].filter(([name]) => regionNames.includes(name))
      );
      spyOn(rpcService.service, "getExecution").and.returnValue(resolved("runner"));
      let handler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
      (rpcService.service.waitExecution as unknown as jasmine.Spy).and.callFake((_request, h) => {
        handler = h;
        return { cancel: jasmine.createSpy() };
      });
      const c = component();
      await c.fetchRunnerExecution();
      c.streamRunnerExecution();
      expect(rpcService.service.waitExecution).toHaveBeenCalledTimes(1);
      expect(eu.waitExecution).not.toHaveBeenCalled();
      handler.next(syntheticMissingResponse(message));
      expect(c.state.runnerLastExecuteOperation?.done).toBe(true);
      expect(c.state.runnerExecution?.executeResponse?.status?.code).toBe(grpc.rpc.Code.NOT_FOUND);
      expect(c.state.runnerExecution?.executeResponse?.status?.message).toBe(message);
    });
  }

  it("shows the final synthetic failure when every US/Europe discovery stream fails without electing an owner", async () => {
    const localLookup = spyOn(rpcService.service, "getExecution").and.returnValue(resolved("shared-id"));
    const localCancel = jasmine.createSpy();
    const euCancel = jasmine.createSpy();
    let localHandler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
    let euHandler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
    (rpcService.service.waitExecution as unknown as jasmine.Spy).and.callFake((_request, h) => {
      localHandler = h;
      return { cancel: localCancel };
    });
    (eu.waitExecution as unknown as jasmine.Spy).and.callFake((_request, h) => {
      euHandler = h;
      return { cancel: euCancel };
    });
    const c = component();
    await c.fetchRunnerExecution();
    c.streamRunnerExecution();
    expect(rpcService.service.waitExecution).toHaveBeenCalledTimes(1);
    expect(eu.waitExecution).toHaveBeenCalledTimes(1);
    localHandler.next(syntheticMissingResponse("receive execution update: US subscription failed"));
    expect(c.state.runnerLastExecuteOperation).toBeUndefined();
    expect(localCancel).toHaveBeenCalled();
    const finalMessage = "receive execution update: EU subscription failed";
    euHandler.next(syntheticMissingResponse(finalMessage));
    expect(c.state.runnerLastExecuteOperation?.done).toBe(true);
    expect(c.state.runnerExecution?.executeResponse?.status?.message).toBe(finalMessage);
    expect(euCancel).toHaveBeenCalled();
    euHandler.next(execution_stats.WaitExecutionResponse.fromObject({ operation: { done: false } }));
    expect(c.state.runnerLastExecuteOperation?.done).toBe(true);
    // Exhaustion reports the failure but does not prove that EU owns the runner.
    await c.fetchRunnerExecution();
    expect(localLookup).toHaveBeenCalledTimes(2);
    expect(eu.getExecution).not.toHaveBeenCalled();
  });

  for (const { name, finish, expectedError } of [
    {
      name: "completes without an operation",
      finish: (handler: ServerStreamHandler<execution_stats.WaitExecutionResponse>) => handler.complete(),
      expectedError: undefined,
    },
    {
      name: "returns an RPC error",
      finish: (handler: ServerStreamHandler<execution_stats.WaitExecutionResponse>) =>
        handler.error(new BuildBuddyError("PermissionDenied", "regional runner access denied")),
      expectedError: "regional runner access denied",
    },
  ]) {
    it(`shows a suppressed subscription failure when the other region ${name}`, async () => {
      spyOn(rpcService.service, "getExecution").and.returnValue(resolved("shared-id"));
      const logged = spyOn(console, "error");
      let localHandler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
      let euHandler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
      (rpcService.service.waitExecution as unknown as jasmine.Spy).and.callFake((_request, h) => {
        localHandler = h;
        return { cancel: jasmine.createSpy() };
      });
      (eu.waitExecution as unknown as jasmine.Spy).and.callFake((_request, h) => {
        euHandler = h;
        return { cancel: jasmine.createSpy() };
      });
      const c = component();
      await c.fetchRunnerExecution();
      c.streamRunnerExecution();
      const message = "receive execution update: US subscription failed";
      localHandler.next(syntheticMissingResponse(message));
      expect(c.state.runnerLastExecuteOperation).toBeUndefined();
      finish(euHandler);
      expect(c.state.runnerExecution?.executeResponse?.status?.message).toBe(message);
      expect(logged.calls.mostRecent()?.args[1]?.description).toBe(expectedError);
    });
  }

  it("retains a genuine completed NOT_FOUND action failure during stream discovery", async () => {
    spyOn(rpcService.service, "getExecution").and.returnValue(resolved("failed-runner"));
    let handler!: ServerStreamHandler<execution_stats.WaitExecutionResponse>;
    (rpcService.service.waitExecution as unknown as jasmine.Spy).and.callFake((_request, h) => {
      handler = h;
      return { cancel: jasmine.createSpy() };
    });
    const losingCancel = jasmine.createSpy();
    (eu.waitExecution as unknown as jasmine.Spy).and.returnValue({ cancel: losingCancel });
    const c = component();
    await c.fetchRunnerExecution();
    c.streamRunnerExecution();
    handler.next(
      execution_stats.WaitExecutionResponse.fromObject({
        operation: {
          done: true,
          response: {
            value: build.bazel.remote.execution.v2.ExecuteResponse.encode(
              build.bazel.remote.execution.v2.ExecuteResponse.fromObject({
                status: { code: grpc.rpc.Code.NOT_FOUND, message: "missing action input" },
              })
            ).finish(),
          },
        },
      })
    );
    expect(c.state.runnerLastExecuteOperation?.done).toBe(true);
    expect(c.state.runnerExecution?.executeResponse?.status?.message).toBe("missing action input");
    expect(losingCancel).toHaveBeenCalled();
  });

  for (const code of ["PermissionDenied", "Unauthenticated", "Internal"] as const) {
    it(`does not hide a local ${code} failure behind cross-region discovery`, async () => {
      const error = new BuildBuddyError(code, "failed lookup");
      spyOn(rpcService.service, "getExecution").and.returnValue(new CancelablePromise(Promise.reject(error)));
      const logged = spyOn(console, "error");
      const c = component();
      await c.fetchRunnerExecution();
      expect(eu.getExecution).not.toHaveBeenCalled();
      expect(us.getExecution).not.toHaveBeenCalled();
      expect(logged).toHaveBeenCalledWith("Failed to fetch runner execution", error);
    });
  }

  it("preserves a regional error when no region returns a runner", async () => {
    spyOn(rpcService.service, "getExecution").and.returnValue(resolved());
    const error = new BuildBuddyError("PermissionDenied", "regional access denied");
    (eu.getExecution as unknown as jasmine.Spy).and.returnValue(new CancelablePromise(Promise.reject(error)));
    const logged = spyOn(console, "error");
    const c = component();
    await c.fetchRunnerExecution();
    expect(logged).toHaveBeenCalledWith("Failed to fetch runner execution", error);
    expect(c.state.runnerExecution).toBeUndefined();
  });

  for (const { name, endpoint, region, pageOrigin } of [
    {
      name: "global public",
      endpoint: "grpcs://remote.buildbuddy.io",
      region: "US",
      pageOrigin: "https://test-org.europe.buildbuddy.io",
    },
    {
      name: "global organization",
      endpoint: "grpcs://test-org.buildbuddy.io",
      region: "US",
      pageOrigin: "https://test-org.europe.buildbuddy.io",
    },
    {
      name: "Europe public",
      endpoint: "grpcs://remote.europe.buildbuddy.io",
      region: "Europe",
      pageOrigin: "https://test-org.buildbuddy.io",
    },
    {
      name: "Europe organization",
      endpoint: "grpcs://test-org.europe.buildbuddy.io",
      region: "Europe",
      pageOrigin: "https://test-org.buildbuddy.io",
    },
  ]) {
    it(`routes a known ${name} runner from the opposite region without discovery`, async () => {
      const local = spyOn(rpcService.service, "getExecution");
      (globalThis as any).window.location = { origin: pageOrigin, href: `${pageOrigin}/invocation/example` };
      const selected = region === "US" ? us : eu;
      const other = region === "US" ? eu : us;
      (selected.getExecution as unknown as jasmine.Spy).and.returnValue(resolved());
      const c = component();
      const model = new InvocationModel(invocation.Invocation.fromObject({ role: "CI_RUNNER" }));
      model.optionsMap.set("rbe_backend", endpoint);
      c.state = { ...c.state, model };
      await c.fetchRunnerExecution();
      expect(selected.getExecution).toHaveBeenCalledTimes(1);
      expect(other.getExecution).not.toHaveBeenCalled();
      expect(local).not.toHaveBeenCalled();
    });
  }

  it("fetches invocation cache statistics from the cache region rather than the BES region", async () => {
    spyOn(console, "log");
    const local = spyOn(rpcService.service, "getInvocation");
    us.getInvocation = jasmine.createSpy().and.returnValue(
      new CancelablePromise(
        Promise.resolve(
          new invocation.GetInvocationResponse({
            invocation: [invocation.Invocation.fromObject({ invocationId: "example" })],
          })
        )
      )
    );
    eu.getInvocation = jasmine.createSpy();
    const c = component();
    c.props.search.delete("queued");
    const model = new InvocationModel(invocation.Invocation.fromObject({}));
    model.optionsMap.set("remote_cache", "grpcs://test-org.buildbuddy.io");
    model.optionsMap.set("bes_backend", "grpcs://test-org.europe.buildbuddy.io");
    c.state = { ...c.state, model };
    await c.fetchInvocation();
    expect(us.getInvocation).toHaveBeenCalledTimes(1);
    expect(eu.getInvocation).not.toHaveBeenCalled();
    expect(local).not.toHaveBeenCalled();
  });

  for (const {
    name,
    besEndpoint,
    pageOrigin,
    pageRegion,
    otherRegion,
    endpointRegion,
    expectedCancellations,
    expectedOtherRequests,
    expectedAfterMetadataLogs,
    expectedFinalLogs,
  } of [
    {
      name: "initial metadata without a BES endpoint",
      besEndpoint: "",
      pageOrigin: "https://test-org.buildbuddy.io",
      pageRegion: "US",
      otherRegion: "Europe",
      endpointRegion: "",
      expectedCancellations: 0,
      expectedOtherRequests: 0,
      expectedAfterMetadataLogs: "persisted logs\n",
      expectedFinalLogs: "persisted logs\ntail logs\n",
    },
    {
      name: "compatibility custom BES endpoint",
      besEndpoint: "grpcs://bes.custom.example:443",
      pageOrigin: "https://test-org.buildbuddy.io",
      pageRegion: "US",
      otherRegion: "Europe",
      endpointRegion: "",
      expectedCancellations: 0,
      expectedOtherRequests: 0,
      expectedAfterMetadataLogs: "persisted logs\n",
      expectedFinalLogs: "persisted logs\ntail logs\n",
    },
    {
      name: "compatibility proxy BES endpoint",
      besEndpoint: "grpc://localhost:1985",
      pageOrigin: "https://test-org.europe.buildbuddy.io",
      pageRegion: "Europe",
      otherRegion: "US",
      endpointRegion: "",
      expectedCancellations: 0,
      expectedOtherRequests: 0,
      expectedAfterMetadataLogs: "persisted logs\n",
      expectedFinalLogs: "persisted logs\ntail logs\n",
    },
    {
      name: "recognized cross-region BES endpoint",
      besEndpoint: "grpcs://remote.europe.buildbuddy.io",
      pageOrigin: "https://test-org.buildbuddy.io",
      pageRegion: "US",
      otherRegion: "Europe",
      endpointRegion: "Europe",
      expectedCancellations: 1,
      expectedOtherRequests: 1,
      expectedAfterMetadataLogs: "persisted logs\nregional replay tail\n",
      expectedFinalLogs: "persisted logs\nregional replay tail\n",
    },
  ]) {
    it(`routes live logs correctly for ${name}`, async () => {
      const previousDocument = (globalThis as any).document;
      const previousLogStreaming = capabilities.config.invocationLogStreamingEnabled;
      (globalThis as any).document = { title: "", getElementById: () => null };
      capabilities.config.invocationLogStreamingEnabled = false;
      let resolveTail!: (response: eventlog.GetEventLogChunkResponse) => void;
      const tail = new CancelablePromise<eventlog.GetEventLogChunkResponse>(
        new Promise((resolve) => (resolveTail = resolve))
      );
      const canceled = spyOn(tail, "cancel").and.callThrough();
      (globalThis as any).window.location = { origin: pageOrigin, href: `${pageOrigin}/invocation/example` };
      const pageService = rpcService.regionalServices.get(pageRegion)!;
      const otherService = rpcService.regionalServices.get(otherRegion)!;
      expect(rpcService.getRegionalServiceOrDefault(pageOrigin)).toBe(pageService);
      expect(rpcService.getRegionalServiceOrDefault(besEndpoint)).toBe(
        endpointRegion ? rpcService.regionalServices.get(endpointRegion)! : rpcService.service
      );
      otherService.getEventLogChunk = jasmine.createSpy().and.returnValue(
        new CancelablePromise(
          Promise.resolve(
            new eventlog.GetEventLogChunkResponse({
              buffer: new TextEncoder().encode("persisted logs\nregional replay tail\n"),
              nextChunkId: "",
              live: false,
            })
          )
        )
      );
      pageService.getEventLogChunk = jasmine.createSpy().and.returnValues(
        new CancelablePromise(
          Promise.resolve(
            new eventlog.GetEventLogChunkResponse({
              buffer: new TextEncoder().encode("persisted logs\n"),
              nextChunkId: "tail",
              live: false,
            })
          )
        ),
        tail
      );
      const local = spyOn(rpcService.service, "getEventLogChunk");
      try {
        const c = component();
        c.props.search.delete("queued");
        spyOn(c, "fetchInvocation").and.returnValue(Promise.resolve());
        spyOn(c, "forceUpdate");
        c.componentWillMount();
        await flush();
        const prevState = c.state;
        const model = new InvocationModel(invocation.Invocation.fromObject({ hasChunkedEventLogs: true }));
        model.optionsMap.set("bes_backend", besEndpoint);
        c.state = { ...c.state, model };
        c.componentDidUpdate(c.props, prevState);
        await flush();
        expect(canceled).toHaveBeenCalledTimes(expectedCancellations);
        expect(pageService.getEventLogChunk).toHaveBeenCalledTimes(2);
        expect(otherService.getEventLogChunk).toHaveBeenCalledTimes(expectedOtherRequests);
        expect(local).not.toHaveBeenCalled();
        expect(c.getBuildLogs(model)).toBe(expectedAfterMetadataLogs);
        resolveTail(
          new eventlog.GetEventLogChunkResponse({
            buffer: new TextEncoder().encode("tail logs\n"),
            nextChunkId: "",
            live: false,
          })
        );
        await flush();
        expect(c.getBuildLogs(model)).toBe(expectedFinalLogs);
      } finally {
        capabilities.config.invocationLogStreamingEnabled = previousLogStreaming;
        (globalThis as any).document = previousDocument;
      }
    });
  }

  function cacheInvocation(hits: number) {
    return invocation.Invocation.fromObject({
      invocationId: "example",
      command: "build",
      cacheStats: { actionCacheHits: hits },
      structuredCommandLine: [
        {
          commandLineLabel: "canonical",
          sections: [
            {
              optionList: {
                option: [
                  { optionName: "remote_cache", optionValue: "grpcs://remote.europe.buildbuddy.io" },
                  { optionName: "bes_backend", optionValue: "grpcs://test-org.buildbuddy.io:443" },
                ],
              },
            },
          ],
        },
      ],
    });
  }

  it("falls back to shared invocation metadata when regional statistics fail and retries the region later", async () => {
    spyOn(console, "log");
    spyOn(console, "warn");
    let resolveFallback!: (response: invocation.GetInvocationResponse) => void;
    const fallback = new CancelablePromise<invocation.GetInvocationResponse>(
      new Promise((resolve) => (resolveFallback = resolve))
    );
    const local = spyOn(rpcService.service, "getInvocation").and.returnValues(
      new CancelablePromise(
        Promise.resolve(new invocation.GetInvocationResponse({ invocation: [cacheInvocation(1)] }))
      ),
      fallback
    );
    let regionalCalls = 0;
    eu.getInvocation = jasmine.createSpy().and.callFake(() => {
      regionalCalls++;
      if (regionalCalls === 1)
        return new CancelablePromise(Promise.reject(new BuildBuddyError("Unavailable", "region unavailable")));
      return new CancelablePromise(
        Promise.resolve(new invocation.GetInvocationResponse({ invocation: [cacheInvocation(10)] }))
      );
    });
    const other = (us.getInvocation = jasmine.createSpy());
    const c = component();
    c.props.search.delete("queued");
    await c.fetchInvocation();
    const initialModel = c.state.model;
    expect(initialModel?.invocation.cacheStats?.actionCacheHits?.toString()).toBe("1");
    expect(rpcService.getRegionalServiceOrDefault(initialModel!.getCacheEndpoint())).toBe(eu);
    expect(rpcService.getRegionalServiceOrDefault(initialModel!.getCacheEndpoint())).not.toBe(rpcService.service);
    const polling = c.fetchInvocation();
    await flush();
    expect(eu.getInvocation).toHaveBeenCalledTimes(1);
    expect(local).toHaveBeenCalledTimes(2);
    expect(c.state.model).toBe(initialModel);
    expect(c.state.error).toBeNull();
    resolveFallback(new invocation.GetInvocationResponse({ invocation: [cacheInvocation(3)] }));
    await polling;
    expect(c.state.error).toBeNull();
    expect(c.state.model?.invocation.cacheStats?.actionCacheHits?.toString()).toBe("3");
    await c.fetchInvocation();
    expect(eu.getInvocation).toHaveBeenCalledTimes(2);
    expect(local).toHaveBeenCalledTimes(2);
    expect(other).not.toHaveBeenCalled();
    expect(c.state.model?.invocation.cacheStats?.actionCacheHits?.toString()).toBe("10");
  });

  for (const code of ["NotFound", "PermissionDenied", "Unauthenticated"] as const) {
    it(`preserves an initial same-origin ${code} error without trying a regional service`, async () => {
      spyOn(console, "error");
      const error = new BuildBuddyError(code, "initial invocation lookup failed");
      const local = spyOn(rpcService.service, "getInvocation").and.callFake(
        () => new CancelablePromise(Promise.reject(error))
      );
      eu.getInvocation = jasmine.createSpy();
      us.getInvocation = jasmine.createSpy();
      const c = component();
      c.props.search.delete("queued");
      await c.fetchInvocation();
      expect(c.state.error).toBe(error);
      expect(c.state.model).toBeUndefined();
      expect(local).toHaveBeenCalledTimes(1);
      expect(eu.getInvocation).not.toHaveBeenCalled();
      expect(us.getInvocation).not.toHaveBeenCalled();
    });
  }

  it("preserves the default service error if a regional fallback also fails", async () => {
    spyOn(console, "error");
    spyOn(console, "warn");
    const error = new BuildBuddyError("PermissionDenied", "invocation access denied");
    spyOn(rpcService.service, "getInvocation").and.callFake(() => new CancelablePromise(Promise.reject(error)));
    eu.getInvocation = jasmine
      .createSpy()
      .and.callFake(
        () => new CancelablePromise(Promise.reject(new BuildBuddyError("Unavailable", "region unavailable")))
      );
    const c = component();
    c.props.search.delete("queued");
    c.state = { ...c.state, model: new InvocationModel(cacheInvocation(1)) };
    await c.fetchInvocation();
    expect(c.state.error).toBe(error);
    expect(eu.getInvocation).toHaveBeenCalledTimes(1);
    expect(rpcService.service.getInvocation).toHaveBeenCalledTimes(1);
  });

  it("ignores an older invocation result without stranding its waiting poll", async () => {
    spyOn(console, "log");
    let resolveOld!: (response: invocation.GetInvocationResponse) => void;
    const old = new CancelablePromise<invocation.GetInvocationResponse>(
      new Promise((resolve) => (resolveOld = resolve))
    );
    const latest = invocation.Invocation.fromObject({ invocationId: "example", command: "current" });
    spyOn(rpcService.service, "getInvocation").and.returnValues(
      old,
      new CancelablePromise(Promise.resolve(new invocation.GetInvocationResponse({ invocation: [latest] })))
    );
    let poll!: () => Promise<void>;
    (globalThis as any).window.setTimeout = jasmine.createSpy().and.callFake((callback: () => Promise<void>) => {
      poll = callback;
      return 1;
    });
    const c = component();
    spyOn(c, "fetchRunnerExecution").and.returnValue(new CancelablePromise(Promise.resolve()));
    c.scheduleRefetch();
    const waitingPoll = poll();
    await c.fetchInvocation();
    resolveOld(
      new invocation.GetInvocationResponse({ invocation: [invocation.Invocation.fromObject({ command: "stale" })] })
    );
    await waitingPoll;
    expect(c.state.model?.invocation.command).toBe("current");
    expect(c.state.loading).toBe(false);
  });

  it("cancels stale discovery, releases a waiting poll, and ignores its delayed response", async () => {
    const pending = pendingExecution();
    const cancel = spyOn(pending.promise, "cancel").and.callThrough();
    spyOn(rpcService.service, "getExecution").and.returnValues(pending.promise, resolved("current"));
    let poll!: () => Promise<void>;
    (globalThis as any).window.setTimeout = jasmine.createSpy().and.callFake((callback: () => Promise<void>) => {
      poll = callback;
      return 1;
    });
    const c = component();
    const fetch = spyOn(c, "fetchInvocation").and.returnValue(Promise.resolve());
    c.fetchRunnerExecution();
    c.scheduleRefetch();
    const waitingPoll = poll();
    await c.fetchRunnerExecution();
    await waitingPoll;
    expect(cancel).toHaveBeenCalled();
    expect(fetch).toHaveBeenCalled();
    pending.resolve(response("stale"));
    await flush();
    expect(c.state.runnerExecution?.executionId).toBe("current");
  });
});
