import { config } from "../../proto/config_ts_proto";
import { eventlog } from "../../proto/eventlog_ts_proto";
import capabilities from "../capabilities/capabilities";
import rpcService, { CancelablePromise, ExtendedBuildBuddyService } from "../service/rpc_service";
import InvocationLogsModel from "./invocation_logs_model";

const flush = () => new Promise<void>((resolve) => setTimeout(resolve, 0));
const chunk = (text: string, nextChunkId = "", live = false) =>
  new eventlog.GetEventLogChunkResponse({ buffer: new TextEncoder().encode(text), nextChunkId, live });

function pendingChunk() {
  let resolve!: (response: eventlog.GetEventLogChunkResponse) => void;
  const promise = new CancelablePromise<eventlog.GetEventLogChunkResponse>(
    new Promise((r) => {
      resolve = r;
    })
  );
  return { promise, resolve };
}

function regionalService() {
  return Object.create(rpcService.service) as ExtendedBuildBuddyService;
}

describe("InvocationLogsModel regional reads", () => {
  let previousWindow: Window;
  let previousRegions: config.Region[];
  let previousServices: typeof rpcService.regionalServices;
  let previousStreaming: boolean;
  let previousLogStreaming: boolean;
  let models: InvocationLogsModel[];

  beforeEach(() => {
    previousWindow = (globalThis as any).window;
    (globalThis as any).window = {
      location: { origin: "https://test-org.buildbuddy.io" },
      setTimeout,
      clearTimeout,
    };
    previousRegions = capabilities.config.regions;
    previousServices = rpcService.regionalServices;
    capabilities.config.regions = [];
    rpcService.regionalServices = new Map();
    previousStreaming = capabilities.config.streamingHttpEnabled;
    previousLogStreaming = capabilities.config.invocationLogStreamingEnabled;
    capabilities.config.streamingHttpEnabled = false;
    capabilities.config.invocationLogStreamingEnabled = false;
    models = [];
  });

  afterEach(() => {
    for (const model of models) model.stopFetching();
    capabilities.config.regions = previousRegions;
    rpcService.regionalServices = previousServices;
    capabilities.config.streamingHttpEnabled = previousStreaming;
    capabilities.config.invocationLogStreamingEnabled = previousLogStreaming;
    (globalThis as any).window = previousWindow;
  });

  function model(type = eventlog.LogType.BUILD_LOG) {
    const logs = new InvocationLogsModel("invocation", type);
    models.push(logs);
    return logs;
  }

  function configureRegions() {
    const us = regionalService();
    const eu = regionalService();
    capabilities.config.regions = [
      new config.Region({ name: "US", server: "https://app.buildbuddy.io", subdomains: "https://*.buildbuddy.io" }),
      new config.Region({
        name: "Europe",
        server: "https://app.europe.buildbuddy.io",
        subdomains: "https://*.europe.buildbuddy.io",
      }),
    ];
    rpcService.regionalServices = new Map([
      ["US", us],
      ["Europe", eu],
    ]);
    return { us, eu };
  }

  for (const { name, origin, endpoint, destination, otherDestination } of [
    {
      name: "US",
      origin: "https://test-org.buildbuddy.io",
      endpoint: "grpcs://test-org.buildbuddy.io:443",
      destination: "US",
      otherDestination: "Europe",
    },
    {
      name: "Europe",
      origin: "https://test-org.europe.buildbuddy.io",
      endpoint: "grpcs://test-org.europe.buildbuddy.io:443",
      destination: "Europe",
      otherDestination: "US",
    },
  ]) {
    it(`keeps the initial ${name} log stream when its same-region BES endpoint arrives`, async () => {
      configureRegions();
      (globalThis as any).window.location.origin = origin;
      const expected = rpcService.regionalServices.get(destination)!;
      const other = rpcService.regionalServices.get(otherDestination)!;
      const tail = pendingChunk();
      const canceled = spyOn(tail.promise, "cancel").and.callThrough();
      expected.getEventLogChunk = jasmine
        .createSpy()
        .and.returnValues(new CancelablePromise(Promise.resolve(chunk("persisted prefix\n", "tail"))), tail.promise);
      other.getEventLogChunk = jasmine.createSpy();
      const defaultGetChunk = spyOn(rpcService.service, "getEventLogChunk");
      const logs = model();
      logs.startFetching();
      await flush();
      const service = rpcService.getRegionalServiceOrDefault(endpoint);
      expect(service).toBe(expected);
      logs.setService(service);
      expect(canceled).not.toHaveBeenCalled();
      expect(expected.getEventLogChunk).toHaveBeenCalledTimes(2);
      expect(other.getEventLogChunk).not.toHaveBeenCalled();
      expect(defaultGetChunk).not.toHaveBeenCalled();
      expect(logs.getLogs()).toBe("persisted prefix\n");
      tail.resolve(chunk("regional tail\n"));
      await flush();
      expect(logs.getLogs()).toBe("persisted prefix\nregional tail\n");
    });
  }

  it("replays EU logs when a US org page learns an EU BES endpoint", async () => {
    const { us, eu } = configureRegions();
    const tail = pendingChunk();
    const canceled = spyOn(tail.promise, "cancel").and.callThrough();
    us.getEventLogChunk = jasmine
      .createSpy()
      .and.returnValues(new CancelablePromise(Promise.resolve(chunk("persisted prefix\n", "tail"))), tail.promise);
    eu.getEventLogChunk = jasmine
      .createSpy()
      .and.returnValue(new CancelablePromise(Promise.resolve(chunk("persisted prefix\nEU tail\n"))));
    const logs = model();
    logs.startFetching();
    await flush();
    const service = rpcService.getRegionalServiceOrDefault("grpcs://test-org.europe.buildbuddy.io:443");
    expect(service).toBe(eu);
    expect(service).not.toBe(us);
    logs.setService(service);
    await flush();
    expect(canceled).toHaveBeenCalled();
    expect(eu.getEventLogChunk).toHaveBeenCalledTimes(1);
    expect((eu.getEventLogChunk as jasmine.Spy).calls.first().args[0].chunkId).toBe("");
    expect(logs.getLogs()).toBe("persisted prefix\nEU tail\n");
    tail.resolve(chunk("abandoned US tail\n"));
    await flush();
    expect(logs.getLogs()).toBe("persisted prefix\nEU tail\n");
  });

  it("replays regional chunks without duplicating a previously persisted prefix", async () => {
    const localTail = pendingChunk();
    spyOn(rpcService.service, "getEventLogChunk").and.returnValues(
      new CancelablePromise(Promise.resolve(chunk("prefix\n", "tail"))),
      localTail.promise
    );
    const regional = regionalService();
    regional.getEventLogChunk = jasmine
      .createSpy()
      .and.returnValue(new CancelablePromise(Promise.resolve(chunk("prefix\nregional tail\n"))));
    const logs = model();
    logs.startFetching();
    await flush();
    expect(logs.getLogs()).toBe("prefix\n");
    logs.setService(regional);
    await flush();
    expect(logs.getLogs()).toBe("prefix\nregional tail\n");
    expect(logs.isComplete()).toBe(true);
    expect((regional.getEventLogChunk as jasmine.Spy).calls.first().args[0].chunkId).toBe("");
    localTail.resolve(chunk("stale tail\n"));
    await flush();
    expect(logs.getLogs()).toBe("prefix\nregional tail\n");
  });

  it("restarts in the BES region after the wrong region reported a completed log", async () => {
    spyOn(rpcService.service, "getEventLogChunk").and.returnValue(
      new CancelablePromise(Promise.resolve(chunk("persisted\n")))
    );
    const regional = regionalService();
    regional.getEventLogChunk = jasmine
      .createSpy()
      .and.returnValue(new CancelablePromise(Promise.resolve(chunk("persisted\nlive regional logs\n"))));
    const logs = model();
    logs.startFetching();
    await flush();
    expect(logs.isComplete()).toBe(true);
    logs.setService(regional);
    await flush();
    expect(logs.getLogs()).toBe("persisted\nlive regional logs\n");
  });

  it("cancels an active chunk request and ignores its delayed response when stopped", async () => {
    const pending = pendingChunk();
    const cancel = spyOn(pending.promise, "cancel").and.callThrough();
    spyOn(rpcService.service, "getEventLogChunk").and.returnValue(pending.promise);
    const logs = model();
    logs.startFetching();
    logs.stopFetching();
    expect(cancel).toHaveBeenCalled();
    pending.resolve(chunk("abandoned\n"));
    await flush();
    expect(logs.getLogs()).toBe("");
    expect(logs.isFetching()).toBe(false);
  });

  for (const { name, type } of [
    { name: "build", type: eventlog.LogType.BUILD_LOG },
    { name: "run", type: eventlog.LogType.RUN_LOG },
  ]) {
    it(`preserves ${name} log identity when selecting a regional service before fetching`, async () => {
      const local = spyOn(rpcService.service, "getEventLogChunk");
      const regional = regionalService();
      const getChunk = jasmine.createSpy().and.returnValue(new CancelablePromise(Promise.resolve(chunk("logs"))));
      regional.getEventLogChunk = getChunk;
      const logs = model(type);
      logs.setService(regional);
      expect(getChunk).not.toHaveBeenCalled();
      logs.startFetching();
      await flush();
      expect(local).not.toHaveBeenCalled();
      expect(getChunk.calls.first().args[0].invocationId).toBe("invocation");
      expect(getChunk.calls.first().args[0].type).toBe(type);
    });
  }

  it("cancels the old live stream and ignores callbacks from it after a region switch", () => {
    capabilities.config.streamingHttpEnabled = true;
    capabilities.config.invocationLogStreamingEnabled = true;
    let oldHandler!: Parameters<typeof rpcService.service.getEventLog>[1];
    const cancel = jasmine.createSpy();
    spyOn(rpcService.service, "getEventLog").and.callFake(((
      _request: Parameters<typeof rpcService.service.getEventLog>[0],
      handler: Parameters<typeof rpcService.service.getEventLog>[1]
    ) => {
      oldHandler = handler;
      return { cancel };
    }) as unknown as typeof rpcService.service.getEventLog);
    const regional = regionalService();
    let newHandler!: Parameters<typeof rpcService.service.getEventLog>[1];
    regional.getEventLog = jasmine.createSpy().and.callFake((_request, handler) => {
      newHandler = handler;
      return { cancel: jasmine.createSpy() };
    }) as unknown as typeof regional.getEventLog;
    const logs = model();
    logs.startFetching();
    oldHandler.next(chunk("old prefix\n", "tail"));
    logs.setService(regional);
    expect(cancel).toHaveBeenCalled();
    newHandler.next(chunk("regional\n"));
    oldHandler.next(chunk("abandoned\n"));
    expect(logs.getLogs()).toBe("regional\n");
  });
});
