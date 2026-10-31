import type { ServerStreamHandler, ServerStreamingRpcMethod } from "../service/rpc_service";
import { FetchError } from "./errors";
import { streamWithRetry } from "./rpc";

describe("streamWithRetry", () => {
  let previousWindow: Window;

  beforeEach(() => {
    previousWindow = (globalThis as any).window;
    (globalThis as any).window = globalThis;
    jasmine.clock().install();
  });

  afterEach(() => {
    jasmine.clock().uninstall();
    (globalThis as any).window = previousWindow;
  });

  function streamingMethod(invoke: (handler: ServerStreamHandler<string>) => { cancel(): void }) {
    const method = jasmine
      .createSpy()
      .and.callFake((_request: {}, handler: ServerStreamHandler<string>) => invoke(handler));
    return { method: method as unknown as ServerStreamingRpcMethod<{}, string>, calls: method.calls };
  }

  function consumer() {
    return { next: jasmine.createSpy(), error: jasmine.createSpy(), complete: jasmine.createSpy() };
  }

  for (const { name, emitFailure } of [
    {
      name: "during abort",
      emitFailure: (handler: ServerStreamHandler<string>) => handler.error(new FetchError(new Error("aborted fetch"))),
    },
    {
      name: "after abort",
      emitFailure: (handler: ServerStreamHandler<string>) =>
        setTimeout(() => handler.error(new FetchError(new Error("aborted fetch"))), 1),
    },
  ]) {
    it(`does not reconnect when cancellation surfaces as a FetchError ${name}`, () => {
      const cancel = jasmine.createSpy();
      const { method, calls } = streamingMethod((handler) => {
        cancel.and.callFake(() => emitFailure(handler));
        return { cancel };
      });
      const handler = consumer();
      const stream = streamWithRetry(method, {}, handler, { retryDelayMs: () => 10 });
      stream.cancel();
      jasmine.clock().tick(1_000);
      expect(cancel).toHaveBeenCalledTimes(1);
      expect(calls.count()).toBe(1);
      expect(handler.error).not.toHaveBeenCalled();
    });
  }

  it("does not deliver late responses or terminal callbacks after cancellation", () => {
    let transport!: ServerStreamHandler<string>;
    const { method } = streamingMethod((handler) => {
      transport = handler;
      return { cancel: jasmine.createSpy() };
    });
    const handler = consumer();
    const stream = streamWithRetry(method, {}, handler);
    stream.cancel();
    transport.next("late response");
    transport.complete();
    transport.error(new Error("late terminal failure"));
    expect(handler.next).not.toHaveBeenCalled();
    expect(handler.complete).not.toHaveBeenCalled();
    expect(handler.error).not.toHaveBeenCalled();
  });

  it("does not execute a scheduled retry after cancellation", () => {
    let transport!: ServerStreamHandler<string>;
    const cancel = jasmine.createSpy();
    const { method, calls } = streamingMethod((handler) => {
      transport = handler;
      return { cancel };
    });
    const stream = streamWithRetry(method, {}, consumer(), { retryDelayMs: () => 10 });
    transport.error(new FetchError(new Error("connection interrupted")));
    stream.cancel();
    jasmine.clock().tick(1_000);
    expect(cancel).toHaveBeenCalledTimes(1);
    expect(calls.count()).toBe(1);
  });

  it("reconnects an active stream after a transient failure and delivers its response", () => {
    let transport!: ServerStreamHandler<string>;
    const { method, calls } = streamingMethod((handler) => {
      transport = handler;
      return { cancel: jasmine.createSpy() };
    });
    const handler = consumer();
    const stream = streamWithRetry(method, {}, handler, { retryDelayMs: () => 10 });
    transport.error(new FetchError(new Error("connection interrupted")));
    jasmine.clock().tick(10);
    expect(calls.count()).toBe(2);
    transport.next("retried response");
    transport.complete();
    expect(handler.next).toHaveBeenCalledWith("retried response");
    expect(handler.complete).toHaveBeenCalledTimes(1);
    expect(handler.error).not.toHaveBeenCalled();
    stream.cancel();
  });
});
