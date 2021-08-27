import { CancelablePromise } from "./async";

function delayedResolve<T>(value: T = undefined, delayMs: number = 1): Promise<T> {
  return new Promise((resolve) => setTimeout(() => resolve(value), delayMs));
}

function delayedReject<T>(value: T = undefined, delayMs: number = 1): Promise<T> {
  return new Promise((_, reject) => setTimeout(() => reject(value), delayMs));
}

describe("CancelablePromise", () => {
  it("should allow getting the value from an already resolved promise", async () => {
    const value = await new CancelablePromise(Promise.resolve(1));
    expect(value).toBe(1);
  });

  it("should allow getting the value from an already rejected promise", async () => {
    let value, error;
    try {
      value = await new CancelablePromise(Promise.reject(1));
    } catch (e) {
      error = e;
    }
    expect(value).toBe(undefined);
    expect(error).toBe(1);
  });

  it("should allow getting the value from a promise that resolves later", async () => {
    const value = await new CancelablePromise(delayedResolve(1));
    expect(value).toBe(1);
  });

  it("should allow getting the value from a promise that rejects later", async () => {
    let value, error;
    try {
      value = await new CancelablePromise(delayedReject(1));
    } catch (e) {
      error = e;
    }
    expect(value).toBe(undefined);
    expect(error).toBe(1);
  });

  it("should allow chaining with an onfulfilled callback that returns an immediate value", async () => {
    const value = await new CancelablePromise(delayedResolve(1)).then(() => 2);
    expect(value).toBe(2);
  });

  it("should allow chaining with an onfulfilled callback that returns a promise", async () => {
    const value = await new CancelablePromise(delayedResolve(1)).then(() => Promise.resolve(2));
    expect(value).toBe(2);
  });

  it("should allow chaining with an onrejected callback that returns an immediate value", async () => {
    const value = await new CancelablePromise(delayedReject(1)).catch(() => 2);
    expect(value).toBe(2);
  });

  it("should allow chaining with an onrejected callback that returns a promise", async () => {
    const value = await new CancelablePromise(delayedReject(1)).catch(() => Promise.resolve(2));
    expect(value).toBe(2);
  });

  it("should allow registering an onfinally callback but still resolve with the original resolved value", async () => {
    const value = await new CancelablePromise(Promise.resolve(1)).finally(() => 2);
    expect(value).toBe(1);
  });

  it("should not fire onfulfilled callback if canceled", async () => {
    const promise = Promise.resolve(1);
    let value;
    const cancelable = new CancelablePromise(promise).then((v) => {
      value = v;
    });
    cancelable.cancel();
    await promise;
    await delayedResolve(); // Wait another tick for good measure.
    expect(value).toBe(undefined);
  });

  it("should not fire onrejected callback if canceled", async () => {
    const promise = Promise.reject(1);
    let error;
    const cancelable = new CancelablePromise(promise).catch((e) => {
      error = e;
    });
    cancelable.cancel();
    await promise.catch(() => {});
    await delayedResolve(); // Wait another tick for good measure.
    expect(error).toBe(undefined);
  });

  it("should not fire onfinally callback if canceled", async () => {
    const promise = Promise.resolve();
    let value;
    new CancelablePromise(promise)
      .finally(() => {
        value = 1;
      })
      .cancel();
    await promise;
    await delayedResolve(); // Wait another tick for good measure.
    expect(value).toBe(undefined);
  });

  it("should cancel all callbacks in a chain if canceled, not just the last one", async () => {
    const promise = Promise.resolve();
    let value1, value2;
    new CancelablePromise(promise)
      .then(() => {
        value1 = 1;
      })
      .then(() => {
        value2 = 2;
      })
      .cancel();
    await promise;
    await delayedResolve(); // Wait another tick for good measure.
    expect(value1).toBe(undefined);
    expect(value2).toBe(undefined);
  });

  it("should cancel all callbacks in a chain if canceled, not just the last one, even if the last one is an onfinally callback", async () => {
    const promise = Promise.resolve();
    let value1, value2;
    new CancelablePromise(promise)
      .then(() => {
        value1 = 1;
      })
      .finally(() => {
        value2 = 2;
      })
      .cancel();
    await promise;
    await delayedResolve(); // Wait another tick for good measure.
    expect(value1).toBe(undefined);
    expect(value2).toBe(undefined);
  });
});
