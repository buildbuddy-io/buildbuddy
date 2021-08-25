/**
 * Promise extended with a `cancel()` method that effectively unregisters all callback
 * functions in the chain.
 */
export class CancelablePromise<T = unknown> implements Promise<T> {
  readonly [Symbol.toStringTag] = "CancelablePromise";

  private cancelled = false;

  /** The parent promise in the chain. */
  private parent: CancelablePromise;

  constructor(private promise: PromiseLike<T>) {}

  then<U, V = never>(
    onfulfilled: (value: T) => U | PromiseLike<U>,
    onrejected?: (reason: any) => V | PromiseLike<V>
  ): CancelablePromise<U | V> {
    const cancelable = new CancelablePromise<U | V>(
      this.promise.then(
        onfulfilled
          ? (value: T) => {
              if (!cancelable.cancelled) return onfulfilled(value);
            }
          : undefined,
        onrejected
          ? (reason: any) => {
              if (!cancelable.cancelled) return onrejected(reason);
            }
          : undefined
      )
    );
    cancelable.parent = this;
    return cancelable;
  }

  catch<U>(onrejected?: (reason: any) => U | PromiseLike<U>): CancelablePromise<T | U> {
    return this.then(undefined, onrejected);
  }

  finally(onfinally: () => void): CancelablePromise<T> {
    this.then(
      () => {
        if (!this.cancelled) onfinally();
      },
      () => {
        if (!this.cancelled) onfinally();
      }
    );
    return this;
  }

  /**
   * Cancels the entire promise chain from which this promise was derived, including all callbacks
   * registered via `then`, `catch`, or `finally`.
   */
  cancel(): void {
    this.cancelled = true;
    if (this.parent) this.parent.cancel();
  }
}

/**
 * Interface in which every method takes a single request parameter
 * and returns a promise wrapping the response.
 */
interface PromiseBasedService {
  [methodName: string]: (request: any) => Promise<any>;
}

/**
 * Extracts the type argument from a `Promise` type.
 *
 * For example, `PromiseTypeArg<Promise<string>>` returns `string`.
 */
type PromiseTypeArgument<T> = T extends Promise<infer U> ? U : never;

/**
 * Utility type that adapts a `PromiseBasedService` so that `CancelablePromise` is
 * returned from all methods, instead of `Promise`.
 */
export type CancelableService<Service extends PromiseBasedService> = {
  [MethodName in keyof Service]: (
    request: Parameters<Service[MethodName]>[0]
  ) => CancelablePromise<PromiseTypeArgument<ReturnType<Service[MethodName]>>>;
};
