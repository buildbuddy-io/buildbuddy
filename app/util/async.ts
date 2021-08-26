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
