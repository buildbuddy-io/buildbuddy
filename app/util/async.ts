/**
 * Promise extended with a `cancel()` method that effectively unregisters all callback
 * functions in the chain.
 */
export class CancelablePromise<T = unknown> implements Promise<T> {
  readonly [Symbol.toStringTag] = "CancelablePromise";

  private cancelled = false;
  private oncancelled?: () => void;

  /** The parent promise in the chain. */
  private parent: CancelablePromise | null = null;

  constructor(
    private promise: PromiseLike<T>,
    { oncancelled = undefined }: { oncancelled?: () => void } = {}
  ) {
    this.oncancelled = oncancelled;
  }

  then<U, V = never>(
    onfulfilled?: (value: T) => U | PromiseLike<U>,
    onrejected?: (reason: any) => V | PromiseLike<V>
  ): CancelablePromise<U | V> {
    const cancelable: CancelablePromise<U | V> = new CancelablePromise<U | V>(
      this.promise.then(
        onfulfilled
          ? (value: T) => {
              if (!cancelable.cancelled) return onfulfilled(value);
              return new Promise<U>(() => {});
            }
          : undefined,
        onrejected
          ? (reason: any) => {
              if (!cancelable.cancelled) return onrejected(reason);
              return new Promise<V>(() => {});
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
    this.oncancelled?.();
    if (this.parent) this.parent.cancel();
  }
}
