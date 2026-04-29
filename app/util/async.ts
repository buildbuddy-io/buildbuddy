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

/**
 * timeout returns a cancelable promise that calls the given function after a
 * given delay.
 */
export function timeout<T>(fn: () => T, ms: number = 0): CancelablePromise<T> {
  let timeout: ReturnType<typeof setTimeout> | undefined;
  let callback: (() => T) | undefined = fn;

  const cleanup = () => {
    if (timeout !== undefined) clearTimeout(timeout);
    timeout = undefined;
    // Drop the callback so values captured by fn can be GC'd.
    callback = undefined;
  };

  const promise = new Promise<T>((resolve, reject) => {
    timeout = setTimeout(() => {
      // If cancel() runs first, clearTimeout prevents this callback from
      // running, so the non-null assertion is safe here.
      const f = callback!;
      try {
        resolve(f());
      } catch (e) {
        reject(e);
      } finally {
        cleanup();
      }
    }, ms);
  });

  return new CancelablePromise(promise, {
    oncancelled: cleanup,
  });
}
