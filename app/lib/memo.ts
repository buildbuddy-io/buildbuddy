/**
 * Memoizes a 1-argument function.
 * @param fn
 */
export const memoize1 = <T, U>(fn: (arg: T) => U) => {
  const cache = new Map<T, U>();
  return (arg: T) => {
    if (!cache.has(arg)) {
      cache.set(arg, fn(arg));
    }
    return cache.get(arg);
  };
};
