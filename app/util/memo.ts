export const memoizeSingleArgumentFunction = <T, U>(fn: (arg: T) => U): ((arg: T) => U) => {
  const cache = new Map<T, U>();
  return (arg: T): U => {
    if (!cache.has(arg)) {
      cache.set(arg, fn(arg));
    }
    return cache.get(arg)!;
  };
};
