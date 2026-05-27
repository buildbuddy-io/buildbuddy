export function tryParseURL(s: string) {
  try {
    return new URL(s);
  } catch {
    return undefined;
  }
}
