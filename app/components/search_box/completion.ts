/** A completion option for a typed token. */
export interface Completion {
  /** Replaces the token, e.g. "kind:deployment". */
  text: string;
  /** How many things have exactly this value, when known. */
  count?: number;
  /** A line about it, shown muted after the text. */
  detail?: string;
  /**
   * More may follow it in the same token, such as a value after a label's
   * key, so accepting it leaves the caret at its end without a space.
   */
  partial?: boolean;
}

/**
 * Produces completions for the token under the caret, up to the caret. It is
 * asked for an empty token too, on focus or after a space, which is the
 * place to offer the keys someone may not know about.
 */
export type Completer = (typed: string) => Promise<Completion[]>;

/** White-space-delimited token within a search string. */
export interface Token {
  start: number;
  end: number;
}

/**
 * Extracts the token in value at the given caret position as well as the partial text up to the caret.
 * The function looks backwards and forwards for the nearest whitespace or string bounds.
 */
export function tokenAt(value: string, caret: number): { token: Token; typed: string } {
  let start = caret;
  while (start > 0 && !/\s/.test(value[start - 1])) start--;
  let end = caret;
  while (end < value.length && !/\s/.test(value[end])) end++;
  return { token: { start, end }, typed: value.slice(start, caret) };
}

/** Applies the completion for the given token to the query text. */
export function apply(value: string, token: Token, c: Completion): { value: string; caret: number } {
  const completionText = c.text;
  // Text after the token, will be left as-is.
  const after = value.slice(token.end);
  const open = !!c.partial;
  // A finished completion is followed by a space. When one is already there
  // it is reused, and the caret stays before it, at the end of the completed
  // text, so typing on never runs into the next token.
  const space = open || /^\s/.test(after) ? "" : " ";
  return {
    value: value.slice(0, token.start) + completionText + space + after,
    caret: token.start + completionText.length + space.length,
  };
}

/** The first completion that extends what was typed, and what it adds. */
export function ghostFor(
  typed: string,
  completions: Completion[]
): { completion: Completion; rest: string } | undefined {
  if (!typed) return undefined;
  const lower = typed.toLowerCase();
  for (const completion of completions) {
    const text = completion.text;
    if (text.length > typed.length && text.toLowerCase().startsWith(lower)) {
      return { completion, rest: text.slice(typed.length) };
    }
  }
  return undefined;
}
