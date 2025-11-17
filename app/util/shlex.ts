// Keep in sync with server/util/shlex/shlex.go

const ALL_SAFE_CHARS_REGEXP = /^[A-Za-z0-9_:\-,.%@\/=]+$/;
const FLAG_ASSIGNMENT_REGEXP = /^--[A-Za-z0-9_:\-,.%@\/]+=/;

/**
 * Quote accepts a program argument and returns a string that can be passed as a
 * shell token which exactly represents the original argument. It avoids quoting
 * arguments which are safe to pass as-is to the shell. It treats flag values
 * specially by placing the quotes around the flag value (after the =).
 *
 * Example:
 *
 * ```js
 *   quote("foo") // returns "foo"
 *   quote("--path=C:\\Program Files") // returns "--path='C:\\Program Files'"
 *   quote("foo bar") // returns "'foo bar'"
 * ```
 */
export function quote(argument: string): string {
  if (ALL_SAFE_CHARS_REGEXP.test(argument)) {
    return argument;
  }
  // If we have a flag assignment like "--foo=bar baz" then keep the "--foo"
  // part unquoted so it renders more closely to how a human would enter it at
  // the command line.
  const prefix = FLAG_ASSIGNMENT_REGEXP.exec(argument)?.[0] ?? "";
  const suffix = argument.slice(prefix?.length ?? 0);
  return prefix + `'` + suffix.replace(/'/g, "'\\''") + `'`;
}
