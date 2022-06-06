/**
 * Returns a git URL like https://{domain}/{owner}/{repo}
 *
 * Some exceptions:
 * - file:// URLs are untouched
 * - localhost URLs are allowed to be http://
 */
export function normalizeRepoURL(url: string): string {
  if (!url) return url;
  url = url.trim();
  // Leave file:// URLs untouched
  if (url.startsWith("file://")) return url;
  url = url
    // Strip ssh:// prefix
    .replace(/^ssh:\/\//, "")
    // Replace git@github.com:foo/bar with github.com/foo/bar
    .replace(/^git@(.*?):\/?/, "$1/")
    // Strip .git suffix
    .replace(/.git$/, "");
  // Coerce https except for localhost
  if (!url.startsWith("https://") && !url.startsWith("http://localhost:")) {
    url = url.replace(/^(http:\/\/)?/, "https://");
  }
  // Strip USER[:PASS]
  url = url.replace(/^https?:\/\/(.*?)@/, "");
  return url;
}
