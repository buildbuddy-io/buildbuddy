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

/** RepoURL represents a structured git repo URL. */
export class RepoURL {
  constructor(public host: string, public owner: string, public repo: string) {}

  static parse(url: string): RepoURL | undefined {
    try {
      const parsed = new URL(normalizeRepoURL(url));
      const [_empty, owner, repo, ..._ignoreRest] = parsed.pathname.split("/");
      if (!owner || !repo) {
        throw new Error("missing /owner/repo in path");
      }
      return new RepoURL(parsed.host, owner, repo);
    } catch (e) {
      return undefined;
    }
  }

  pullRequestLink(pullRequestNumber: number): string {
    return `${this.toString()}/pull/${pullRequestNumber}`;
  }

  /** toString returns the normalized repo URL. */
  toString(): string {
    return `https://${this.host}/${this.owner}/${this.repo}`;
  }
}
