export const READ_WRITE_GITHUB_APP_URL = "/auth/github/app/link/";
export const READ_ONLY_GITHUB_APP_URL = "/auth/github/read_only_app/link/";

// linkReadWriteGitHubAppURL is the URL to authorize against the read-write
// BuildBuddy GitHub app. This stores an oauth token for the user in our db.
export function linkReadWriteGitHubAppURL(userID: string, groupID: string): string {
  return `${READ_WRITE_GITHUB_APP_URL}?${new URLSearchParams({
    user_id: userID,
    group_id: groupID,
    redirect_url: window.location.href,
  })}`;
}

// installReadWriteGitHubAppURL is the URL to install the read-write
// BuildBuddy GitHub app.
export function installReadWriteGitHubAppURL(userID: string, groupID: string): string {
  return `${READ_WRITE_GITHUB_APP_URL}?${new URLSearchParams({
    user_id: userID,
    group_id: groupID,
    redirect_url: window.location.href,
    install: "true",
  })}`;
}

// linkReadOnlyGitHubAppURL is the URL to authorize against the read-only
// BuildBuddy GitHub app. This stores an oauth token for the user in our db.
export function linkReadOnlyGitHubAppURL(userID: string, groupID: string): string {
  return `${READ_ONLY_GITHUB_APP_URL}?${new URLSearchParams({
    user_id: userID,
    group_id: groupID,
    redirect_url: window.location.href,
  })}`;
}

// installReadOnlyGitHubAppURL is the URL to install the read-only
// BuildBuddy GitHub app.
export function installReadOnlyGitHubAppURL(userID: string, groupID: string): string {
  return `${READ_ONLY_GITHUB_APP_URL}?${new URLSearchParams({
    user_id: userID,
    group_id: groupID,
    redirect_url: window.location.href,
    install: "true",
  })}`;
}
