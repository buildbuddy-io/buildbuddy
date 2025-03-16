export const READ_WRITE_GITHUB_APP_URL = "/auth/github/app/link/"
export const READ_ONLY_GITHUB_APP_URL = "/auth/github/read_only_app/link/"

// LinkReadWriteAppURL is the URL to authorize against the read-write
// BuildBuddy GitHub app. This stores an oauth token for the user in our db.
export function LinkReadWriteGitHubAppURL(userID: string, groupID: string): string {
    return `${READ_WRITE_GITHUB_APP_URL}?${new URLSearchParams({
        user_id: userID,
        group_id: groupID,
        redirect_url: window.location.href,
    })}`;
}

// LinkReadWriteAppURL is the URL to install the read-write
// BuildBuddy GitHub app.
export function InstallReadWriteGitHubAppURL(userID: string, groupID: string): string {
    return `${READ_WRITE_GITHUB_APP_URL}?${new URLSearchParams({
        user_id: userID,
        group_id: groupID,
        redirect_url: window.location.href,
        install: "true",
    })}`;
}

// LinkReadOnlyAppURL is the URL to authorize against the read-only
// BuildBuddy GitHub app. This stores an oauth token for the user in our db.
export function LinkReadOnlyGitHubAppURL(userID: string, groupID: string): string {
    return `${READ_ONLY_GITHUB_APP_URL}?${new URLSearchParams({
        user_id: userID,
        group_id: groupID,
        redirect_url: window.location.href,
    })}`;
}

// LinkReadOnlyAppURL is the URL to install the read-only
// BuildBuddy GitHub app.
export function InstallReadOnlyGitHubAppURL(userID: string, groupID: string): string {
    return `${READ_ONLY_GITHUB_APP_URL}?${new URLSearchParams({
        user_id: userID,
        group_id: groupID,
        redirect_url: window.location.href,
        install: "true",
    })}`;
}

