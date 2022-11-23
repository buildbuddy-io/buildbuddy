import { Octokit } from "@octokit/rest";

// Based on https://github.com/gr2m/octokit-plugin-create-pull-request with added functionality of updating PRs.
export async function createPullRequest(
  octokit: Octokit,
  { owner, repo, title, body, base, head, createWhenEmpty, changes: changesOption, draft = false }: Options
): Promise<any> {
  let response = await updatePullRequest(octokit, {
    owner,
    repo,
    base,
    head,
    createWhenEmpty,
    changes: changesOption,
    createRef: true,
  });

  // https://developer.github.com/v3/pulls/#create-a-pull-request
  return await octokit.request("POST /repos/{owner}/{repo}/pulls", {
    owner,
    repo,
    head: `${response.fork}:${head}`,
    base: response.base,
    title,
    body,
    draft,
  });
}

export async function updatePullRequest(
  octokit: Octokit,
  {
    owner,
    repo,
    base,
    head,
    createWhenEmpty,
    changes: changesOption,
    createRef,
  }: {
    owner: string;
    repo: string;
    base?: string;
    head: string;
    createWhenEmpty?: boolean;
    createRef?: boolean;
    changes: Changes | Changes[];
  }
): Promise<any> {
  const state: State = { octokit, owner, repo };

  const changes = Array.isArray(changesOption) ? changesOption : [changesOption];

  if (changes.length === 0) throw new Error('[octokit-plugin-create-pull-request] "changes" cannot be an empty array');

  // https://developer.github.com/v3/repos/#get-a-repository
  const { data: repository, headers } = await octokit.request("GET /repos/{owner}/{repo}", {
    owner,
    repo,
  });

  const isUser = !!headers["x-oauth-scopes"];

  if (!repository.permissions) {
    throw new Error("[octokit-plugin-create-pull-request] Missing authentication");
  }

  if (!base) {
    base = repository.default_branch;
  }

  state.fork = owner;

  if (isUser && !repository.permissions.push) {
    // https://developer.github.com/v3/users/#get-the-authenticated-user
    const user = await octokit.request("GET /user");

    // https://developer.github.com/v3/repos/forks/#list-forks
    const forks = await octokit.request("GET /repos/{owner}/{repo}/forks", {
      owner,
      repo,
    });
    const hasFork = forks.data.find(
      /* istanbul ignore next - fork owner can be null, but we don't test that */
      (fork) => fork.owner && fork.owner.login === user.data.login
    );

    if (!hasFork) {
      // https://developer.github.com/v3/repos/forks/#create-a-fork
      await octokit.request("POST /repos/{owner}/{repo}/forks", {
        owner,
        repo,
      });
    }

    state.fork = user.data.login;
  }

  // https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
  const {
    data: [latestCommit],
  } = await octokit.request("GET /repos/{owner}/{repo}/commits", {
    owner,
    repo,
    sha: base,
    per_page: 1,
  });
  state.latestCommitSha = latestCommit.sha;
  state.latestCommitTreeSha = latestCommit.commit.tree.sha;
  const baseCommitTreeSha = latestCommit.commit.tree.sha;

  for (const change of changes) {
    let treeCreated = false;
    if (change.files && Object.keys(change.files).length) {
      const latestCommitTreeSha = await createTree(state as Required<State>, change as Required<Changes>);

      if (latestCommitTreeSha) {
        state.latestCommitTreeSha = latestCommitTreeSha;
        treeCreated = true;
      }
    }

    if (treeCreated || change.emptyCommit !== false) {
      state.latestCommitSha = await createCommit(state as Required<State>, treeCreated, change);
    }
  }

  const hasNoChanges = baseCommitTreeSha === state.latestCommitTreeSha;
  if (hasNoChanges && createWhenEmpty === false) {
    return null;
  }

  if (!createRef) {
    // https://developer.github.com/v3/git/refs/#update-a-reference
    await octokit.request("PATCH /repos/{owner}/{repo}/git/refs/heads/{head}", {
      owner: state.fork,
      repo,
      sha: state.latestCommitSha,
      head: `${head}`,
      force: true,
    });

    return { fork: state.fork, base };
  }

  // https://developer.github.com/v3/git/refs/#create-a-reference
  await octokit.request("POST /repos/{owner}/{repo}/git/refs", {
    owner: state.fork,
    repo,
    sha: state.latestCommitSha,
    ref: `refs/heads/${head}`,
  });

  return { fork: state.fork, base };
}

export async function createCommit(state: Required<State>, treeCreated: boolean, changes: Changes): Promise<string> {
  const { octokit, repo, fork, latestCommitSha } = state;

  const message = treeCreated
    ? changes.commit
    : typeof changes.emptyCommit === "string"
    ? changes.emptyCommit
    : changes.commit;

  // https://developer.github.com/v3/git/commits/#create-a-commit
  const { data: latestCommit } = await octokit.request("POST /repos/{owner}/{repo}/git/commits", {
    owner: fork,
    repo,
    message,
    tree: state.latestCommitTreeSha,
    parents: [latestCommitSha],
  });

  return latestCommit.sha;
}

export async function createTree(state: Required<State>, changes: Required<Changes>): Promise<string | null> {
  const { octokit, owner, repo, fork, latestCommitSha, latestCommitTreeSha } = state;

  const tree = (
    await Promise.all(
      Object.keys(changes.files).map(async (path) => {
        const value = changes.files[path];

        if (value === null) {
          // Deleting a non-existent file from a tree leads to an "GitRPC::BadObjectState" error,
          // so we only attempt to delete the file if it exists.
          try {
            // https://developer.github.com/v3/repos/contents/#get-contents
            await octokit.request("HEAD /repos/{owner}/{repo}/contents/:path", {
              owner: fork,
              repo,
              ref: latestCommitSha,
              path,
            });

            return {
              path,
              mode: "100644",
              sha: null,
            };
          } catch (error) {
            return;
          }
        }

        // When passed a function, retrieve the content of the file, pass it
        // to the function, then return the result
        if (typeof value === "function") {
          let result;

          try {
            const { data: file } = await octokit.request("GET /repos/{owner}/{repo}/contents/:path", {
              owner: fork,
              repo,
              ref: latestCommitSha,
              path,
            });

            result = await value(Object.assign(file, { exists: true }) as UpdateFunctionFile);
          } catch (error) {
            // istanbul ignore if
            if (error.status !== 404) throw error;

            // @ts-ignore
            result = await value({ exists: false });
          }

          if (result === null || typeof result === "undefined") return;
          return valueToTreeObject(octokit, owner, repo, path, result);
        }

        return valueToTreeObject(octokit, owner, repo, path, value);
      })
    )
  ).filter(Boolean) as any;

  if (tree.length === 0) {
    return null;
  }

  // https://developer.github.com/v3/git/trees/#create-a-tree
  const {
    data: { sha: newTreeSha },
  } = await octokit.request("POST /repos/{owner}/{repo}/git/trees", {
    owner: fork,
    repo,
    base_tree: latestCommitTreeSha,
    tree,
  });

  return newTreeSha;
}

async function valueToTreeObject(octokit: Octokit, owner: string, repo: string, path: string, value: string | File) {
  let mode = "100644";
  if (value !== null && typeof value !== "string") {
    mode = value.mode || mode;
  }

  // Text files can be changed through the .content key
  if (typeof value === "string") {
    return {
      path,
      mode: mode,
      content: value,
    };
  }

  // Binary files need to be created first using the git blob API,
  // then changed by referencing in the .sha key
  const { data } = await octokit.request("POST /repos/{owner}/{repo}/git/blobs", {
    owner,
    repo,
    ...value,
  });
  const blobSha = data.sha;

  return {
    path,
    mode: mode,
    sha: blobSha,
  };
}

export type Options = {
  owner: string;
  repo: string;
  title: string;
  body: string;
  head: string;
  base?: string;
  createWhenEmpty?: boolean;
  changes: Changes | Changes[];
  draft?: boolean;
};

export type Changes = {
  files?: {
    [path: string]: string | File | UpdateFunction;
  };
  emptyCommit?: boolean | string;
  commit: string;
};

// https://developer.github.com/v3/git/blobs/#parameters
export type File = {
  content: string;
  encoding: "utf-8" | "base64";
  mode?: string;
};

export type UpdateFunctionFile =
  | {
      exists: true;
      size: number;
      encoding: "base64";
      content: string;
    }
  | {
      exists: false;
      size: never;
      encoding: never;
      content: never;
    };

export type UpdateFunction = (file: UpdateFunctionFile) => string | File | null;

export type State = {
  octokit: Octokit;
  owner: string;
  repo: string;
  fork?: string;
  latestCommitSha?: string;
  latestCommitTreeSha?: string;
  treeSha?: string;
};
