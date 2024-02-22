import Long from "long";
import rpc_service from "../../../app/service/rpc_service";
import { github } from "../../../proto/github_ts_proto";

// Based on https://github.com/gr2m/octokit-plugin-create-pull-request with added functionality of updating PRs.
export async function createPullRequest({
  owner,
  repo,
  title,
  body,
  base,
  head,
  createWhenEmpty,
  changes: changesOption,
  draft = false,
}: Options): Promise<github.CreateGithubPullResponse> {
  let response = await updatePullRequest({
    owner,
    repo,
    base,
    head,
    createWhenEmpty,
    changes: changesOption,
    createRef: true,
  });

  // https://developer.github.com/v3/pulls/#create-a-pull-request
  return await rpc_service.service.createGithubPull(
    new github.CreateGithubPullRequest({
      owner,
      repo,
      head: `${response.fork}:${head}`,
      base: response.base,
      title,
      body,
      draft,
    })
  );
}

export async function updatePullRequest({
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
}): Promise<any> {
  const state: State = { owner, repo };

  const changes = Array.isArray(changesOption) ? changesOption : [changesOption];

  if (changes.length === 0) throw new Error('[code_pull_request.ts] "changes" cannot be an empty array');

  // https://developer.github.com/v3/repos/#get-a-repository
  const repository = await rpc_service.service.getGithubRepo(
    new github.GetGithubRepoRequest({
      owner,
      repo,
    })
  );

  if (!repository.permissions) {
    throw new Error("[code_pull_request.ts] Missing authentication");
  }

  if (!base) {
    base = repository.defaultBranch;
  }

  state.fork = owner;

  if (!repository.permissions.push) {
    // https://developer.github.com/v3/users/#get-the-authenticated-user
    const user = await rpc_service.service.getGithubUser(new github.GetGithubUserRequest());

    // https://developer.github.com/v3/repos/forks/#list-forks
    const forks = await rpc_service.service.getGithubForks(
      new github.GetGithubForksRequest({
        owner,
        repo,
      })
    );
    const hasFork = forks.forks.find((fork) => fork.owner && fork.owner === user.login);

    if (!hasFork) {
      // https://developer.github.com/v3/repos/forks/#create-a-fork
      await rpc_service.service.createGithubFork(
        new github.CreateGithubForkRequest({
          owner,
          repo,
        })
      );
    }

    state.fork = user.login;
  }

  // https://developer.github.com/v3/repos/commits/#list-commits-on-a-repository
  const {
    commits: [latestCommit],
  } = await rpc_service.service.getGithubCommits(
    new github.GetGithubCommitsRequest({
      owner,
      repo,
      sha: base,
      perPage: new Long(1),
    })
  );
  state.latestCommitSha = latestCommit.sha;
  state.latestCommitTreeSha = latestCommit.treeSha;
  const baseCommitTreeSha = latestCommit.treeSha;

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
    await rpc_service.service.updateGithubRef(
      new github.UpdateGithubRefRequest({
        owner: state.fork,
        repo,
        sha: state.latestCommitSha,
        head: `refs/heads/${head}`,
        force: true,
      })
    );

    return { fork: state.fork, base };
  }

  // https://developer.github.com/v3/git/refs/#create-a-reference
  await rpc_service.service.createGithubRef(
    new github.CreateGithubRefRequest({
      owner: state.fork,
      repo,
      sha: state.latestCommitSha,
      ref: `refs/heads/${head}`,
    })
  );

  return { fork: state.fork, base };
}

export async function createCommit(state: Required<State>, treeCreated: boolean, changes: Changes): Promise<string> {
  const { repo, fork, latestCommitSha } = state;

  const message = treeCreated
    ? changes.commit
    : typeof changes.emptyCommit === "string"
    ? changes.emptyCommit
    : changes.commit;

  // https://developer.github.com/v3/git/commits/#create-a-commit
  const latestCommit = await rpc_service.service.createGithubCommit(
    new github.CreateGithubCommitRequest({
      owner: fork,
      repo,
      message,
      tree: state.latestCommitTreeSha,
      parents: [latestCommitSha],
    })
  );

  return latestCommit.sha;
}

export async function createTree(state: Required<State>, changes: Required<Changes>): Promise<string | null> {
  const { owner, repo, fork, latestCommitSha, latestCommitTreeSha } = state;

  const tree = (
    await Promise.all(
      Object.keys(changes.files).map(async (path) => {
        const value = changes.files[path];

        if (value === null) {
          // Deleting a non-existent file from a tree leads to an "GitRPC::BadObjectState" error,
          // so we only attempt to delete the file if it exists.
          try {
            // https://developer.github.com/v3/repos/contents/#get-contents
            await rpc_service.service.getGithubContent(
              new github.GetGithubContentRequest({
                owner: fork,
                repo,
                ref: latestCommitSha,
                path,
                existenceOnly: true,
              })
            );

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
            const file = await rpc_service.service.getGithubContent(
              new github.GetGithubContentRequest({
                owner: fork,
                repo,
                ref: latestCommitSha,
                path,
              })
            );

            result = await value({
              exists: true,
              size: file.content.byteLength,
              content: file.content,
            });
          } catch (error: any) {
            if (error?.status !== 404) throw error;

            // @ts-ignore
            result = await value({ exists: false });
          }

          if (result === null || typeof result === "undefined") return;
          return valueToTreeObject(owner, repo, path, result);
        }

        return valueToTreeObject(owner, repo, path, value);
      })
    )
  ).filter(Boolean) as any;

  if (tree.length === 0) {
    return null;
  }

  // https://developer.github.com/v3/git/trees/#create-a-tree
  const { sha: newTreeSha } = await rpc_service.service.createGithubTree(
    new github.CreateGithubTreeRequest({
      owner: fork,
      repo,
      baseTree: latestCommitTreeSha,
      nodes: tree,
    })
  );

  return newTreeSha;
}

async function valueToTreeObject(owner: string, repo: string, path: string, value: string | File) {
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
  const response = await rpc_service.service.createGithubBlob(
    new github.CreateGithubBlobRequest({
      owner,
      repo,
      content: (value as File).content,
    })
  );
  const blobSha = response.sha;

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
    [path: string]: string | File | UpdateFunction | null;
  };
  emptyCommit?: boolean | string;
  commit: string;
};

// https://developer.github.com/v3/git/blobs/#parameters
export type File = {
  content: Uint8Array;
  mode?: string;
};

export type UpdateFunctionFile =
  | {
      exists: true;
      size: number;
      content: Uint8Array;
    }
  | {
      exists: false;
      size: never;
      content: never;
    };

export type UpdateFunction = (file: UpdateFunctionFile) => string | File | null;

export type State = {
  owner: string;
  repo: string;
  fork?: string;
  latestCommitSha?: string;
  latestCommitTreeSha?: string;
  treeSha?: string;
};
