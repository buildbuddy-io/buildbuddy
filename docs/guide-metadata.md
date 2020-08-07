<!--
{
  "name": "Build Metadata Guide",
  "category": "5f18d20522eec65d44a3c1cd",
  "priority": 800
}
-->
# Build Metadata Guide

Additional metadata can be sent up with your Bazel invocation to give BuildBuddy more information about your build.

## Repository URL

BuildBuddy allows you to group invocations by the repository on which they were run. In order to perform this grouping, BuildBuddy needs the repository's git url. There are three ways of providing this information to BuildBuddy:

### Build metadata

The first method is simple - just provide the repo URL with Bazel's build_metadata flag with the key `REPO_URL`. You can do this by adding the following line to your `.bazelrc`:
```
build --build_metadata=REPO_URL=https://github.com/buildbuddy-io/buildbuddy.git
```

### Workspace info

The second method is a little more involved, but allows you to populate multiple pieces of metadata at once.

First, you'll need to point your `workspace_status_command` flag at a `workspace_status.sh` file at the root of your workspace. You can do this by adding the following line to your `.bazelrc`.
```
build --workspace_status_command=$(pwd)/workspace_status.sh
```

Then you'll need to add a `workspace_status.sh` file to the root of your workspace. You can copy the contents of [this one](https://github.com/buildbuddy-io/buildbuddy/blob/master/workspace_status.sh). This will populate your repo url, commit sha, and other build metadata automatically for every invocation.

### Environment variables

BuildBuddy will automatically pull your repo URL from environment variables if you're using a common CI platform like Github Actions or CircleCI. The environment variables currently supported are `GITHUB_REPOSITORY` and `CIRCLE_REPOSITORY_URL`.

## Commit SHA

### Build metadata

You can provide the currnet commit SHA with Bazel's build_metadata flag with the key `COMMIT_SHA`. You can do this by adding the flag to your bazel invocations:
```
--build_metadata=COMMIT_SHA=$(git rev-parse HEAD)
```

Note: you cannot add this particular flag to your `.bazelrc` file because it does not support parameter substitution. If you're looking for a solution that supports your `.bazelrc` file, try the Workspace info method below.

### Workspace info

The second method is a little more involved, but allows you to populate multiple pieces of metadata at once.

First, you'll need to point your `workspace_status_command` flag at a `workspace_status.sh` file at the root of your workspace. You can do this by adding the following line to your `.bazelrc`.
```
build --workspace_status_command=$(pwd)/workspace_status.sh
```

Then you'll need to add a `workspace_status.sh` file to the root of your workspace. You can copy the contents of [this one](https://github.com/buildbuddy-io/buildbuddy/blob/master/workspace_status.sh). This will populate your repo url, commit sha, and other build metadata automatically for every invocation.

### Environment variables

BuildBuddy will automatically pull your commit SHA from environment variables if you're using a common CI platform like Github Actions or CircleCI. The environment variables currently supported are `GITHUB_SHA` and `CIRCLE_SHA1`.

## Role

The role metadata field allows you to specify whether this invocation was done on behalf of a CI (continuous integration) system. If set, this enables features like Github commit status reporting (if a Github account is linked).

For CI builds, you can add the following line to your `.bazelrc` and run your CI builds with the `--config=ci` flag:
```
build:ci --build_metadata=ROLE=CI
```

## Test groups

If using Github commit status reporting, you can use the test groups metadata field to specify how tests are grouped in your Github commit statuses. Test groups are specified as a comma separated list of test path prefixes that should be grouped together.

```
build --build_metadata=TEST_GROUPS=//foo/bar,//foo/baz
```

## Visibility

The visibility metadata field determines who is allowed to view your build results. By default, unauthenticated builds are publicly visible to anyone with a link, while authenticated builds are only available to members of your organization.

You can override these default settings and make authenticated builds visible to anyone with a link by adding the following line to your `.bazelrc`:
```
build --build_metadata=VISIBILITY=PUBLIC
```