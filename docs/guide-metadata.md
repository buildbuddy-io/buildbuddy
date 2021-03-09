---
id: guide-metadata
title: Build Metadata Guide
sidebar_label: Build Metadata Guide
---

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

BuildBuddy will automatically pull your repo URL from environment variables if you're using a common CI platform like Github Actions, CircleCI, Travis, Jenkins, Gitlab CI, or BuildKite. The environment variables currently supported are `GITHUB_REPOSITORY`, `CIRCLE_REPOSITORY_URL`, `TRAVIS_REPO_SLUG`, `GIT_URL`, `CI_REPOSITORY_URL`, and `BUILDKITE_REPO`.

## Commit SHA

### Build metadata

You can provide the current commit SHA with Bazel's build_metadata flag with the key `COMMIT_SHA`. You can do this by adding the flag to your bazel invocations:

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

BuildBuddy will automatically pull your commit SHA from environment variables if you're using a common CI platform like Github Actions, CircleCI, Travis, Jenkins, Gitlab CI, or BuildKite. The environment variables currently supported are `GITHUB_SHA`, `CIRCLE_SHA1`, `TRAVIS_COMMIT`, `GIT_COMMIT`, `CI_COMMIT_SHA`, and `BUILDKITE_COMMIT`.

## Role

The role metadata field allows you to specify whether this invocation was done on behalf of a CI (continuous integration) system. If set, this enables features like Github commit status reporting (if a Github account is linked).

For CI builds, you can add the following line to your `.bazelrc` and run your CI builds with the `--config=ci` flag:

```
build:ci --build_metadata=ROLE=CI
```

This role will automatically be populated if the environment variable `CI` is set, which it is in most CI systems like Github Actions, CircleCI, Travis, Gitlab CI, BuildKite, and others.

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

## User

By default a build's user is determined by the system on which Bazel is run.

You can override this using build metadata or workspace info.

### Build metadata

You can provide your user with Bazel's build_metadata flag with the key `USER`. You can do this by adding the flag to your bazel invocations:

```
--build_metadata=USER=yourname
```

### Workspace info

The second method is a little more involved, but allows you to populate multiple pieces of metadata at once.

First, you'll need to point your `workspace_status_command` flag at a `workspace_status.sh` file at the root of your workspace. You can do this by adding the following line to your `.bazelrc`.

```
build --workspace_status_command=$(pwd)/workspace_status.sh
```

Then you'll need to add a `workspace_status.sh` file to the root of your workspace that prints `USER yourname`.

## Host

By default a build's host is determined by the system on which Bazel is run.

You can override this using build metadata or workspace info.

### Build metadata

You can provide your user with Bazel's build_metadata flag with the key `HOST`. You can do this by adding the flag to your bazel invocations:

```
--build_metadata=HOST=yourhost
```

### Workspace info

The second method is a little more involved, but allows you to populate multiple pieces of metadata at once.

First, you'll need to point your `workspace_status_command` flag at a `workspace_status.sh` file at the root of your workspace. You can do this by adding the following line to your `.bazelrc`.

```
build --workspace_status_command=$(pwd)/workspace_status.sh
```

Then you'll need to add a `workspace_status.sh` file to the root of your workspace that prints `HOST yourhost`.

### Custom Links

You can add custom links to the BuildBuddy overview page using the `BUILDBUDDY_LINKS` build metadata flag. These links must be comma separated, and in the form [link text](https://linkurl.com). Urls must begin with either `http://` or `https://`.

Example:
```
--build_metadata=BUILDBUDDY_LINKS="[Search Github](https://github.com/search),[GCP Dashboard](https://console.cloud.google.com/home/dashboard)"
```

## Environment variable redacting

By default, all environment variables are redacted by BuildBuddy except for `USER`, `GITHUB_ACTOR`, `GITHUB_REPOSITORY`, `GITHUB_SHA`, `GITHUB_RUN_ID` which are displayed in the BuildBuddy UI.

Redacted environment variables are displayed in the BuildBuddy UI as `<REDACTED>`.

You can optionally allow other environment variables to be displayed using the `ALLOW_ENV` metadata flag.

The `ALLOW_ENV` metadata param accepts a comma separated list of allowed environment variables and supports trailing wildcards.

### Examples

Don't redact the `PATH` environment variable:

```
build --build_metadata=ALLOW_ENV=PATH
```

Don't redact the `PATH` environment or any environment variable beginning with `BAZEL_`

```
build --build_metadata=ALLOW_ENV=PATH,BAZEL_*
```

Don't redact any environment variables.

```
build --build_metadata=ALLOW_ENV=*
```
