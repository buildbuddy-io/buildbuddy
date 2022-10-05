---
id: guide-auth
title: Authentication Guide
sidebar_label: Authentication Guide
---

BuildBuddy uses API keys to authenticate Bazel invocations. In order to authenticate an invocation, you must first [create a BuildBuddy account](https://app.buildbuddy.io/).

## Setup

An API key should be passed along with all gRPCs requests that you'd like to be associated with your BuildBuddy organization. This key can be used by anyone in your organization, as it ties builds to your org - not your individual user.

You can find your API key on your [Quickstart page](https://app.buildbuddy.io/docs/setup/) once you've [created an account](https://app.buildbuddy.io/) and logged in. You can also create multiple API keys for use in different contexts.

Your API key can be added directly to your `.bazelrc` as long as no one outside of your organization has access to your source code.

```
build --remote_header=x-buildbuddy-api-key=YOUR_API_KEY
```

If people outside of your organization have access to your source code (open source projects, etc) - you'll want to pull your credentials into a separate file that is only accessible by members of your organization and/or your CI machines.

Alternatively, you can store your API key in an environment variable / secret and pass these flags in manually or with a wrapper script.

## Separate auth file

Using the `try-import` directive in your `.bazelrc` - you can direct bazel to pull in additional bazel configuration flags from a different file if the file exists (if the file does not exist, this directive will be ignored).

You can then place a second `auth.bazelrc` file in a location that's only accessible to members of your organization:

```
build --remote_header=x-buildbuddy-api-key=YOUR_API_KEY
```

And add a `try-import` to your main `.bazelrc` file at the root of your `WORKSPACE`:

```
try-import /path/to/your/auth.bazelrc
```

## Command line

The command line method allows you to store your API key in an environment variable or Github secret, and then pass authenticated flags in either manually or with a wrapper script.

If using Github secrets - you can create a secret called `BUILDBUDDY_API_KEY` containing your API key, then use that in your actions:

```
bazel build --config=remote --remote_header=x-buildbuddy-api-key=${BUILDBUDDY_API_KEY}
```

## Managing keys

You can create multiple API keys on your [organization settings page](https://app.buildbuddy.dev/settings/org/api-keys). These keys can be used in different contexts (i.e. one for CI, one for developers) and cycled independently. Here you can also edit and delete existing API keys.

When creating multiple keys, we recommending labeling your API keys with descriptive names to describe how they will be used.

## Read only keys

When creating new API keys, you can check the box that says **Read-only key (disable remote cache uploads)**. This will allow users of these keys to download from the remote cache, but not upload artifacts into the cache.

## Executor keys

When creating API keys to link your self-hosted executors to your organization (if using **Bring Your Own Runners**), you'll need to check the box that says **Executor key (for self-hosted executors)**.
