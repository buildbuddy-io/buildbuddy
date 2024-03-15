---
id: cloud
title: Cloud Quickstart
sidebar_label: Cloud Quickstart
---

[Cloud BuildBuddy](https://app.buildbuddy.io/) is a fully managed SaaS solution for Enterprise Bazel features. It provides a results store & UI, remote build caching, remote build execution, and more.

It's easy to get set up and is free for individuals and open source projects. For companies, we offer an [Enterprise](enterprise.md) version of BuildBuddy that contains advanced features like OIDC Auth, API access, and more.

## Setup

To use BuildBuddy's Results UI, you just need to configure Bazel to send build events to our cloud BuildBuddy instance. The easiest way to do this is with a `.bazelrc` file in the root of your project.

```bash title=".bazelrc"
build --bes_results_url=https://app.buildbuddy.io/invocation/
build --bes_backend=grpcs://remote.buildbuddy.io
```

That's it, 2 lines and you're up and running. For more advanced configurations, see the [Authentication](#authentication) and [More features](#more-features) sections below.

## Verifying your installation

Now, when you build or test with Bazel, it will print a url where you can view your build or test results. For example:

```shellsession
tylerw@lunchbox:~/buildbuddy-io/buildbuddy$ bazel build server:all
INFO: Streaming build results to: https://app.buildbuddy.io/invocation/24a37b8f-4cf2-4909-9522-3cc91d2ebfc4
INFO: Analyzed 13 targets (0 packages loaded, 0 targets configured).
INFO: Found 13 targets...
INFO: Elapsed time: 0.212s, Critical Path: 0.01s
INFO: 0 processes.
INFO: Streaming build results to: https://app.buildbuddy.io/invocation/24a37b8f-4cf2-4909-9522-3cc91d2ebfc4
INFO: Build completed successfully, 1 total action
```

You can ⌘ + double-click on these urls to quickly view the invocation's details.

## Authentication

BuildBuddy Cloud offers three authentication options which are easy to configure on BuildBuddy Cloud:

- **Unauthenticated** - your build logs are uploaded over an encrypted gRPCS/TLS connection and be accessible with anyone you share your BuildBuddy URL with, without credentials. They will not be associated with any BuildBuddy account or organization.
- **API key based auth** - your build logs are uploaded over an encrypted gRPCS/TLS connection, and will be associated with your account. Only your account and members of your BuildBuddy organization will be able to view your build logs.
- **Certificate based auth** - your build logs are uploaded over an encrypted [mTLS](https://en.wikipedia.org/wiki/Mutual_authentication) (mutual TLS) connection. Only your account and members of your BuildBuddy organization will be able to view your build logs.

To configure one of these authentication methods:

1. [Create](https://app.buildbuddy.io/) a BuildBuddy account.
1. Visit the [Quickstart page](https://app.buildbuddy.io/docs/setup) which will now contain authentication options.

For more information see our [Authentication Guide](guide-auth.md).

## More features

For instructions on how to configure additional BuildBuddy features like Remote Build Caching, Remote Build Execution, and more:

1. [Create](https://app.buildbuddy.io/) a BuildBuddy account.
1. Visit the [Quickstart page](https://app.buildbuddy.io/docs/setup) which has instructions on enabling Remote Build Cache, Remote Build Execution, and more.

For more information on getting started with Remote Build Execution, see our [RBE setup docs](rbe-setup.md).
