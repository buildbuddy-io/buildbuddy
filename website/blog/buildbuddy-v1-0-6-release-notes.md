---
slug: buildbuddy-v1-0-6-release-notes
title: BuildBuddy v1.0.6 Release Notes
author: Siggi Simonarson
author_title: Co-founder @ BuildBuddy
date: 2020-05-20:12:00:00
author_url: https://www.linkedin.com/in/siggisim/
author_image_url: https://avatars.githubusercontent.com/u/1704556?v=4
tags: [product, release-notes]
---

Excited to share that v1.0.6 of BuildBuddy is live on both [Cloud Hosted BuildBuddy](https://app.buildbuddy.io/) and open source via [Github](https://github.com/buildbuddy-io/buildbuddy) and [Docker](https://github.com/buildbuddy-io/buildbuddy/blob/master/SETUP.md#docker-image)!

Thanks to all of you that have been using open source and cloud-hosted BuildBuddy. We've made lots of improvements in this release based on your feedback.

A special thank you to our new contributors:

- [Roger Hu](https://github.com/rogerhu) who contributed [Amazon S3 storage support](https://github.com/buildbuddy-io/buildbuddy/commit/8ba12398e448b457cdbd1e0c8913e9aba46323cb).
- [Andrew Allen](https://github.com/achew22) who [updated BuildBuddy's open source repo](https://github.com/buildbuddy-io/buildbuddy/commit/59bee5228c7c3da9d0cdaba934fce2118e7e9adc) to conform to open source golang expectations.

Our three major focuses for this release were on a better test results view, certificate based authentication, and our new results-store API.

We also laid a lot of groundwork for remote build execution in this release, which will be available in the coming weeks.

<!-- truncate -->

## New to Open Source BuildBuddy

- **Test results view** - we've added support for parsing test.xml files that are uploaded to a BuildBuddy remote cache. This allows us to show information about individual test cases and quickly surface information on which test cases failed and why.

- **Large log file support** - we've improved BuildBuddy's log viewer to enable the rendering of 100MB+ log files with full ANSI color support in milliseconds using incremental rendering.

- **Timing controls** - BuildBuddy's timing tab now has improved controls that enable users to choose grouping and page size options. This allows you to easily see the slowest build phases across threads.

- **gRPCS support** - BuildBuddy now supports and defaults to encrypted gRPCS connections to Bazel using TLS. Support includes automatic obtaining of server-side TLS certificates using [ACME](https://en.wikipedia.org/wiki/Automated_Certificate_Management_Environment) and [Let's Encrypt](https://letsencrypt.org/). This also includes the ability to connect to remote caches over gRPCS via the bytestream API.
- **URL secret redaction** - we've updated our log scrubbing logic to redact any URLs that might contain secrets from uploaded build events.

Our open source BuildBuddy distribution is targeted at individuals viewing and debugging their Bazel builds. For teams and organizations, we provide an enterprise version of BuildBuddy that adds support for team-specific features.

Many of these Enterprise features are also available for free to individuals via [Cloud Hosted BuildBuddy](https://app.buildbuddy.io/).

## New to Cloud & Enterprise BuildBuddy

- **Certificate based auth** - authentication between Bazel and BuildBuddy can now be authenticated and encrypted using certificate-based [mTLS](https://en.wikipedia.org/wiki/Mutual_authentication).
- **Auth configuration widget** - using BuildBuddy's new configuration widget, it's easy to setup an auth configuration that makes sense for your team. This includes options to pull credentials into user-specific `.bazelrc` files and download generated auth certificates.

- **Build Results API** - many teams want to do more with their build results. With BuildBuddy's [Build Results API](https://github.com/buildbuddy-io/buildbuddy/blob/master/proto/api/v1/service.proto) - users have programmatic access to an invocation's targets, actions, and build artifacts. This allows teams to build out custom integrations with their existing tooling. If you'd like access to the API, or have more information you'd like exposed, email [developers@buildbuddy.io](https://buildbuddy.io/blog/buildbuddy-v1-0-6-release-notes/developers@buildbuddy.io).

That's it for this release. Stay tuned for more updates coming soon!

As always, we love your feedback - email us at <hello@buildbuddy.io> with any questions, comments, or thoughts.
