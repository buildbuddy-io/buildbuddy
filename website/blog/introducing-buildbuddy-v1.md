---
slug: introducing-buildbuddy-v1
title: Introducing BuildBuddy Version 1.0
author: Siggi Simonarson
author_title: Co-founder @ BuildBuddy
date: 2020-04-24:12:00:00
author_url: https://www.linkedin.com/in/siggisim/
author_image_url: https://avatars.githubusercontent.com/u/1704556?v=4
tags: [product, release-notes]
---

We released our initial open source version of BuildBuddy to the Bazel community last month and have received a ton of interest, support, and feedback. We really appreciate everyone who's taken the time to kick the tires and try it out!

We're excited to share that BuildBuddy has been run on-prem at over 20 companies, and hundreds more developers have tried our cloud-hosted version.

People have found the shareable invocation links particularly useful when debugging builds remotely with co-workers while working from home. No more pasting console outputs into Pastebin!

We've taken all of the feedback we've gotten and made lots of improvements to both the open source and enterprise versions of BuildBuddy.

Our three major focuses for this release were on better build artifact handling, better test support, and enterprise authentication. We hope these changes help you continue to build and debug software faster. Keep the feedback coming!

<!-- truncate -->

## New to Open Source BuildBuddy

- **Remote cache support** - we've added a built-in Bazel remote cache to BuildBuddy, implementing the gRPC remote caching APIs. This allows BuildBuddy to optionally collect build artifacts, timing profile information, test logs, and more.

- **Clickable build artifacts** - this was our most requested feature. Clicking on build artifacts in the BuildBuddy UI now downloads the artifact when using either the built-in BuildBuddy cache, or a third-party cache running in gRPC mode that supports the Byte Stream API - like [bazel-remote](https://github.com/buchgr/bazel-remote).

- **Detailed timing information** - getting detailed timing information on your Bazel builds can be a hassle. Now BuildBuddy invocations include a new "Timing" tab - which pulls the Bazel profile logs from your build cache and displays them in a human-readable format. Stay tuned for flame charts!

- **Viewable test logs** - digging into test logs for your Bazel runs can be a pain. Now BuildBuddy surfaces test logs directly in the UI when you click on a test target (gRPC remote cache required).

- **Multiple test-run support** - one of our favorite features of Bazel is that it will rerun flaky tests for you. BuildBuddy now supports viewing information about multiple attempts of a single test run.

- **Client environment variable redaction** - client environment variables are now redacted from BuildBuddy's invocation details to avoid over-sharing.

- **Dense UI mode** - based on feedback on information density of the default BuildBuddy UI, we added a "Dense mode" that packs more information into every square inch.

- **BES backend multiplexing** - we heard from some of you that you'd like to try BuildBuddy, but were already pointing your bes_backend flag at another service. We've added the build_event_proxy configuration option that allows you to specify other backends that your build events should be forwarded to. See the [configuration docs](https://github.com/buildbuddy-io/buildbuddy/blob/master/CONFIG.md#buildeventproxy) for more information.

- **Slack webhook support** - we've added a configuration option that allows you to message a Slack channel when builds finish. This is a nice way of getting a quick notification when a long running build completes, or a CI build fails. See the [configuration docs](https://github.com/buildbuddy-io/buildbuddy/blob/master/CONFIG.md#integrations) for more information.

Our open source BuildBuddy distribution is targeted at individuals viewing and debugging their Bazel builds. For teams and organizations, we provide an enterprise version of BuildBuddy that adds support for team-specific features.

## New to Enterprise BuildBuddy

- **OpenID Connect auth support** - organizations can now specify an OpenID Connect provider to handle authentication for their BuildBuddy instance. This allows for the flexibility to use Google login if you use GSuite, auth services like Okta, or an in-house solution that supports OpenID Connect.

- **Authenticated build log & cache uploads** - BuildBuddy now supports generating authenticated upload URLs for both the build event and remote cache backends. Events uploaded with authentication will be associated with your organization and will not be viewable by unauthorized clients.

- **Organization support** - BuildBuddy now supports creating organizations that allow builds to be viewed and aggregated across your team/company.

- **Organization build history** - with organization support comes a new view that allows you to see recent builds across your organization.

- **User & host overviews** - you can now see all of the users and hosts that have uploaded builds to your organization. This allows you to drill into all of the builds uploaded from a CI machine for example.

- **Build grid** - the new build grid gives you a visual overview of the build history for an organization, host, or user. This allows you to quickly find and fix failing builds.

That's it for this release. Stay tuned for more updates coming soon!

As always, we love your feedback - email us at <hello@buildbuddy.io> with any questions, comments, or thoughts.
