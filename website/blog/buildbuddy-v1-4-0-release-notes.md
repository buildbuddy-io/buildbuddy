---
slug: buildbuddy-v1-4-0-release-notes
title: BuildBuddy v1.4.0 Release Notes
author: Siggi Simonarson
author_title: Co-founder @ BuildBuddy
date: 2020-11-12:12:00:00
author_url: https://www.linkedin.com/in/siggisim/
author_image_url: https://avatars.githubusercontent.com/u/1704556?v=4
tags: [product, release-notes]
---

We're excited to share that v1.4.0 of BuildBuddy is live on both [Cloud Hosted BuildBuddy](https://app.buildbuddy.io/) and open-source via [Github](https://github.com/buildbuddy-io/buildbuddy) and [Docker](https://github.com/buildbuddy-io/buildbuddy/blob/master/docs/on-prem.md#docker-image)!

Thanks to everyone using open source, cloud-hosted, and enterprise BuildBuddy. We've made lots of improvements in this release based on your feedback.

A special thank you to our new contributors who we'll soon be sending BuildBuddy t-shirts and holographic BuildBuddy stickers:

-   [**Daniel Purkhús**](https://github.com/purkhusid) who enabled environment variable expansion in BuildBuddy config files & more

-   [**Joshua Katz**](https://github.com/gravypod) who added support for auto-populating build metadata from GitLab CI invocations

Our focus for this release was on giving users new tools to share, compare, analyze, and manage BuildBuddy invocations - as well as major performance and reliability improvements to our remote build execution service.

We're also excited to share that over the coming weeks and months, we'll be open sourcing much more of BuildBuddy - including our remote build execution platform. At BuildBuddy we're firmly committed to open source and believe that a transparent and open model is the only way to build truly great developer infrastructure for all.

**New to Open Source BuildBuddy**
---------------------------------

-   **Invocation sharing & visibility controls** - while you've always been able to share BuildBuddy links with members of your organization, it's been difficult to share invocations more broadly (in GitHub issues or on StackOverflow). Now that working from home is the new norm, sharing links to your build logs or invocations details and artifacts has become more important than ever. To support this, we've added a **Share** button on the invocation page that allows you to control visibility of your invocations (this can be disabled at the organization level). We've also disabled the expiration of invocations and build logs for everyone on BuildBuddy Cloud - so you can share BuildBuddy links with confidence.

![](https://uploads-ssl.webflow.com/5eeba6a6c5230ea3d1a60d83/5fad7e7e02aaaabf670ca163_Screen%20Shot%202020-11-12%20at%2010.25.43%20AM.png)

-   **Invocation diffing** - we've all run into the problem where a build works on your machine, but not on your coworker's machine. To support debugging these kinds of issues, we've added the ability to diff builds straight from the invocations page. This allows you to quickly find any flags or invocation details that may have changed between builds. Stay tuned for more diffing features here, including cache hit debugging and more.

![](https://uploads-ssl.webflow.com/5eeba6a6c5230ea3d1a60d83/5fad7f9eb7782df7e6d2ec76_Screen%20Shot%202020-11-12%20at%2010.31.25%20AM.png)

-   **Suggested fixes** - as software engineers, we often find ourselves bumping into errors and issues that many others have bumped into before. A tool like BuildBuddy provides the perfect way to quickly surface these suggested fixes to developers quickly, without even so much as a Google search. We've started by adding suggestions for common issues that BuildBuddy users run into, but stay tuned for the ability to add your own custom fix suggestions and share them with your organization and beyond!

![](https://uploads-ssl.webflow.com/5eeba6a6c5230ea3d1a60d83/5fad86d1f58677c468250e2c_Screen%20Shot%202020-11-12%20at%2011.01.13%20AM.png)

-   **Easy invocation deletion** - you can now delete your BuildBuddy invocations directly from the invocation page "three dot" menu in case you want to share an invocation and delete it when you're done.

![](https://uploads-ssl.webflow.com/5eeba6a6c5230ea3d1a60d83/5fad85c7ae39b0100858c2b8_Screen%20Shot%202020-11-12%20at%2010.56.53%20AM.png)

New to Cloud & Enterprise BuildBuddy
------------------------------------

-   **Cache stats & filters** - our trends page now allows you to see trends in caching performance broken down by the Action Cache (AC) and the Content Addressable Store (CAS). The trends page is now also filterable by CI vs non-CI builds, and by user, repo, commit, or host.

![](https://uploads-ssl.webflow.com/5eeba6a6c5230ea3d1a60d83/5fad8533a1566a7bd6c70deb_Screen%20Shot%202020-11-06%20at%2011.54.19%20AM.png)

-   **Simplified API key header auth** - previously if you wanted to authenticate your BuildBuddy invocations using an API key (instead of using certificated based mTLS), you had to place your API key in each BuildBuddy flag that connected to BuildBuddy with YOUR_API_KEY@cloud.buildbuddy.io. This has been greatly simplified in this release with the support for the --remote_header flag, which allows you to more easily separate auth credentials into a separate .bazelrc file.

![](https://uploads-ssl.webflow.com/5eeba6a6c5230ea3d1a60d83/5fad8825c75ffe49d07e8b46_Screen%20Shot%202020-11-12%20at%2011.06.29%20AM.png)

-   **Organization creation and invitations** - you can now create organizations and send invitation links to others.

![](https://uploads-ssl.webflow.com/5eeba6a6c5230ea3d1a60d83/5fad85aabb83d90aa2813edf_Screen%20Shot%202020-11-12%20at%2010.57.26%20AM.png)

-   **Remote build execution performance and reliability improvements** - we've made a whole host of changes to our remote build execution executors and schedulers to make them more fault tolerant, easier to scale, and faster. We've also exposed support for executor pools on BuildBuddy Enterprise which allow you to route remote execution traffic based on OS, CPU architecture, GPU requirements, CPU/memory requirements, and more. Routing can be configured at both the platform and individual target level. Finally, we've added improved documentation to help get up and running with RBE more quickly.

That's it for this release. Stay tuned for more updates coming soon!

As always, we love your feedback - join our [Slack channel](https://slack.buildbuddy.io) or email us at <hello@buildbuddy.io> with any questions, comments, or thoughts.
