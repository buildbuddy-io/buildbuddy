---
slug: meet-rules_xcodeproj
title: Meet rules_xcodeproj
description: We are happy to announce that today we are releasing the first
  version of rules_xcodeproj, version 0.1.0.
author: Brentley Jones
author_title: Developer Evangelist @ BuildBuddy
date: 2022-04-05:08:00:00
author_url: https://brentleyjones.com
author_image_url: https://avatars.githubusercontent.com/u/158658?v=4
image: /img/meet_rules_xcodeproj.png
tags: [rules_xcodeproj]
---

[rules_xcodeproj][rules_xcodeproj] is a Bazel ruleset that can be used to
generate Xcode projects from targets in your workspace. We are happy to announce
that today we are releasing the first version of the ruleset,
[version 0.1.0][0.1.0].

[rules_xcodeproj]: https://github.com/buildbuddy-io/rules_xcodeproj
[0.1.0]: https://github.com/buildbuddy-io/rules_xcodeproj/releases/tag/0.1.0

<!-- truncate -->

<p align="center">
  <img src="/img/blog/rules_xcodeproj.png" height="256" />
</p>

There are already other tools out there that allow you to integrate Xcode with
Bazel. So you may be wondering why we decided to create another one. The core
reason is there wasn't one tool that satisfied all of these requirements:

- Fully supports all of Xcode's features
  - Indexing (i.e. autocomplete, syntax highlighting, jump to definition)
  - Debugging
  - Inline warnings and errors
  - Fix-its
  - Tests
  - SwiftUI Previews
- Supports building with Xcode (_not_ Bazel)
  - Useful for testing new Xcode features before Bazel and/or
    rules_apple/rules_swift supports it
  - An option if you don't want developers to build with Bazel quite yet
- Supports building with Bazel, in Xcode
  - While still fully supporting all of Xcode's features
  - Without needing an XCBBuildService proxy
- Supports all of the Core C/C++/Obj-C, rules_apple, and rules_swift rules
- Can be extended to support custom rules if needed
- Supports target discovery and focused projects
  - Target discovery means it can use a query system to find related targets,
    such as tests, without having to manually list them all
  - Focused projects allow Xcode to have only a portion of your build graph
    included in it, making Xcode perform better for large projects
- Produces Xcode projects that look and feel like normal Xcode projects

We've been working on rules_xcodeproj for two months, and while it doesn't
yet support all of the above, we have designs and a roadmap to cover all of
those requirements. Depending on the rules your project uses, or the attributes
of those rules, you should be able to generate an Xcode project that builds with
Xcode today, using version 0.1.0.

If you run into any problems with rules_xcodeproj, please check if
[another issue already exists][issues] and comment on it, and if not,
[file an issue][file-an-issue]! You can also email us at <hello@buildbuddy.io>
with any questions, comments, or thoughts.

[issues]: https://github.com/buildbuddy-io/rules_xcodeproj/issues
[file-an-issue]: https://github.com/buildbuddy-io/rules_xcodeproj/issues/new/choose
