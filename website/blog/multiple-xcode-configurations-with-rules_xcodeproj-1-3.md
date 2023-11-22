---
slug: multiple-xcode-configurations-with-rules_xcodeproj-1-3
title: "Multiple Xcode Configurations with rules_xcodeproj 1.3"
description: The one where we added a much requested, but surprisingly difficult
  to implement, feature.
author: Brentley Jones
author_title: "Developer Evangelist @ BuildBuddy"
date: 2023-03-17:12:00:00
author_url: https://brentleyjones.com
author_image_url: https://avatars.githubusercontent.com/u/158658?v=4
image: /img/rules_xcodeproj_1_3.png
tags: [rules_xcodeproj]
---

Today we released [version 1.3.2][version-1.3] of rules_xcodeproj!

This is a pretty exciting release, as it adds support for multiple Xcode
configurations (e.g. Debug and Release). Since early in rules_xcodeproj’s
development, being able to have more than the default Debug configuration has
been highly requested. We would have implemented support much sooner, but
because rules_xcodeproj accounts for every file path and compiler/linker flag,
in order to have rock solid indexing and debugging support, it wasn’t an easy
task.

[version-1.3]: https://github.com/buildbuddy-io/rules_xcodeproj/releases/tag/1.3.2

<!-- truncate -->

# The challenge

rules_xcodeproj uses a Bazel aspect to collect all of the information about your
build graph. It also uses Bazel split transitions in order to apply variations
of certain flags in order to support simulator and device builds in a single
project. It seems that it should have been pretty easy to extend this method to
apply to Xcode configurations as well, right? There were two problems to being
able to do that nicely, and we only really solved one of them at this time.

The common way that Bazel developers express various configurations is by
defining various configs in `.bazelrc` files, and then using the `--config`
stanza to select them. So, that brings us to our first problem: Bazel
transitions can’t transition on `--config`. Because of our nested invocation
architecture, we are able to apply a single `--config` to the inner invocation,
and we’ve had support for this for a while. Being able to transition on
`--config` would have allowed us to support multiple Xcode configurations a lot
sooner. Of note, in the solution we’ve implemented, you still can’t use
`--config`, and need to list out all the flags you want for each configuration.
This is because of this limitation of transitions.

For now we’ve decided to continue to use transitions, and wanted to extend our
approach to cover multiple configurations as well. That brought us to our second
problem: transitions are specified as part of a rule definition, and Bazel
macros can’t create anonymous rule. The easy approach to this would have been to
require users to define transitions in `.bzl` files (with the help of some
macros), and then reference them in their `xcodeproj` targets (which are
actually macros, not rules). This would go against one of our driving principles
of only needing a single `xcodeproj` target for all but the most complicated
setups, as we believe Xcode configurations are a fundamental aspect of projects
that everyone should be able to easily specify.

# The solution

The solution we implemented allows you to specify a dictionary of transition
settings in the `xcodeproj.xcode_configurations` attribute. Given the
previously mentioned limitations, you may be wondering how we were able to
accomplish this. Earlier I mentioned our nested invocation architecture, which
calls `bazel run` in `runner.sh` (the script that is invoked when you call
`bazel run //:xcodeproj`). We leverage this architecture to generate a Bazel
package in an external repository. This package contains a `BUILD` file with a
target using the actual `xcodeproj` rule, along with a `.bzl` file that defines
a custom transition containing information from
`xcodeproj.xcode_configurations`. And just like how a solution for a previous
feature was built upon to enable another feature (i.e. nested invocations which
enabled isolated build configurations, was built on for generated packages to
enable multiple Xcode configurations), we should be able to build on this
solution the same way (e.g. to enable automatic target discovery).

Here is an example of how you could specify Debug and Release configurations:

```python
xcodeproj(
    ...
    xcode_configurations = {
        "Debug": {
            "//command_line_option:compilation_mode": "dbg",
        },
        "Release": {
            "//command_line_option:compilation_mode": "opt",
        },
    },
    ...
)
```

We think the end result is a good starting point, but can be refined futher in
future releases. Please give it a try, and if you run into any problems
[file an issue][file-an-issue]! You can also join us in the `#rules_xcodeproj`
channel in the [Bazel Slack workspace][bazel-slack], and you can email us at
[hello@buildbuddy.io](mailto:hello@buildbuddy.io) with any questions, comments, or thoughts.

[bazel-slack]: https://slack.bazel.build/
[file-an-issue]: https://github.com/buildbuddy-io/rules_xcodeproj/issues/new/choose
[issues]: https://github.com/buildbuddy-io/rules_xcodeproj/issues
