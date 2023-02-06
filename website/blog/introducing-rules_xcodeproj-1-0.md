---
slug: introducing-rules_xcodeproj-1-0
title: "Introducing rules_xcodeproj 1.0"
description: How we got here and what’s next.
author: Brentley Jones
author_title: Developer Evangelist @ BuildBuddy
date: 2023-02-06:9:00:00
author_url: https://brentleyjones.com
author_image_url: https://avatars.githubusercontent.com/u/158658?v=4
image: /img/rules_xcodeproj_1_0.png
tags: [rules_xcodeproj]
---

Almost exactly one year ago I wrote the [first commit][first-commit] for
[rules_xcodeproj][rules_xcodeproj]. Like a lot of software engineers, I’m pretty
bad at estimating, and thought that I would be able to finish 1.0 in 2 to 4
months 😅. The longer development cycle was a result of an increased scope and
level of quality that I came to expect for a proper 1.0 release. Over the course
of the year, I believe the project has risen to meet my expectations, and today
I’m happy to announce the release of [version 1.0][version-1.0] of
rules_xcodeproj!

[first-commit]: https://github.com/buildbuddy-io/rules_xcodeproj/commit/0bb516569aee5dd49b004c89a761b5d186f25b15
[rules_xcodeproj]: https://github.com/buildbuddy-io/rules_xcodeproj
[version-1.0]: https://github.com/buildbuddy-io/rules_xcodeproj/releases/tag/1.0.1

<!-- truncate -->

<div align="center">
  <img alt="Screenshot of a rules_xcodeproj generated project open in Xcode" src="/img/blog/rules_xcodeproj_screenshot.png" width="1245" />
</div>

## The road to 1.0

The road to 1.0 has been an incredible journey. Early in the development cycle
Spotify, Robinhood, and Slack engineers became adopters and contributors;
without their help I wouldn’t be writing this blog post today 🙏.
[JP](https://github.com/jpsim) became a vocal champion of rules_xcodeproj after
integrating it with the SwiftLint and Envoy Mobile projects. During BazelCon
2022 the project got a couple shout-outs, including during
[Erik’s wonderful talk](https://youtu.be/wy3Q38VJ5uQ?t=1209). And I’m also
incredibly grateful that I was able to
[present rules_xcodeproj itself](https://youtu.be/B__SHnz3K3c) at the same
conference.

Deciding what was _actually_ important for the 1.0 release shifted throughout
the year. Some things that I initially thought were “nice to haves”, such as
framework targets and comprehensive ruleset support, became “blockers” for the
release. Other things that I was sure we would need before releasing, such as
multiple Xcode configurations (e.g. `Debug`, `Profile`, and `Release`),
diagnostic replaying, and BwX feature parity, ended up not being as important
(though don’t worry if you are wanting some of those things, we aren’t done
yet!).

The supported feature list also expanded throughout the year. It went from just
needing to support the Core C/C++/Objective-C, [rules_swift][rules_swift], and
[rules_apple][rules_apple] rules, to supporting nearly any ruleset. Also,
outside contributors made their mark: [Chuck](https://github.com/cgrindel) added
support for custom Xcode schemes; [Chirag](https://github.com/chiragramani)
added support for Runtime Sanitizers; [Maxwell](https://github.com/maxwellE)
added support for Launch, Profile, and Test action environment variables and
command-line arguments; [Thi](https://github.com/thii) contributed BwB mode
speed-ups and assisted with landing many more efficiency changes; and many
others helped me test changes on their varied and complex projects. Overall
these contributions, both in actual code and in invaluable time, allowed
rules_xcodepoj to be what it is today. Without this community effort the scope
and quality of the 1.0 release wouldn’t be at the same level.

[rules_apple]: https://github.com/bazelbuild/rules_apple
[rules_swift]: https://github.com/bazelbuild/rules_swift

## What’s next?

While the 1.0 release marks a certain level “doneness”, there is still a lot we
want to add and improve. When I
[announced rules_xcodeproj](meet-rules_xcodeproj.md), I listed a set of
requirements that other projects didn’t fulfill, and I stated that I wanted
rules_xcodeproj to cover all of them. I feel that with the 1.0 release we still
have a little ways to go to fully cover those requirements; though some
will have to be in spirit instead of to the letter, as Bazel and Xcode proved
to be more stubborn than I expected.

Here’s a list of changes that I hope we can implement in the near future:

- An additional build mode, currently called “Build with Bazel via Proxy”
  (BwBvP)
  - Uses [XCBBuildServiceProxyKit][xcbbuildserviceproxykit] to create an
    XCBBuildService proxy
  - Adds replaying of diagnostics, which removes duplicate errors/warnings,
    persists warnings, and enables Fix-Its
  - Produces a custom Build Report, removing noise from the logs, and fixing the
    progress bar
- BwB or BwBvP support for Xcode features that break with relative paths
  - Source code viewer in Instruments
  - Undefined Behavior Sanitizer navigation
  - Thread Performance Checker navigation
  - Memory Graph Analyzer with Malloc stack logging navigation
  - Inline code coverage
- Support for multiple Xcode configurations

[xcbbuildserviceproxykit]: https://github.com/MobileNativeFoundation/XCBBuildServiceProxyKit

## Thank you

Once again, I would like to thank all of the contributors and users we’ve gained
throughout the last year. Hopefully you are as proud of the 1.0 release as I am.
And for anyone reading that hasn’t tried rules_xcodeproj yet, or it didn’t meet
your requirements in an earlier release, I invite you to give it a shot now.

If you run into any problems with rules_xcodeproj, please check if
[another issue already exists][issues] and comment on it, and if not,
[file an issue][file-an-issue]! You can also join us in the `#rules_xcodeproj`
channel in the [Bazel Slack workspace][bazel-slack], and you can email us at
<hello@buildbuddy.io> with any questions, comments, or thoughts.

[bazel-slack]: https://slack.bazel.build/
[file-an-issue]: https://github.com/buildbuddy-io/rules_xcodeproj/issues/new/choose
[issues]: https://github.com/buildbuddy-io/rules_xcodeproj/issues
