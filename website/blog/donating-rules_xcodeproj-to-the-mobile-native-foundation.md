---
slug: donating-rules_xcodeproj-to-the-mobile-native-foundation
title: "Donating rules_xcodeproj to the Mobile Native Foundation"
description: We are solidifying the community ownership of rules_xcodeproj by
  donating it to the Mobile Native Foundation.
author: Brentley Jones
author_title: Developer Evangelist @ BuildBuddy
date: 2023-03-22:9:00:00
author_url: https://brentleyjones.com
author_image_url: https://avatars.githubusercontent.com/u/158658?v=4
image: /img/rules_xcodeproj_mnf.png
tags: [rules_xcodeproj]
---

Since the first commit of rules_xcodeproj, we’ve been committed to it being
community driven and community owned. Early in
[rules_xcodeproj][rules_xcodeproj]’s development, multiple companies started
using the project, while also putting forth significant contributions. And all
throughout its development we’ve solicited feedback from users on how the
project is working for them, not working for them, and what we should be
focusing on next.

Today we are taking the next step in this commitment; we are solidifying the
community ownership of rules_xcodeproj by donating it to the
[Mobile Native Foundation][mnf]!

[mnf]: https://mobilenativefoundation.org
[rules_xcodeproj]: https://github.com/MobileNativeFoundation/rules_xcodeproj

<!-- truncate -->

## Mobile Native Foundation

The Mobile Native Foundation (MNF) is a Linux Foundation project that provides a
place to collaborate on open source projects and discuss wide ranging topics in
order to improve processes and technologies for large-scale Android and iOS
applications. Some of the popular iOS projects under its banner include
[bluepill][bluepill], [index-import][index-import], and
[XCLogParser][xclogparser].

[bluepill]: https://github.com/MobileNativeFoundation/bluepill
[index-import]: https://github.com/MobileNativeFoundation/index-import
[xclogparser]: https://github.com/MobileNativeFoundation/XCLogParser

> We are thrilled to welcome rules_xcodeproj to the Mobile Native Foundation.
> BuildBuddy has done an exceptional job solving one of the longest running
> problems facing Apple developers trying to adopt Bazel. Through community
> ownership we hope to enable rules_xcodeproj to continue to flourish as it
> enables even more developers to benefit from Bazel’s advanced feature set.

&ndash; Keith Smiley, Chair @ Mobile Native Foundation

With this donation our dedication to rules_xcodeproj isn’t wavering; we are
going to continue to sponsor the project with both development hours and
leadership. As Keith mentioned, we believe that this change in ownership will be
an accelerant to the project, while also allowing adoption by users that were
blocked by the previous non-MNF ownership.

## User statements

We asked some users of rules_xcodeproj to comment on its donation to the MNF,
and this is what they had to say:

### Cash App

> Following the work done on rules_xcodeproj has been incredible, all that
> effort has led to a great Bazel + Xcode experience! At Cash App we’ve been
> using it to build Swift tooling for months now and have started using it to
> build the iOS app as we ship Bazel to all our engineers. The move to Mobile
> Native Foundation will mean an even larger audience which makes me feel great
> about the future of rules_xcodeproj.

&ndash; Luis Padron, Engineer @ Cash App

### Lyft

> rules_xcodeproj is a game-changer for apple platform development using Bazel.
> It was a key missing piece to bring custom native rules to SwiftLint.
>
> For Envoy Mobile, it has been invaluable in building and debugging our
> polyglot codebase, seamlessly stepping through call stacks involving a mix of
> Swift, Objective-C++, and C++.
>
> Overall, rules_xcodeproj has boosted our confidence in the reliability and
> flexibility of our tools, thanks to its active open-source community and
> responsive contributors.

&ndash; JP Simard, Staff Engineer @ Lyft

### Reddit

> rules_xcodeproj has made onboarding to Bazel for Apple related development all
> the easier. Finally consolidating many disparate project generation solutions
> to a modern one that the community has/can choose to use and support. The
> maintainers of the project are responsive and helpful in all respects related
> to community engagement and the donation to Mobile Native Foundation is just
> another gesture of goodwill to that community.

&ndash; Matt Robinson, Staff Engineer @ Reddit

### Robinhood

> The rules_xcodeproj project has been a huge accelerant and motivating factor
> for our iOS team’s migration to Bazel. I’ve previously seen first hand how
> tricky the Bazel + Xcode local experience can be to implement in-house and
> create a negative experience for developers; however, BuildBuddy’s expertise
> and stewardship of this project enabled us to immediately generate a
> functional Xcode project and focus our codebase migration. The Bazel iOS
> community has also been an integral part of the project contributing new
> features and optimizations; I’m excited to see the project move into the
> Mobile Native Foundation and continue to grow!

&ndash; Sebastian Shanus, Senior Engineer @ Robinhood

### Slack

> At Slack, the Bazel Xcode experience has always been where we invest the most
> time in, and has always been the most difficult to get right. With
> rules_xcodeproj and the outstanding community around it, a native Xcode
> experience “just works” allowing us to focus on building Slack. Under the
> Mobile Native Foundation, we can be confident rules_xcodeproj will continue to
> grow and be supported by the community, and serve us at Slack for years to
> come.

&ndash; Erik Kerber, Staff Engineer @ Slack

### Snap

> Snap has been struggling with a subpar Xcode developer experience, which has
> impeded the developer productivity. We have tried various Xcode project
> generators, but a lack of community support has left us constantly frustrated.
> Fortunately, rules_xcodeproj and its supportive community have provided a
> solution. With rules_xcodeproj, iOS development will be as smooth as “native”
> Xcode projects, even at scale.
>
> Snap is thrilled to see that rules_xcodeproj is now part of the Mobile Native
> Foundation. This will not only bring more community contributions to the
> project, but also provide the benefits of rules_xcodeproj to everyone
> involved.

&ndash; Yongjin Cho, Engineer @ Snap

### Spotify

> We are proud to have been involved in rules_xcodeproj since the early days. It
> has significantly lowered the entry-barrier to Bazel for the Apple ecosystem
> and allowed our iOS engineers at Spotify to start using Bazel with Xcode
> faster than we could ever have imagined. We are looking forward to continuing
> this journey of improving the developer experience for Apple developers under
> the Mobile Native Foundation umbrella with the rest of the community.

&ndash; Patrick Balestra, Staff Engineer @ Spotify

### Tinder

> Tinder’s local development experience prior to rules_xcodeproj was subpar and
> our developers wanted more. After we adopted this project generator our Xcode
> project experience is no longer a constant source of feedback. This project
> has also been a joy to contribute to and to learn more about Bazel in general.
> The migration to Mobile Native Foundation signifies the importance this
> project holds for the entire Apple development community.

&ndash; Maxwell Elliot, Staff Engineer @ Tinder

### Uber

> Getting Bazel running inside Xcode the right way with all the IDE
> functionality working as expected has been a challenge for the entire
> community. Constant Bazel updates, and additions happening in rules_apple, and
> rules_swift makes maintenance even more challenging and all of this takes most
> of the time of all the mobile dev platform teams. Since it happens all
> in-house, we don’t get community benefits and vice-versa.
>
> However, with rules_xcodeproj and the great community support that it has, I
> was really surprised to see our core Rider app building in Xcode in both BwB
> and BwX modes with no code changes, it literally has been a treat to see its
> progress. I am really impressed by the quality bar it has, and how it went
> beyond by adding various fixes in Bazel and rule sets to get the experience
> working the right way. All of this has boosted my confidence in
> rules_xcodeproj.
>
> I strongly believe that this project is the core foundation for any Apple
> Bazel workspace and with great community ownership under the Mobile Native
> Foundation, I feel confident about the future of rules_xcodeproj and the
> Bazel community it benefits.

&ndash; Chirag Ramani, Staff Engineer @ Uber

## Comments?

Do you have comments on rules_xcodeproj being donated to the Mobile Native
Foundation? You can send us a message in the `#rules_xcodeproj` channel in the
[Bazel Slack workspace][bazel-slack], or you can email us at
<hello@buildbuddy.io>.

[bazel-slack]: https://slack.bazel.build/
