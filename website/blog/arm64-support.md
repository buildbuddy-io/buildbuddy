---
slug: arm64-support
title: Remote Builds on linux/arm64 with BuildBuddy
description: Announcing arm64 support for remote linux builds
authors: zoey
date: 2024-10-29:12:00:00
image: /img/blog/postgres-support.png
tags: [product]
---

You asked, and we answered: BuildBuddy now offers the option to build your arm64
binaries remotely! We've created a new executor pool running on arm, and we've
updated the `platform-linux` platform target in
[`buildbuddy-toolchain`](https://github.com/buildbuddy-io/buildbuddy-toolchain)
to automatically detect your architecture and set your platform appropriately.
This should make maintaining a project that builds on both arm and amd
ergonomic and easy!

<!-- truncate -->

## Try it out!

On an arm64 machine, add to your project:

`WORKSPACE`:

```
# BuildBuddy Toolchain
http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "baa9af1b9fcc96d18ac90a4dd68ebd2046c8beb76ed89aea9aabca30959ad30c",
    strip_prefix = "buildbuddy-toolchain-287d6042ad151be92de03c83ef48747ba832c4e2",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/287d6042ad151be92de03c83ef48747ba832c4e2.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "UBUNTU20_04_IMAGE", "buildbuddy")

buildbuddy(
    name = "buildbuddy_toolchain",
    container_image = UBUNTU20_04_IMAGE,
)

register_execution_platforms(
    "@buildbuddy_toolchain//:platform_linux",
)
```

Then just build your project with remote execution specified like normal, and
your build will sent off to the new arm executors to be built.

As always, please report any problems you find to us either on our
[GitHub repo](https://github.com/buildbuddy-io/buildbuddy) or come chat with us
on [Slack](https://community.buildbuddy.io/)!
