<!--
{
  "name": "RBE Setup",
  "category": "5f84be4816a467f32f4ca128",
  "priority": 900
}
-->

# Remote Build Execution Setup

Getting started with Remote Build Execution (RBE) is less daunting than it may seem. We've put together a guide that not only helps you get started with BuildBuddy RBE, but also helps you understand what is going on under the hood.

This guide assumes you're using [BuildBuddy Cloud](cloud.md) or [BuildBuddy Enterprise on-prem](enterprise.md).

## The basics

The very simplest Bazel command needed to enable RBE is the following:

```
bazel build //... --remote_executor=grpcs://cloud.buildbuddy.io
```

This points Bazel at BuildBuddy Cloud as a remote executor. A simple repo that has C/C++/CGO or Java dependencies will build just fine like this. Most interesting repos have some dependencies on C/C++/CGO or Java - so we'll need to tell our remote executors where to find tools like gcc or the JRE. We do this with [platforms](https://docs.bazel.build/versions/master/platforms.html) and [toolchains](https://docs.bazel.build/versions/master/toolchains.html).

## Configuring your workspace

There are several options for configuring your platforms and toolchains, the most fully features of which being [bazel-toolchains](https://releases.bazel.build/bazel-toolchains.html). It comes with an `rbe_autoconfig` rule that works nicely with BuildBuddy.

Unfortunately, bazel-toolchains has a dependency on Docker and can take quite some time to start up in a clean workspace, so we provide a simple and easy-to-use [BuildBuddy toolchain](https://github.com/buildbuddy-io/toolchain) that enables you to get up and running quickly, and works for most use cases.

To get started with the BuildBuddy Toolchain, add the following lines to your `WORKSPACE` file:

```
http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "9055a3e6f45773cd61931eba7b7cf35d6477ab6ad8fb2f18bf9815271fc682fe",
    strip_prefix = "buildbuddy-toolchain-52aa5d2cc6c9ba7ee4063de35987be7d1b75f8e2",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/52aa5d2cc6c9ba7ee4063de35987be7d1b75f8e2.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")

buildbuddy(name = "buildbuddy_toolchain")
```

You'll likely want to pin this to a specific commit/version and add a SHA for reproducible builds.

## Platforms

The first thing you'll want to do is tell BuildBuddy RBE in what environment you'll want to run your build actions. This is tools can be found in different locations on different platforms. This is done with the `--host_platform` flag.

BuildBuddy's default platform is Ubuntu 16.04 with Java 8 installed. We can specify this platform with the `--host_platform` flag:

```
--host_platform=@buildbuddy_toolchain//:platform
```

If you want to use a different environment, you can specify a custom Docker container image to use. More information on how to do this can be found in our [platforms documentation](rbe-platforms.md).

## Toolchains

Toolchains sound complicated (and they can be) - but the concept is simple. We're telling our remote executors where to find tools that are needed to build our code.

### C toolchain

The first toolchain you'll likely run into the need for is a C/C++ compiler. Even if your code isn't written in one of these languages, it's likely that one of your dependencies is - or calls some C code with something like [cgo](https://golang.org/cmd/cgo/).

You'll know you need a C toolchain when you see an error for a missing gcc or clang that looks like:

```
exec: "/usr/bin/gcc": stat /usr/bin/gcc: no such file or directory
```

To use BuildBuddy's default C toolchain, we can use the `--crosstool_top` flag:

```
--crosstool_top=@buildbuddy_toolchain//:toolchain
```

### Java toolchain

If your project depends on Java code, you'll need 4 more flags to tell the executors where to look for Java tools.

Using BuildBuddy's default Java 8 config:

```
--javabase=@buildbuddy_toolchain//:javabase_jdk8
--host_javabase=@buildbuddy_toolchain//:javabase_jdk8
--java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8
--host_java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8
```

If you need a different version of Java, we recommend using [bazel-toolchains](https://releases.bazel.build/bazel-toolchains.html) for now.

## Putting it all together

Now that we've got our platform and toolchains setup - we can do a remote build that includes C & Java dependencies with a command like:

```
bazel build //... --remote_executor=grpcs://cloud.buildbuddy.io --crosstool_top=@buildbuddy_toolchain//:toolchain --javabase=@buildbuddy_toolchain//:javabase_jdk8 --host_javabase=@buildbuddy_toolchain//:javabase_jdk8 --java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8 --host_java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8
```

This can be a lot of flags to tack onto each bazel build, so instead you can move these to your `.bazelrc` file:

```
build:remote --host_platform=@buildbuddy_toolchain//:platform
build:remote --crosstool_top=@buildbuddy_toolchain//:toolchain
build:remote --javabase=@buildbuddy_toolchain//:javabase_jdk8
build:remote --host_javabase=@buildbuddy_toolchain//:javabase_jdk8
build:remote --java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8
build:remote --host_java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8
```

And running:

```
bazel build //... --config=remote
```

## Authentication

You'll want to authenticate your RBE builds with either API key or certificate based auth. For more info on how to set this up, see our [authentication guide](guide-auth.md).

## Optimizations

By default, bazel will download intermediate results of remote executions - so in case an artifact isn't found in the remote cache, it can be re-uploaded. This can slow down builds in networks constrained environments.

This can be turned off with the flag:

```
--remote_download_minimal
```

While this flag can speed up your build, it makes them more sensitive to caching issues - and likely shouldn't be used in production yet.
