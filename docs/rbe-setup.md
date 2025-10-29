---
id: rbe-setup
title: RBE Setup
sidebar_label: Remote Build Execution Setup
---

Getting started with Remote Build Execution (RBE) is less daunting than it may seem. We've put together a guide that not only helps you get started with BuildBuddy RBE, but also helps you understand what is going on under the hood.

This guide assumes you're using [BuildBuddy Cloud](cloud.md) or [BuildBuddy Enterprise on-prem](enterprise.md).

## The basics

To execute a build remotely on BuildBuddy, pass the `--remote_executor` flag to your `bazel build` command:

```bash
bazel build //... --remote_executor=grpcs://remote.buildbuddy.io
```

This points Bazel at BuildBuddy Cloud as a remote executor. Projects that do not build native libraries or binaries will likely build correctly using the command above.

Projects that build native libraries or binaries or depend on platform-specific tools will need more configuration to take advantage of RBE. We do this with [platforms](https://bazel.build/extending/platforms) and [toolchains](https://bazel.build/extending/toolchains). Adding the [BuildBuddy Toolchain](https://github.com/buildbuddy-io/buildbuddy-toolchain) to your project will let you build and test targets remotely.

## Using the BuildBuddy Toolchain with Bazel modules

If your project uses [Bazel modules](https://bazel.build/external/module), you can add the BuildBuddy Toolchain as a dependency for your module:

```python title="MODULE.bazel"
bazel_dep(name = "toolchains_buildbuddy")
archive_override(
    module_name = "toolchains_buildbuddy",
    integrity = "sha256-VtJjefgP2Vq5S6DiGYczsupNkosybmSBGWwcLUAYz8c=",
    strip_prefix = "buildbuddy-toolchain-66146a3015faa348391fcceea2120caa390abe03",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/66146a3015faa348391fcceea2120caa390abe03.tar.gz"],
)
buildbuddy = use_extension("@toolchains_buildbuddy//:extensions.bzl", "buildbuddy")

register_toolchains(
    "@toolchains_buildbuddy//toolchains/cc:ubuntu_gcc_x86_64",
)
```

Then, to build remotely on x86_64 Linux:

```bash
bazel build //... \
    --bes_results_url=https://app.buildbuddy.io/invocation/ \
    --bes_backend=grpcs://remote.buildbuddy.io \
    --remote_executor=grpcs://remote.buildbuddy.io \
    --platforms=@toolchains_buildbuddy//platforms:linux_x86_64 \
    --extra_execution_platforms=@toolchains_buildbuddy//platforms:linux_x86_64
```

## Using the BuildBuddy Toolchain with a WORKSPACE file

If your project still uses a [`WORKSPACE`](https://bazel.build/concepts/build-ref) file, you can add the BuildBuddy Toolchain with this snippet:

```python title="WORKSPACE"
http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    integrity = "sha256-VtJjefgP2Vq5S6DiGYczsupNkosybmSBGWwcLUAYz8c=",
    strip_prefix = "buildbuddy-toolchain-66146a3015faa348391fcceea2120caa390abe03",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/66146a3015faa348391fcceea2120caa390abe03.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")

buildbuddy(name = "buildbuddy_toolchain")
```

## Platforms

The first thing you'll want to do is tell BuildBuddy RBE in what environment you'll want to run your build actions. This is tools can be found in different locations on different platforms. This is done with the `--host_platform`, `--platforms`, and `--extra_execution_platforms` flags.

BuildBuddy's default platform is Ubuntu 16.04 with Java 8 installed. We can specify this platform with the `--host_platform`, `--platforms`, and `--extra_execution_platforms` flags:

```bash
--host_platform=@buildbuddy_toolchain//:platform
--platforms=@buildbuddy_toolchain//:platform
--extra_execution_platforms=@buildbuddy_toolchain//:platform
```

If you want to use a different environment, you can specify a custom Docker container image to use. More information on how to do this can be found in our [platforms documentation](rbe-platforms.md).

## Toolchains

Toolchains sound complicated (and they can be) - but the concept is simple. We're telling our remote executors where to find tools that are needed to build our code.

### C toolchain

The first toolchain you'll likely run into the need for is a C/C++ compiler. Even if your code isn't written in one of these languages, it's likely that one of your dependencies is - or calls some C code with something like [cgo](https://golang.org/cmd/cgo/).

You'll know you need a C toolchain when you see an error for a missing gcc or clang that looks like:

```bash
exec: "/usr/bin/gcc": stat /usr/bin/gcc: no such file or directory
```

To use BuildBuddy's default C toolchain, we can use the `--crosstool_top` and `--extra_toolchains` flag:

```bash
--crosstool_top=@buildbuddy_toolchain//:toolchain
--extra_toolchains=@buildbuddy_toolchain//:cc_toolchain
```

If you're looking for an llvm based toolchain instead, take a look at [this project](https://github.com/grailbio/bazel-toolchain).

### Java toolchain

If your project depends on Java code, you'll need to set the following flags:

```bash
--java_language_version=11
--tool_java_language_version=11
--java_runtime_version=remotejdk_11
--tool_java_runtime_version=remotejdk_11
```

Available versions are listed in [Bazel's User Manual](https://bazel.build/docs/user-manual#java-language-version).

If you need a custom Java toolchain, see Bazel's docs on [Java toolchain configuration](https://bazel.build/docs/bazel-and-java#config-java-toolchains).

#### Java toolchain for older Bazel versions

If your project is using a Bazel version before 6.0.0, you will need the following 4 flags instead.
They will tell the executors where to look for Java tools.

::::warning

Setting both the old flags and the new flags will result in an error and may result in incorrect toolchain selection.

See https://github.com/bazelbuild/bazel/issues/7849 for more information.

::::

Using BuildBuddy's default Java 8 config:

```bash
--javabase=@buildbuddy_toolchain//:javabase_jdk8
--host_javabase=@buildbuddy_toolchain//:javabase_jdk8
--java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8
--host_java_toolchain=@buildbuddy_toolchain//:toolchain_jdk8
```

If you need a different version of Java, we recommend using [bazel-toolchains](https://github.com/bazelbuild/bazel-toolchains) for now.

### Attributes

Some tools like Bazel's zipper (@bazel_tools//tools/zip:zipper) use an attribute to determine whether or not they're being run remotely or not. For tools like these to work properly, you'll need to define an attribute called `EXECUTOR` and set it to the value `remote`.

```bash
--define=EXECUTOR=remote
```

## Putting it all together

This can be a lot of flags to tack onto each bazel build, so instead you can move these to your `.bazelrc` file under the `remote` config block:

```bash title=".bazelrc"
build:remote --remote_executor=grpcs://remote.buildbuddy.io
build:remote --host_platform=@buildbuddy_toolchain//:platform
build:remote --platforms=@buildbuddy_toolchain//:platform
build:remote --extra_execution_platforms=@buildbuddy_toolchain//:platform
build:remote --crosstool_top=@buildbuddy_toolchain//:toolchain
build:remote --extra_toolchains=@buildbuddy_toolchain//:cc_toolchain
build:remote --java_language_version=11
build:remote --tool_java_language_version=11
build:remote --java_runtime_version=remotejdk_11
build:remote --tool_java_runtime_version=remotejdk_11
build:remote --define=EXECUTOR=remote
```

And running:

```bash
bazel build //... --config=remote
```

## Authentication

You'll want to authenticate your RBE builds with either API key or certificate based auth. For more info on how to set this up, see our [authentication guide](guide-auth.md).

## Configuration options

### --jobs

This determines the number of parallel actions Bazel will remotely execute at once. If this flag is not set, Bazel will use a heuristic based on the number of cores on your local machine. Your builds & tests can likely be parallelized much more aggressively when executing remotely. We recommend starting with `50` and working your way up.

```bash
--jobs=50
```

[Bazel docs](https://bazel.build/reference/command-line-reference#build-flag--jobs)

### --remote_timeout

This determines the maximum time Bazel will spend on any single remote call, including cache writes. The default value is 60s. We recommend setting this high to avoid timeouts when uploading large cache artifacts.

```bash
--remote_timeout=10m
```

[Bazel docs](https://bazel.build/reference/command-line-reference#build-flag--remote_timeout)

### --remote_download_minimal

By default, bazel will download intermediate results of remote executions - so in case an artifact isn't found in the remote cache, it can be re-uploaded. This can slow down builds in network-constrained environments.

This can be turned off with the flag:

```bash
--remote_download_minimal
```

This flag can speed up your build and reduce your cache transfer. It's recommended as long you're using Bazel version 7.0 or higher.

[Bazel docs](https://bazel.build/reference/command-line-reference#build-flag--remote_download_minimal)

### --remote_instance_name

If you'd like separate remote caches, whether it's for CI builds vs local builds or other reasons, you can use the `remote_instance_name` flag to namespace your cache artifacts:

```bash
--remote_instance_name=buildbuddy-io/buildbuddy/ci
```

[Bazel docs](https://bazel.build/reference/command-line-reference#build-flag--remote_instance_name)

### --disk_cache

While setting a local disk cache can speed up your builds, when used in conjunction with remote execution - your local and remote state has the opportunity to get out of sync. If you suspect you're running into this problem, you can disable your local disk cache by setting this to an empty value.

```bash
--disk_cache=
```

[Bazel docs](https://bazel.build/reference/command-line-reference#build-flag--disk_cache)

### --incompatible_strict_action_env

Some rules (like protobuf) are particularly sensitive to changes in environment variables and will frequently be rebuilt due to resulting cache misses. To mitigate this, you can use the `incompatible_strict_action_env` which sets a static value for `PATH`.

```bash
--incompatible_strict_action_env
```

[Bazel docs](https://bazel.build/reference/command-line-reference#flag--incompatible_strict_action_env)

### --action_env, --test_env

You can set environment variables that are available to actions with the `--action_env` flag.
If you want to set environment variables that are only available to test actions, you can use the `--test_env` flag.

This is commonly used to set environment variables such as `GO_TEST_WRAP_TESTV=1`, which modifies how the `go_test` rule handles the XML reports.

```bash
--test_env=GO_TEST_WRAP_TESTV=1
```

[`--action_env` Bazel docs](https://bazel.build/reference/command-line-reference#flag--action_env)
[`--test_env` Bazel docs](https://bazel.build/reference/command-line-reference#flag--test_env)

### --repo_env

This flag sets additional environment variables that are available to repository rules executions.
It's commonly used to set environment variables that are used to augment how toolchains and external repositories are prepared.

```bash
# Tell Bazel to not detect the CC toolchain automatically from the host machine to improve hermeticity.
# This requires you to declare and register a CC toolchain explicitly.
--repo_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1

# Tell Gazelle's go_repository rule to use the path set in GOMODCACHE (or GOPATH) as a local cache directory.
# This can be useful to speed up Go modules downloads.
--repo_env=GO_REPOSITORY_USE_HOST_CACHE=1
--repo_env=GOMODCACHE=/some-path/my-go-mod-cache
```

[Bazel docs](https://bazel.build/reference/command-line-reference#common_options-flag--repo_env)

### --define

Define allows you to assign build variables. This is commonly use to set `EXECUTOR` to [compile singlejar and ijar from source](https://github.com/bazelbuild/bazel/issues/7254).

```bash
--define=EXECUTOR=remote
```

[Bazel docs](https://bazel.build/reference/command-line-reference#flag--define)

### --spawn_strategy

Sets the list of strategies in priority order from highest to lowest. Each action picks the highest priority strategy that it can execute. The default value is `remote,worker,sandboxed,local`.

```bash
--strategy=remote,local
```

[Bazel docs](https://bazel.build/reference/command-line-reference#build-flag--spawn_strategy)

### --strategy

Explicitly setting strategies should [no longer be needed](https://github.com/bazelbuild/bazel/issues/7480) for Bazel versions post 0.27.0. It can be used to force certain bazel mnemonics to be build remotely.

```bash
--strategy=Scalac=remote
```

[Bazel docs](https://bazel.build/reference/command-line-reference#build-flag--strategy)

### --remote_execution_priority

Sets the priority of remotely executed actions within a build, relative to
other builds within your BuildBuddy organization.

The default value is 0, and BuildBuddy accepts values between -1000 and
1000 (inclusive). Lower priority values cause actions to be executed
_before_ actions with larger priority values.

Examples:

```bash
# Run a build that is deprioritized relative to other builds in the org:
bazel test //experimental/... --remote_execution_priority=1000

# Run a build that is prioritized over all other builds in the org:
bazel run //emergency_deployment --remote_execution_priority=-1000
```

Note: remote execution priority is applied on a best-effort basis. Setting
this flag doesn't provide a strong guarantee of execution ordering.

[Bazel docs](https://bazel.build/reference/command-line-reference#build-flag--remote_execution_priority)

### --experimental_inmemory_dotd_files

If enabled, C++ .d files will be passed through in memory directly from the remote build nodes instead of being written to disk. This flag is automatically set when using `--remote_download_minimal`.

```bash
--experimental_inmemory_dotd_files
```

[Bazel docs](https://bazel.build/reference/command-line-reference#flag--experimental_inmemory_dotd_files)

### --experimental_inmemory_jdeps_files

If enabled, .jdeps files generated from Java compilations will be passed through in memory directly from the remote build nodes instead of being written to disk. This flag is automatically set when using `--remote_download_minimal`.

```bash
--experimental_inmemory_jdeps_files
```

[Bazel docs](https://bazel.build/reference/command-line-reference#flag--experimental_inmemory_jdeps_files)

## Examples

- [buildbuddy-io/buildbuddy .bazelrc --config=remote](https://github.com/buildbuddy-io/buildbuddy/blob/master/.bazelrc#L23)
- [graknlabs/grakn .bazelrc --config=rbe](https://github.com/graknlabs/grakn/blob/master/.bazelrc#L6)
- [wix/exodus .bazlerc.remote](https://github.com/wix/exodus/blob/master/.bazelrc.remote#L8)

## Advanced configuration

If you need a more advanced configuration than provided by the basic BuildBuddy toolchain, we recommend exploring Bazel's [bazel-toolchains](https://releases.bazel.build/bazel-toolchains.html) repo. Its `rbe_autoconfig` rule is highly configurable and works nicely with BuildBuddy.

Here's a quick snippet you can add to your `WORKSPACE` file if using bazel 3.6.0:

```python title="WORKSPACE"
load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")

http_archive(
    name = "bazel_toolchains",
    sha256 = "4fb3ceea08101ec41208e3df9e56ec72b69f3d11c56629d6477c0ff88d711cf7",
    strip_prefix = "bazel-toolchains-3.6.0",
    urls = [
        "https://github.com/bazelbuild/bazel-toolchains/releases/download/3.6.0/bazel-toolchains-3.6.0.tar.gz",
        "https://mirror.bazel.build/github.com/bazelbuild/bazel-toolchains/releases/download/3.6.0/bazel-toolchains-3.6.0.tar.gz",
    ],
)

load("@bazel_toolchains//rules:rbe_repo.bzl", "rbe_autoconfig")

# Creates a default toolchain config for RBE.
# Use this as is if you are using the rbe_ubuntu16_04 container,
# otherwise refer to RBE docs.
rbe_autoconfig(name = "rbe_default")
```

And to your `.bazelrc`:

```bash title=".bazelrc"
# Depending on how many machines are in the remote execution instance, setting
# this higher can make builds faster by allowing more jobs to run in parallel.
# Setting it too high can result in jobs that timeout, however, while waiting
# for a remote machine to execute them.
build:remote --jobs=50

# Set several flags related to specifying the platform, toolchain and java
# properties.
# These flags should only be used as is for the rbe-ubuntu16-04 container
# and need to be adapted to work with other toolchain containers.
build:remote --java_language_version=11
build:remote --tool_java_language_version=11
build:remote --java_runtime_version=remotejdk_11
build:remote --tool_java_runtime_version=remotejdk_11
build:remote --crosstool_top=@rbe_default//cc:toolchain
build:remote --repo_env=BAZEL_DO_NOT_DETECT_CPP_TOOLCHAIN=1
# Platform flags:
# The toolchain container used for execution is defined in the target indicated
# by "extra_execution_platforms", "host_platform" and "platforms".
# More about platforms: https://bazel.build/extending/platforms
build:remote --extra_toolchains=@rbe_default//config:cc-toolchain
build:remote --extra_execution_platforms=@rbe_default//config:platform
build:remote --host_platform=@rbe_default//config:platform
build:remote --platforms=@rbe_default//config:platform

# Starting with Bazel 0.27.0 strategies do not need to be explicitly
# defined. See https://github.com/bazelbuild/bazel/issues/7480
build:remote --define=EXECUTOR=remote

# Enable remote execution so actions are performed on the remote systems.
build:remote --remote_executor=grpcs://remote.buildbuddy.io

# Enforce stricter environment rules, which eliminates some non-hermetic
# behavior and therefore improves both the remote cache hit rate and the
# correctness and repeatability of the build.
build:remote --incompatible_strict_action_env=true

# Set a higher timeout value, just in case.
build:remote --remote_timeout=10m
```

And then run:

```bash
bazel build //... --config=remote
```
