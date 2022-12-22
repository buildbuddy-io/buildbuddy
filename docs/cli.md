---
id: cli
title: BuildBuddy CLI
sidebar_label: CLI Overview
---

The [BuildBuddy CLI](/cli) brings the power of BuildBuddy to the command line. It's a [Bazel](https://bazel.build/) wrapper that's built on top of [Bazelisk](https://github.com/bazelbuild/bazelisk) and brings support for [plugins](#plugins), [authentication](#authentication), [flaky network conditions](#networking), and more.

Because it's built on top of [Bazelisk](https://github.com/bazelbuild/bazelisk), it's command line compatible with Bazel - which means you can simply `alias bazel=bb` and keep using Bazel the way you normally would.

It's written in [go](https://go.dev/), [fully open source](https://github.com/buildbuddy-io/buildbuddy/tree/master/cli), and [MIT licensed](https://opensource.org/licenses/MIT).

## Installation

The easiest way to install the BuildBuddy CLI is by running this simple bash script, which works on both MacOS and Linux:

```bash
curl -fsSL install.buildbuddy.io | bash
```

If you're not comfortable executing random bash scripts from the internet (we totally get it!), you can take a look at what this script is doing under the hood, by visiting [install.buildbuddy.io](https://install.buildbuddy.io) in your browser.

It's downloading the latest BuildBuddy CLI binary for your OS and architecture from our Github repo [here](https://github.com/buildbuddy-io/bazel/releases) and moving it to `/usr/local/bin/bb`.

You can perform those steps manually yourself if you'd like!

## Updating

You can update the cli by re-running the installation script:

```bash
curl -fsSL install.buildbuddy.io | bash
```

If you installed BuildBuddy manually instead, you can repeat those installation steps to update your CLI.

You can check your BuildBuddy CLI version at any time by running:

```bash
bb version
```

## Installing for a project

If you're already using Bazelisk, you can easily install the BuildBuddy CLI for your entire project by running:

```bash
echo "$(echo "buildbuddy-io/0.1.4"; cat .bazelversion)" > .bazelversion
```

This will simply prepend `buildbuddy-io/0.1.4` on a new line above your `.bazelversion` file like so:

```title=".bazelversion"
buildbuddy-io/0.1.4
5.3.2
```

The version 0.0.12 of the BuildBuddy CLI will now automatically be used when you type `bazel` or `bazelisk` and continue to use the Bazel version specified on the second line of your `.bazelrc` file.

To find the latest version of the BuildBuddy CLI, you can view our releases page [here](https://github.com/buildbuddy-io/bazel/releases).

## Features

### Networking

The BuildBuddy CLI was built to handle flaky network conditions without affecting your build. It does this by forwarding all remote cache & build event stream requests through a local proxy. This means that you'll never have to sit around waiting for outputs or build events to upload, and your build won't fail if you're not connected to the internet.

### Plugins

The BuildBuddy CLI comes with a robust plugin system. Plugins are super simple to write, share, and install.

You can find a list of plugins that you can install in our [plugin library](/plugins).

For more information on how to write your own plugins, check out the [plugin documentation](/docs/cli-plugins).

### Authentication

The BuildBuddy CLI makes authentication to BuildBuddy a breeze. You can simply type `bb login` and follow the instructions. Once you're logged in, all of your requests to BuildBuddy will be authenticated to your organization.

## Contributing

We welcome pull requests! You can find the code for the BuildBuddy CLI on Github [here](https://github.com/buildbuddy-io/buildbuddy/tree/master/cli). See our [contributing docs](https://www.buildbuddy.io/docs/contributing) for more info.

## Reporting an issue

If you run into an issue with the BuildBuddy CLI, please let us know by [filing an issue](https://github.com/buildbuddy-io/buildbuddy/issues/new) and including **[CLI]** in the title.
