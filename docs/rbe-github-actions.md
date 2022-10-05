---
id: rbe-github-actions
title: RBE with GitHub Actions
sidebar_label: RBE with GitHub Actions
---

Using BuildBuddy with Github Actions is an easy way to get started using BuildBuddy with a CI system. For an even easier way to get started, see the [BuildBuddy Workflows Setup Guide](workflows-setup.md).

## Setup instructions

There are three steps:

1. Create a workflow file
1. Update your `.bazelrc`
1. Set up a GitHub Secret containing your BuildBuddy API key

### Workflow file

All you have to do is create a file `.github/workflows/main.yaml`

```
name: CI

on:
  push:
    branches:
      - master

jobs:
  build:
    runs-on: ubuntu-latest

    steps:
    - name: Checkout
      uses: actions/checkout@v3

    - name: Install bazelisk
      run: |
        curl -LO "https://github.com/bazelbuild/bazelisk/releases/download/v1.1.0/bazelisk-linux-amd64"
        mkdir -p "${GITHUB_WORKSPACE}/bin/"
        mv bazelisk-linux-amd64 "${GITHUB_WORKSPACE}/bin/bazel"
        chmod +x "${GITHUB_WORKSPACE}/bin/bazel"
    - name: Build
      run: |
        "${GITHUB_WORKSPACE}/bin/bazel" build \
            --config=ci \
            --remote_header=x-buildbuddy-api-key=${{ secrets.BUILDBUDDY_ORG_API_KEY }} \
            //...
    - name: Test
      run: |
        "${GITHUB_WORKSPACE}/bin/bazel" test \
            --config=ci \
            --remote_header=x-buildbuddy-api-key=${{ secrets.BUILDBUDDY_ORG_API_KEY }} \
            //...

```

### Updating your .bazelrc

You'll then need to add the following configuration to your `.bazelrc`

```
build:ci --build_metadata=ROLE=CI
build:ci --bes_results_url=https://app.buildbuddy.io/invocation/
build:ci --bes_backend=grpcs://remote.buildbuddy.io
```

### Github secrets

Finally, you'll need to create a GitHub Secret containing your BuildBuddy API Key.

You can get your BuildBuddy API key by logging in to your [BuildBuddy account](https://app.buildbuddy.io) and visiting your [Quickstart page](https://app.buildbuddy.io/docs/setup/).

Add your BuildBuddy API Key as GitHub Secret named `BUILDBUDDY_ORG_API_KEY`. For more information on setting up Github Secrets, [click here](https://docs.github.com/en/actions/configuring-and-managing-workflows/creating-and-storing-encrypted-secrets).

## More

### Github commit statuses

If you'd like BuildBuddy to publish commit statuses to your repo, you can do so by [logging in](https://app.buildbuddy.io) and clicking `Link Github Account` in the user menu in the top right.

### Visibility

By default, authenticated builds are only visible to members of your BuildBuddy organization. If you'd like your BuildBuddy results pages to be visible to members outside of your organization, you can add the following line to your `.bazelrc`:

```
build:ci --build_metadata=VISIBILITY=PUBLIC
```

### Remote build execution

If you'd like to use BuildBuddy's Remote Build Execution capabilities in your CI workflow, you can add the following lines to your `.bazelrc`:

```
build:remote --remote_cache=grpcs://remote.buildbuddy.io
build:remote --remote_executor=grpcs://remote.buildbuddy.io
build:remote --remote_upload_local_results
build:remote --host_platform=@buildbuddy_toolchain//:platform
build:remote --platforms=@buildbuddy_toolchain//:platform
build:remote --crosstool_top=@buildbuddy_toolchain//:toolchain
build:remote --jobs=100

build:ci --config=remote
```

And the following lines to your `WORKSPACE` file:

```
http_archive(
    name = "io_buildbuddy_buildbuddy_toolchain",
    sha256 = "a2a5cccec251211e2221b1587af2ce43c36d32a42f5d881737db3b546a536510",
    strip_prefix = "buildbuddy-toolchain-829c8a574f706de5c96c54ca310f139f4acda7dd",
    urls = ["https://github.com/buildbuddy-io/buildbuddy-toolchain/archive/829c8a574f706de5c96c54ca310f139f4acda7dd.tar.gz"],
)

load("@io_buildbuddy_buildbuddy_toolchain//:deps.bzl", "buildbuddy_deps")

buildbuddy_deps()

load("@io_buildbuddy_buildbuddy_toolchain//:rules.bzl", "buildbuddy")

buildbuddy(name = "buildbuddy_toolchain")
```

If you're using Java, or have a complex project - you'll likely need to configure the toolchain flags a bit. For more information, see our [Remote Build Execution guide](rbe-setup.md).
