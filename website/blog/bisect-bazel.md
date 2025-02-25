---
slug: bisect-bazel
title: "Troubleshooting Bazel with Git Bisect"
description: A guide to using Git bisect to troubleshoot Bazel issues
authors: son
date: 2025-02-18:12:00:00
image: /img/blog/troubleshooting.png
tags: [bazel, git, engineering]
---

Upgrading Bazel and the related dependencies can sometimes lead to unexpected issues.
These issues can range from build failures to runtime errors, and generally, they can be hard to troubleshoot.

So today, we will discuss how to narrow down the root cause of build failures after a dependency upgrade using `git bisect`.

<!-- truncate -->

## Build failed after upgrading Bazel

If you are like me, you would enjoy having the latest and greatest tools in your project.
And the tool I use the most is Bazel, so as the recent `8.1.0` release came out, I decided to upgrade our BuildBuddy repository to use it.

```bash
$ echo '8.1.0' > .bazelversion

$ bazel test --config=remote-minimal //...
...
ERROR: /root/workspace/output-base/external/bazel_tools/tools/build_defs/repo/http.bzl:137:45: An error occurred during the fetch of repository 'rules_cc+':
   Traceback (most recent call last):
        File "/root/workspace/output-base/external/bazel_tools/tools/build_defs/repo/http.bzl", line 137, column 45, in _http_archive_impl
                download_info = ctx.download_and_extract(
Error in download_and_extract: com.google.devtools.build.lib.remote.common.CacheNotFoundException: Missing digest: abc605dd850f813bb37004b77db20106a19311a96b2da1c92b789da529d28fe1/178823
...
```

Whoops! It seems like the build failed after upgrading Bazel to `8.1.0`.
Flipping back to `8.0.1` made the problem go away, which means the issue was introduced in Bazel `8.1.0`.

What should we do next?

#### Auto-bisect with bazelisk

Luckily, the Bazel team has provided us with a magic flag inside `bazelisk` that can help us with this.

[--bisect](https://github.com/bazelbuild/bazelisk?tab=readme-ov-file#--bisect) flag automatically lists the commits between the two Bazel releases and helps us bisect them.
It works like this:

```bash
alias bazel=bazelisk

# Usage:
#   bazelisk --bisect=<good commit hash>..<bad commit hash> test //foo:bar_test
$ BAZELISK_CLEAN=1 bazelisk --bisect=8.0.1..8.1.0 test --config=remote-minimal //...
```

**Explanation**: The last-known good release for us was `8.0.1`, and the bad release was `8.1.0`. Bazelisk will grab the list of commits between 2 versions from the Github API and start a bisect. For each bisect commit, it will grab a pre-built Bazel binary of that version from a Google Cloud Storage bucket and run the test command accordingly. The `.bazelversion` file will be ignored in these runs.

Because the issue was related to external dependencies download, we set `BAZELISK_CLEAN=1` for the whole Bazelisk bisect process.
This will add `bazel clean --expunge` in between each test run, effectively cleaning Bazel's output base and shutting down the Bazel JVM process, to make sure we reproduce the issue in a mint environment.
You can check out other environment variables that Bazelisk supports [here](https://github.com/bazelbuild/bazelisk/blob/740e1b91b5b2e86730b6bb12aaf211fc19f388a5/README.md?plain=1#L162)

Since we know that the issue was caused by the external download of `@rules_cc`, we can narrow down the test command to a `bazel query` that would still trigger the same download. This should help speed up each bisect run and reduce the chance of flakiness.

```bash
$ BAZELISK_CLEAN=1 bazelisk --bisect=8.0.1..8.1.0 query --config=remote-minimal @rules_cc//:all
```

And here is the result:

```bash
--- Getting the list of commits between 8.0.1 and 8.1.0
Found 95 commits between (24cba660a7231786405ac40335c2e7e5bd4d6859, 8.1.0]
--- Verifying if the given good Bazel commit (24cba660a7231786405ac40335c2e7e5bd4d6859) is actually good
--- Start bisecting
--- Testing with Bazel built at c5cf63199b3964b4a188da73a6c24599468131e9, 95 commits remaining...
--- Succeeded at c5cf63199b3964b4a188da73a6c24599468131e9
--- Testing with Bazel built at 040769767613287a0f1b5ceed41b9d7729126983, 47 commits remaining...

2025/02/18 10:56:29 Using unreleased version at commit 040769767613287a0f1b5ceed41b9d7729126983
2025/02/18 10:56:29 Downloading https://storage.googleapis.com/bazel-builds/artifacts/macos_arm64/040769767613287a0f1b5ceed41b9d7729126983/bazel...
2025/02/18 10:56:29 Skipping basic authentication for storage.googleapis.com because no credentials found in /Users/sluongng/.netrc
2025/02/18 10:56:29 could not run Bazel: could not download Bazel: failed to download bazel: failed to download bazel: HTTP GET https://storage.googleapis.com/bazel-builds/artifacts/macos_arm64/040769767613287a0f1b5ceed41b9d7729126983/bazel failed with error 404
```

Here, Bazelisk was able to identify `8.0.1` to be `24cba660a` and verified that it was indeed a good commit. It then started bisecting the commits between `24cba660a` and `8.1.0`. The first bisect commit `c5cf63199b` was good. However, the second bisect commit `0407697676` failed to download the Bazel binary from the Google Cloud Storage (GCS) bucket. This is a different failure than the original issue we are investigating, so how do we proceed?

#### Manual bisect with git-bisect

If Bazelisk had a flag to help us mark a few known commits as "SKIP", it would have been perfect here, but unfortunately, it doesn't. But I know for a fact that `git bisect` does have this feature, so let's switch to manual bisecting.

```bash
# My personal working directory
$ cd ~/work/bazelbuild/

# Clone the Bazel repository
$ git clone https://github.com/bazelbuild/bazel.git
$ cd bazel

# Start bisecting
$ git bisect start --no-checkout --first-parent 8.1.0 8.0.1
Bisecting: 47 revisions left to test after this (roughly 6 steps)
[8afe16e0396be93cc5d9bc2108aab2e45dfcb2bd] [8.1.0] Fix docs link rewriting for rules_android (#25018)
```

Note that thanks to the pre-built binaries from the GCS bucket managed by the Bazel team, we don't need to build Bazel ourselves. Because of this, we can use the `--no-checkout` flag to speed up the bisect process. When `--no-checkout` is used, the current bisect commit could be found in `.git/BISECT_HEAD` file.
`--first-parent` forces the bisect to only follow the first parent of the commit, which is usually all the merge commits in the `master/main` branch of each repo.
Thankfully, the `bazel.git` repo history is very linear so `--first-parent` is not really needed here, but I will keep the flag in to help the readers (including my future self) copy pasting easier.

```bash
$ cat .git/BISECT_HEAD
8afe16e0396be93cc5d9bc2108aab2e45dfcb2bd
```

So how do we tell Bazelisk to use a pre-built binary from the GCS bucket that belongs to this commit? Luckily, we can set the environment variable `USE_BAZEL_VERSION` for this exact reason.

```bash
$ USE_BAZEL_VERSION=7.1.0 bazel version
version
Bazelisk version: 1.25.0
Extracting Bazel installation...
Build label: 7.1.0
Build target: @@//src/main/java/com/google/devtools/build/lib/bazel:BazelServer
Build time: Mon Mar 11 17:55:51 2024 (1710179751)
Build timestamp: 1710179751
Build timestamp as int: 1710179751
```

With this, we can manually alternate between the 2 directories `~/work/bazelbuild/bazel` and `~/work/buildbuddy-io/buildbuddy` to run the test command and mark the commit as good or bad.

```bash
# Copy the new bisect commit to the clipboard
$ export USE_BAZEL_VERSION=$(cat .git/BISECT_HEAD)

# Verify the commit
$ cd ~/work/buildbuddy-io/buildbuddy
$ bazel ... query ...
...

$ cd ~/work/bazelbuild/bazel
# $ git bisect good
# $ git bisect bad
# $ git bisect skip

# Repeat
```

However, that would not make for a really good blog post.
So let's be a bit more fancy and create a script to help us automate this bisect process using `git bisect run`.

```bash
$ cat test-buildbuddy.sh
#!/bin/bash

export USE_BAZEL_VERSION='8.0.1'
# export USE_BAZEL_VERSION=$(cat .git/BISECT_HEAD)

function cleanup()
{
  (
    cd ~/work/buildbuddy-io/buildbuddy
    bazel clean --expunge
  )
}
trap cleanup EXIT

(
  cd ~/work/buildbuddy-io/buildbuddy
  STDERROUT=$(bazel 2>&1 query --repository_cache='' --config=remote-minimal @bazel_features//:all)
  BAZEL_EXIT_CODE=$?
  # If stderr contains 'could not download Bazel' then return with code 125
  # to skip the current commit during bisect.
  if [[ $BAZEL_EXIT_CODE -ne 0 && $STDERROUT == *"could not download Bazel"* ]]; then
    echo "Bazel download failed. Skipping commit $USE_BAZEL_VERSION."
    exit 125
  fi
  if [[ $BAZEL_EXIT_CODE -ne 0 ]]; then
    echo "Bazel query failed with exit code $BAZEL_EXIT_CODE."
  fi
  exit $BAZEL_EXIT_CODE
)

```

**Explanation**:

First, we want to export the `USE_BAZEL_VERSION` environment variable to the commit hash that we are currently bisecting. This will tell Bazelisk to use the pre-built binary from the GCS bucket that belongs to this commit. However, we won't use BISECT_HEAD just yet since we want to verify that this script works first. Knowing that `8.0.1` was the last known good commit, we can set the `USE_BAZEL_VERSION` to do a sanity check.

Next, since we are not using Bazelisk bisect feature, the automatic cleanup feature of `BAZELISK_CLEAN` won't work.
Instead, we will manually clean the workspace after each test run with `bazel clean --expunge` to achieve similar result.

After that, we want to make sure that we handle the download issue as some commits might be missing from the GCS bucket.
`git bisect run` allows us to skip the current commit by returning with an exit code of `125`.

> The special exit code 125 should be used when the current source code cannot be tested.
> If the script exits with this code, the current revision will be skipped

So we check if the Bazel query failed with the error message `could not download Bazel` and return with the exit code `125` to skip the current commit.
Otherwise, we return with the exit code of the Bazel query command.

Now let's verify that our script works on `8.0.1`:

```bash
$ ./test-buildbuddy.sh
Starting local Bazel server (no_version) and connecting to it...
...
@rules_cc//:empty_lib
@rules_cc//:link_extra_lib
@rules_cc//:link_extra_libs
Loading: 1 packages loaded
```

Now let's edit the script to use the `USE_BAZEL_VERSION` from the `BISECT_HEAD` file for the bisect run and run the bisect:

```bash
$ cat test-buildbuddy.sh
#!/bin/bash

# export USE_BAZEL_VERSION='8.0.1'
export USE_BAZEL_VERSION=$(cat .git/BISECT_HEAD)
...

$ git bisect run ./test-buildbuddy.sh
running './run.sh'
Bisecting: 23 revisions left to test after this (roughly 5 steps)
[11e85a4dc73f93ef25809e7d5f0409aaca7d42f1] [8.1.0] Respect comprehension variable shadowing in Starlark debugger output (#25139)
running './run.sh'
Bazel download failed. Skipping commit 11e85a4dc73f93ef25809e7d5f0409aaca7d42f1.
2025/02/18 13:18:12 Using unreleased version at commit 11e85a4dc73f93ef25809e7d5f0409aaca7d42f1
2025/02/18 13:18:12 Downloading https://storage.googleapis.com/bazel-builds/artifacts/macos_arm64/11e85a4dc73f93ef25809e7d5f0409aaca7d42f1/bazel...
2025/02/18 13:18:12 Skipping basic authentication for storage.googleapis.com because no credentials found in /Users/sluongng/.netrc
2025/02/18 13:18:12 could not download Bazel: failed to download bazel: failed to download bazel: HTTP GET https://storage.googleapis.com/bazel-builds/artifacts/macos_arm64/11e85a4dc73f93ef25809e7d5f0409aaca7d42f1/bazel failed with error 404
Bisecting: 23 revisions left to test after this (roughly 5 steps)
[12a3fc001b6629b52f5b24dce6018884222a0608] [8.1.0] See and use more than 64 CPUs on Windows (#25140)
running './run.sh'
Bisecting: 10 revisions left to test after this (roughly 4 steps)
[af6307bc6832d66cce772c5170961b4ff4521e48] [8.1.0] Configure `--run_under` target for the test exec platform (#25184)
running './run.sh'
Bisecting: 4 revisions left to test after this (roughly 3 steps)
[14219c4698e112a03ebe62eed7cb324f625f13c8] [8.1.0] Use digest function matching the checksum in gRPC remote downloader (#25225)
running './run.sh'
Bazel query failed with exit code 1.
Bisecting: 2 revisions left to test after this (roughly 2 steps)
[5f3a083d5649715dc0bed811ef41f53b91539d1d] [8.1.0] Update to use coverage_output_generator-v2.8 (#25202)
running './run.sh'
Bisecting: 1 revision left to test after this (roughly 1 step)
[a40a0cd9947dd73ec07f2394d108eb7e98745161] [8.1.0] Add version selector buttons to repo rule docs (#25211)
running './run.sh'
Bisecting: 0 revisions left to test after this (roughly 0 steps)
[aa4531d5a2116f85b80a753c53528032ed3cda71] [8.1.0] Don't suggest updates to private repo rule attributes (#25213)
running './run.sh'
14219c4698e112a03ebe62eed7cb324f625f13c8 is the first bad commit
commit 14219c4698e112a03ebe62eed7cb324f625f13c8
Author: bazel.build machine account <ci.bazel@gmail.com>
Date:   Fri Feb 7 12:07:48 2025 +0100

    [8.1.0] Use digest function matching the checksum in gRPC remote downloader (#25225)

    Fixes https://bazelbuild.slack.com/archives/CA31HN1T3/p1738763759125489

    Closes #25206.

    PiperOrigin-RevId: 724267755
    Change-Id: Ia23bdae310231bd0ee5763311b948f3465aa8ed0

    Commit
    https://github.com/bazelbuild/bazel/commit/ef45e02bfb4af1124bb9ad1ef94f36c70c82ce48

    Co-authored-by: Fabian Meumertzheim <fabian@meumertzhe.im>

 .../remote/downloader/GrpcRemoteDownloader.java    | 23 ++++++++++++++--------
 .../downloader/GrpcRemoteDownloaderTest.java       | 23 +++++++++++++---------
 2 files changed, 29 insertions(+), 17 deletions(-)
bisect found first bad commit
```

And voilà! We have found the commit that introduced the issue.
It was `14219c4698e112a03ebe62eed7cb324f625f13c8`, which was introduced in Bazel `8.1.0`.

We won't be diving into the details of this specific issue in this blog post,
but if you are interested, you can read more about it via [the revert PR #25320](https://github.com/bazelbuild/bazel/pull/25320).
We expect this to be fixed in the upcoming Bazel 8.1.1 release.

## Build failed after upgrading a dependency

This bisect technique can also be used to troubleshoot issues that arise after upgrading a Bazel dependency.
For example, recently we attempted upgrading `@rules_go` in our repository from `v0.51.0` to `v0.53.0`, and the build failed.

```bash
$ bazel build server
...
ERROR: /private/var/tmp/_bazel_sluongng/06e573a93bc2d6a9cad4ad41f00b4310/external/bazel_gazelle/internal/go_repository_cache.bzl:30:17: An error occurred during the fetch of repository 'bazel_gazelle_go_repository_cache':
   Traceback (most recent call last):
	File "/private/var/tmp/_bazel_sluongng/06e573a93bc2d6a9cad4ad41f00b4310/external/bazel_gazelle/internal/go_repository_cache.bzl", line 30, column 17, in _go_repository_cache_impl
		fail('gazelle found more than one suitable Go SDK ({}). Specify which one to use with gazelle_dependencies(go_sdk = "go_sdk").'.format(", ".join(matches)))
Error in fail: gazelle found more than one suitable Go SDK (go_host_compatible_sdk_label, go_sdk_darwin_arm64). Specify which one to use with gazelle_dependencies(go_sdk = "go_sdk").
ERROR: no such package '@@org_golang_google_grpc//reflection': gazelle found more than one suitable Go SDK (go_host_compatible_sdk_label, go_sdk_darwin_arm64). Specify which one to use with gazelle_dependencies(go_sdk = "go_sdk").
ERROR: /Users/sluongng/work/buildbuddy-io/buildbuddy/cli/cmd/sidecar/BUILD:3:11: //cli/cmd/sidecar:sidecar depends on @@org_golang_google_grpc//reflection:reflection in repository @@org_golang_google_grpc which failed to fetch. no such package '@@org_golang_google_grpc//reflection': gazelle found more than one suitable Go SDK (go_host_compatible_sdk_label, go_sdk_darwin_arm64). Specify which one to use with gazelle_dependencies(go_sdk = "go_sdk").
ERROR: Analysis of target '//cli:cli' failed; build aborted: Analysis failed
...
```

This time, we can use the same bisect technique to identify the commit that introduced the issue.
To make the `git bisect` a bit less tedious, let's use a local copy of `@rules_go` instead of the one managed by Bazel.

```bash
$ cd ~/work/bazelbuild
$ git clone https://github.com/bazel-contrib/rules_go.git
$ cd rules_go
$ git checkout v0.51.0
```

With this, we can add the following lines to our `.bazelrc` to tell Bazel to use our local copy instead of the one managed by Bazel.

```bash
$ cd ~/work/buildbuddy-io/buildbuddy
$ tail -n 5 .bazelrc
## BZLMOD
common --override_module=rules_go=/Users/sluongng/work/bazelbuild/rules_go

## WORKSPACE
common --override_repository=io_bazel_rules_go=/Users/sluongng/work/bazelbuild/rules_go
```

**Pro tips:** These flags are really handy, so I would recommend keeping them in your `.bazelrc` file as comments for future use.

Now we can start the bisect process in the `@rules_go` repository.

```bash
$ cd ~/work/bazelbuild/rules_go
$ cat test-rules-go.sh
#!/bin/bash

(
  cd ~/work/buildbuddy-io/buildbuddy
  bazel build server
)
$ chmod +x test-rules-go.sh
```

Since we are relying on our local copy of `@rules_go`, we do not need to handle the download issue like the previous script.
We are also not expecting the build to fail because of external factors, so no need for setting `BAZELISK_CLEAN` or handling our own cleanup.
This also means that we are reusing the same Bazel JVM process for each bisect run, taking advantage of the hot analysis cache to keep our builds blazingly fast.

Now let's start the bisect process:

```bash
$ cd ~/work/bazelbuild/rules_go
$ git bisect start --first-parent v0.53.0 v0.51.0
```

Note here that we do NOT want to use the `--no-checkout` flag as the working copy of the repo is used for the bisect run and therefore, needs to be updated.

```bash
$ git bisect run ./test-rules-go.sh
running './test-rules-go.sh'
Bisecting: 8 revisions left to test after this (roughly 3 steps)
[4f5202adf56521b3048536d04eef12690557fa7c] Mention `dev_dependency` in `go_sdk.host` error (#4246)
running './test-rules-go.sh'
Bisecting: 4 revisions left to test after this (roughly 2 steps)
[66477c1b41b2449c8102f4338d011f07e4df04b6] Update documentation reference (#4237)
running './test-rules-go.sh'
Bisecting: 1 revision left to test after this (roughly 1 step)
[5eb06119c49b97f16aa79d53cdcd99f95b1000bf] Allow .so files to have more extensions (#4232)
running './test-rules-go.sh'
Bisecting: 0 revisions left to test after this (roughly 0 steps)
[d25e4e75f0ce8e419593a5c633f852ff1c08e292] Use same Go SDK as Gazelle for `go_bazel_test` (#4231)
running './test-rules-go.sh'
d25e4e75f0ce8e419593a5c633f852ff1c08e292 is the first bad commit
commit d25e4e75f0ce8e419593a5c633f852ff1c08e292
Author: Fabian Meumertzheim <fabian@meumertzhe.im>
Date:   Thu Jan 16 08:13:09 2025 +0100

    Use same Go SDK as Gazelle for `go_bazel_test` (#4231)

    **What type of PR is this?**

    Bug fix

    **What does this PR do? Why is it needed?**

    **Which issues(s) does this PR fix?**

    Fixes #4228

    **Other notes for review**

 MODULE.bazel                       |  2 +-
 go/private/repositories.bzl        | 11 +++++++++++
 go/tools/bazel_testing/BUILD.bazel |  9 ++++++---
 3 files changed, 18 insertions(+), 4 deletions(-)
bisect found first bad commit
```

And there you have it! The issue was introduced in commit `d25e4e75f0ce8e419593a5c633f852ff1c08e292` in the `@rules_go` repository, which was introduced in `v0.53.0`.
This was also fixed swiftly by Fabian in rules_go's [PR #4264](https://github.com/bazel-contrib/rules_go/pull/4264).

## Conclusion

Using `git bisect` to troubleshoot build failures after upgrading Bazel or its dependencies can be a powerful tool.
This helps us narrow down the root cause of the issue to the exact commit that introduced it.

This makes the error much more actionable.
For example, when we were able to identify the issue to be in rules_go@v0.53.0, we were able to upgrade to v0.52.0 instead and reported the fix to the upstream open-source project.
In other cases, we can proceed with the upgrade but with a specific revert of the commit that introduced the issue patched into the external dependencies.

I hope this guide was helpful to you and that you can use it to troubleshoot your external dependency upgrades in the future.
