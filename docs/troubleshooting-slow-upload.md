---
id: troubleshooting-slow-upload
title: Troubleshooting Slow Uploads
sidebar_label: Slow Uploads
---

## The Build Event Protocol upload timed out

This error means the `bes_timeout` [flag](https://docs.bazel.build/versions/master/command-line-reference.html#flag--bes_timeout) is likely set to a value that's not long enough for bazel to finish uploading all build artifacts.

We recommend using the following flag to increase this upload timeout:

```
--bes_timeout=600s
```

These slow uploads should only happen once when artifacts are initially written to the cache, and shouldn't happen on subsequent builds.

## Waiting for build events upload

If your build has finished but you're frequently sitting around waiting for build events to upload - you're likely in a network constrained environment trying to upload large build artifacts like docker images or large binaries.

For network constrained environments, we recommend running with the flag:

```
--noremote_upload_local_results
```

This will upload build, test, and profiling logs - but not the larger build artifacts that can take much longer to upload.
