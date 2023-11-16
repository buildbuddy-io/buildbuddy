---
slug: download-with-rbe
title: "Bazel: Download with RBE"
description: A discussion on performing dependencies downloads with RBE.
author: Son Luong Ngoc
author_title: Solution Engineer @ BuildBuddy
date: 2023-11-16:12:00:00
author_url: https://github.com/sluongng/
author_image_url: https://avatars.githubusercontent.com/u/26684313?v=4
image: /img/blog/buck2-header.png
tags: [bazel, engineering]
---

# Bazel: Download with RBE

In Bazel, before each build happen, the third party dependencies has to be prepared locally, on disk, in full form so that Bazel could
analyze them and form the target graph.
Even with recent advance ment in Bazel's deferred materialization of build artifacts, aka. Build without the Bytes (BwotB),
when using Remote Build Execution (RBE), users would still have to download tens to hundreds of gigabytes of dependencies before they can start building.
This makes up a really bad on-boarding experience for new Bazel adopters.

With [recent discussions (by Julio Merino)](https://jmmv.dev/2023/10/bazelcon-2023-et-al-trip-report.html) regarding shifting part of these external dependencies downloads from Bazel's early Load Phase to the later Execution Phase,
let's us discuss what does this potentially mean and how would it help improving the Bazel's user experience.

## Traditional Approach


## The newly proposed approach

### Case 1: replacing `http_archive`

### Case 2: replacing `go_repository`

### Case 3: replacing `dockerfile_image`


## Potential Bazel improvements?


## Conclusion

