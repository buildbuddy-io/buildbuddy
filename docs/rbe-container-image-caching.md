---
id: rbe-container-image-caching
title: RBE Container Image Caching
sidebar_label: RBE Container Image Caching
---

Remote Build Execution (RBE) runs actions inside a container. By default, actions use an Ubuntu 16.04 container image. You can specify a [custom container image](rbe-platforms#using-a-custom-docker-image).

BuildBuddy stores container images on disk to make action startup fast and to avoid fetching container images each time an action runs. As executors scale up and down or remove unused container images from disk, they may need to fetch the image from the upstream registry.

BuildBuddy RBE caches container images in the same remote cache that stores Bazel build artifacts. If an action needs to run inside a container image that is not already on the executor's disk, the executor can download it from the remote cache. This download counts as Internal Download and saves you from having to pay for egress from your registry provider.
